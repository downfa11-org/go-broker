package transaction

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

type State string

const (
	StateOpen       State = "open"
	StateCommitting State = "committing"
	StateCommitted  State = "committed"
	StateAborted    State = "aborted"
)

var ErrProducerReinitializationRequired = errors.New("producer reinitialization required")

type MessageOperation struct {
	Topic     string
	Partition int
	Message   types.Message
}

type OffsetOperation struct {
	RegistrationEpoch uint64
	Topic             string
	Group             string
	Member            string
	Generation        int
	Partition         int
	Offset            uint64
}

type Transaction struct {
	ID        string
	Producer  string
	Epoch     int64
	Revision  uint64
	Ready     bool
	Expired   bool
	State     State
	Messages  []MessageOperation
	Offsets   []OffsetOperation
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Snapshot struct {
	ID        string             `json:"id"`
	Producer  string             `json:"producer"`
	Epoch     int64              `json:"epoch"`
	Revision  uint64             `json:"revision,omitempty"`
	Ready     bool               `json:"ready,omitempty"`
	Expired   bool               `json:"expired,omitempty"`
	State     State              `json:"state"`
	Messages  []MessageOperation `json:"messages,omitempty"`
	Offsets   []OffsetOperation  `json:"offsets,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type Manager struct {
	mu         sync.Mutex
	txns       map[string]*Transaction
	expiration time.Duration
	limits     Limits
	sizes      map[string]int64
	totalBytes int64
}

func NewManager() *Manager {
	return NewManagerWithExpiration(7 * 24 * time.Hour)
}

func NewManagerWithExpiration(expiration time.Duration) *Manager {
	if expiration <= 0 {
		expiration = 7 * 24 * time.Hour
	}
	return &Manager{txns: make(map[string]*Transaction), expiration: expiration,
		limits: DefaultLimits(), sizes: make(map[string]int64)}
}

func (m *Manager) PruneExpired(now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pruneExpiredLocked(now)
}

func (m *Manager) pruneExpiredLocked(now time.Time) int {
	if m.expiration <= 0 {
		return 0
	}
	cutoff := now.Add(-m.expiration)
	removed := 0
	for id, tx := range m.txns {
		if tx == nil {
			m.deleteLocked(id)
			removed++
			continue
		}
		if tx.Expired {
			if tx.UpdatedAt.Before(cutoff) {
				m.deleteLocked(id)
				removed++
			}
			continue
		}
		if expireTransactionLocked(tx, cutoff, now) {
			m.recordSizeLocked(id, transactionCharge(tx))
			removed++
		}
	}
	return removed
}

func expireTransactionLocked(tx *Transaction, cutoff, now time.Time) bool {
	if tx == nil || tx.Expired || !tx.UpdatedAt.Before(cutoff) {
		return false
	}
	if tx.State != StateCommitted && tx.State != StateAborted {
		return false
	}
	tx.Messages = nil
	tx.Offsets = nil
	tx.Ready = false
	tx.Expired = true
	tx.Revision++
	tx.UpdatedAt = now
	return true
}

func (m *Manager) InitProducer(id string) (string, int64, error) {
	if id == "" {
		return "", 0, fmt.Errorf("missing transaction id")
	}
	if len(id) > 1024 {
		return "", 0, fmt.Errorf("%w: transactional id exceeds 1024 bytes", ErrLimitExceeded)
	}
	id = strings.Clone(id)

	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	previous := m.txns[id]

	producer := producerIDForTransactionalID(id)
	epoch := int64(0)
	revision := uint64(1)
	if tx := previous; tx != nil {
		if tx.State == StateCommitting {
			return "", 0, fmt.Errorf("transaction %s is committing; retry END_TXN before reinitializing producer", id)
		}
		if tx.Producer != "" {
			producer = tx.Producer
		}
		epoch = tx.Epoch + 1
		revision = tx.Revision + 1
	}

	next := &Transaction{
		ID:        id,
		Producer:  producer,
		Epoch:     epoch,
		Revision:  revision,
		Ready:     true,
		State:     StateAborted,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := m.admissionErrorLocked(id, transactionCharge(next), 0, 0); err != nil {
		return "", 0, err
	}
	m.replaceLocked(id, next)
	return producer, epoch, nil
}
func (m *Manager) Begin(id, producer string, epoch int64) error {
	if id == "" {
		return fmt.Errorf("missing transaction id")
	}
	if producer == "" {
		return fmt.Errorf("missing transactional producer")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok || tx.Expired {
		return fmt.Errorf("transaction %s is not initialized; call INIT_PRODUCER_ID first", id)
	}
	if tx.Producer != "" && tx.Producer != producer {
		return fmt.Errorf("transaction owner mismatch transactional_id=%s producer=%s requested=%s", id, tx.Producer, producer)
	}
	if epoch < tx.Epoch {
		return fmt.Errorf("producer fenced transactional_id=%s current_epoch=%d requested_epoch=%d", id, tx.Epoch, epoch)
	}
	if epoch > tx.Epoch {
		return fmt.Errorf("producer epoch not initialized transactional_id=%s current_epoch=%d requested_epoch=%d", id, tx.Epoch, epoch)
	}
	if !tx.Ready {
		return fmt.Errorf("%w: transactional_id=%s epoch=%d", ErrProducerReinitializationRequired, id, epoch)
	}
	if tx.State != StateCommitted && tx.State != StateAborted {
		return fmt.Errorf("transaction %s is already active", id)
	}

	now := time.Now()
	next := &Transaction{
		ID:        tx.ID,
		Producer:  tx.Producer,
		Epoch:     epoch,
		Revision:  tx.Revision + 1,
		Ready:     false,
		State:     StateOpen,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := m.admissionErrorLocked(id, transactionCharge(next), 0, 0); err != nil {
		return err
	}
	m.replaceLocked(id, next)
	return nil
}

func (m *Manager) AddMessage(id, producer string, epoch int64, op MessageOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	size := m.sizes[id] + messageCharge(op)
	if err := m.admissionErrorLocked(id, size, len(tx.Messages)+1, len(tx.Offsets)); err != nil {
		return err
	}
	tx.Messages = append(tx.Messages, ownMessageOperation(op))
	m.recordSizeLocked(id, size)
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) AddOffsets(id, producer string, epoch int64, offsets []OffsetOperation) error {
	if len(offsets) == 0 {
		return fmt.Errorf("no offsets supplied")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.activeLocked(id)
	if err != nil {
		return err
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	if len(offsets) > m.limits.Offsets || len(tx.Offsets) > m.limits.Offsets {
		return fmt.Errorf("%w: offset request count=%d maximum=%d", ErrLimitExceeded, len(offsets), m.limits.Offsets)
	}
	scope := offsets[0]
	if len(tx.Offsets) > 0 {
		scope = tx.Offsets[0]
	}
	for _, op := range offsets {
		if op.Topic != scope.Topic || op.Group != scope.Group || op.Member != scope.Member || op.Generation != scope.Generation {
			return fmt.Errorf(
				"transaction offset scope mismatch: expected topic=%s group=%s member=%s generation=%d",
				scope.Topic, scope.Group, scope.Member, scope.Generation,
			)
		}
	}

	next := append([]OffsetOperation(nil), tx.Offsets...)
	size := m.sizes[id]
	for _, op := range offsets {
		updated := false
		for i := range next {
			current := &next[i]
			if current.Topic != op.Topic || current.Group != op.Group || current.Partition != op.Partition {
				continue
			}
			if op.Offset < current.Offset {
				return fmt.Errorf(
					"transaction offset regression topic=%s group=%s partition=%d current=%d attempted=%d",
					op.Topic, op.Group, op.Partition, current.Offset, op.Offset,
				)
			}
			size += offsetCharge(op) - offsetCharge(*current)
			*current = op
			updated = true
			break
		}
		if !updated {
			next = append(next, op)
			size += offsetCharge(op)
		}
	}
	if err := m.admissionErrorLocked(id, size, len(tx.Messages), len(next)); err != nil {
		return err
	}
	for i := range next {
		op := &next[i]
		op.Topic, op.Group, op.Member = strings.Clone(op.Topic), strings.Clone(op.Group), strings.Clone(op.Member)
	}
	tx.Offsets = next
	m.recordSizeLocked(id, size)
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) BuildPreparedSnapshot(id, producer string, epoch int64) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	if tx.State != StateOpen {
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
	snap := snapshot(tx)
	snap.State = StateCommitting
	snap.Revision++
	snap.UpdatedAt = time.Now()
	return snap, nil
}

func (m *Manager) PrepareCommit(id, producer string, epoch int64) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateOpen:
		tx.State = StateCommitting
		tx.Revision++
		tx.UpdatedAt = time.Now()
		return clone(tx), nil
	case StateCommitting:
		return clone(tx), nil
	case StateCommitted:
		return clone(tx), nil
	case StateAborted:
		return nil, fmt.Errorf("transaction %s is aborted", id)
	default:
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
}

func (m *Manager) Commit(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateCommitting {
		return fmt.Errorf("transaction %s is not prepared for commit", id)
	}
	tx.State = StateCommitted
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) Abort(id, producer string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return err
	}
	if tx.State == StateCommitted {
		return fmt.Errorf("transaction %s is already committed", id)
	}
	if tx.State == StateAborted {
		return nil
	}
	if tx.State == StateCommitting {
		return fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	}
	tx.State = StateAborted
	tx.Messages = nil
	tx.Offsets = nil
	m.recordSizeLocked(id, transactionCharge(tx))
	tx.Revision++
	tx.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) ValidateOwner(id, producer string, epoch int64) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	return clone(tx), nil
}
func (m *Manager) Status(id string) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok || tx.Expired {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	return clone(tx), nil
}

func (m *Manager) TransactionDecision(id string, epoch int64) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok || tx.Expired || tx.Epoch != epoch {
		return "", false
	}
	return string(tx.State), true
}

func (m *Manager) BuildCommittedSnapshot(id string) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	switch tx.State {
	case StateCommitted:
		return snapshot(tx), nil
	case StateCommitting:
	default:
		return nil, fmt.Errorf("transaction %s is not prepared for commit", id)
	}
	committed := snapshot(tx)
	committed.State = StateCommitted
	committed.Revision++
	committed.UpdatedAt = time.Now()
	return committed, nil
}
func (m *Manager) BuildAbortedSnapshot(id, producer string, epoch int64) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	switch tx.State {
	case StateCommitted:
		return nil, fmt.Errorf("transaction %s is already committed", id)
	case StateAborted:
		return snapshot(tx), nil
	case StateCommitting:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	case StateOpen:
		aborted := snapshot(tx)
		aborted.State = StateAborted
		aborted.Revision++
		aborted.UpdatedAt = time.Now()
		return aborted, nil
	default:
		return nil, fmt.Errorf("transaction %s cannot be aborted from state %s", id, tx.State)
	}
}

func (m *Manager) TransactionsByState(states ...State) []*Transaction {
	wanted := make(map[State]struct{}, len(states))
	for _, state := range states {
		wanted[state] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*Transaction, 0)
	for _, tx := range m.txns {
		if tx.Expired {
			continue
		}
		if _, ok := wanted[tx.State]; ok {
			out = append(out, clone(tx))
		}
	}
	return out
}
func (m *Manager) ExportState() map[string]*Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(map[string]*Snapshot, len(m.txns))
	for id, tx := range m.txns {
		out[id] = snapshot(tx)
	}
	return out
}

func (m *Manager) SnapshotByID(id string) (*Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, ok := m.txns[id]
	return snapshot(tx), ok
}

func (m *Manager) ImportState(state map[string]*Snapshot) error {
	if err := ValidateImportState(state); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.txns = make(map[string]*Transaction, len(state))
	m.sizes = make(map[string]int64, len(state))
	m.totalBytes = 0
	for id, snap := range state {
		m.replaceLocked(id, transactionFromSnapshot(snap))
	}
	return nil
}

func (m *Manager) ApplySnapshot(snap *Snapshot) {
	if snap == nil || snap.ID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaceLocked(snap.ID, transactionFromSnapshot(snap))
}

func (m *Manager) ApplyReplicatedSnapshot(snap *Snapshot) error {
	if err := validateSnapshot(snap); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.txns[snap.ID]
	if !ok || snapshotIsNewer(current, snap) {
		m.replaceLocked(snap.ID, transactionFromSnapshot(snap))
		return nil
	}
	if snapshotsEqual(current, snap) {
		return nil
	}
	// A retried coordinator must re-propose its durable committing snapshot
	// before applying records. If this replica has already applied the exact
	// successor committed decision, the predecessor is an idempotent no-op;
	// it must never regress the terminal state.
	if committedSnapshotSucceeds(current, snap) {
		return nil
	}
	return fmt.Errorf("stale transaction snapshot transactional_id=%s current_epoch=%d current_revision=%d incoming_epoch=%d incoming_revision=%d", snap.ID, current.Epoch, current.Revision, snap.Epoch, snap.Revision)
}

func committedSnapshotSucceeds(current *Transaction, incoming *Snapshot) bool {
	return current != nil && incoming != nil &&
		current.ID == incoming.ID &&
		current.Producer == incoming.Producer &&
		current.Epoch == incoming.Epoch &&
		current.State == StateCommitted &&
		incoming.State == StateCommitting &&
		current.Revision == incoming.Revision+1 &&
		current.Ready == incoming.Ready &&
		current.Expired == incoming.Expired &&
		current.CreatedAt.Equal(incoming.CreatedAt) &&
		reflect.DeepEqual(current.Messages, incoming.Messages) &&
		reflect.DeepEqual(current.Offsets, incoming.Offsets)
}

func snapshotIsNewer(current *Transaction, incoming *Snapshot) bool {
	if incoming.Epoch != current.Epoch {
		return incoming.Epoch > current.Epoch
	}
	if incoming.Revision != current.Revision {
		return incoming.Revision > current.Revision
	}
	return false
}

// ValidateImportState rejects incomplete transaction state before a journal
// or Raft snapshot can mutate the live manager.
func ValidateImportState(state map[string]*Snapshot) error {
	for id, snap := range state {
		if snap == nil {
			return fmt.Errorf("transaction snapshot %q is nil", id)
		}
		if snap.ID != id {
			return fmt.Errorf("transaction snapshot key %q does not match id %q", id, snap.ID)
		}
		if err := validateSnapshot(snap); err != nil {
			return fmt.Errorf("transaction snapshot %q: %w", id, err)
		}
	}
	return nil
}

func validateSnapshot(snap *Snapshot) error {
	if snap == nil || snap.ID == "" || snap.Producer == "" {
		return fmt.Errorf("invalid transaction snapshot identity")
	}
	if snap.Epoch < 0 {
		return fmt.Errorf("transaction snapshot has negative epoch %d", snap.Epoch)
	}
	if snap.Revision == 0 {
		return fmt.Errorf("transaction snapshot is missing revision; clean bootstrap required")
	}
	switch snap.State {
	case StateOpen, StateCommitting, StateCommitted, StateAborted:
	default:
		return fmt.Errorf("transaction snapshot has invalid state %q", snap.State)
	}
	if snap.CreatedAt.IsZero() || snap.UpdatedAt.IsZero() || snap.UpdatedAt.Before(snap.CreatedAt) {
		return fmt.Errorf("transaction snapshot has invalid timestamps")
	}
	return nil
}

func snapshotsEqual(current *Transaction, incoming *Snapshot) bool {
	return current.ID == incoming.ID &&
		current.Producer == incoming.Producer &&
		current.Epoch == incoming.Epoch &&
		current.Revision == incoming.Revision &&
		current.Ready == incoming.Ready &&
		current.Expired == incoming.Expired &&
		current.State == incoming.State &&
		current.CreatedAt.Equal(incoming.CreatedAt) &&
		current.UpdatedAt.Equal(incoming.UpdatedAt) &&
		reflect.DeepEqual(current.Messages, incoming.Messages) &&
		reflect.DeepEqual(current.Offsets, incoming.Offsets)
}

func (m *Manager) Delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteLocked(id)
}

func (m *Manager) activeLocked(id string) (*Transaction, error) {
	tx, ok := m.txns[id]
	if !ok {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if tx.State != StateOpen {
		return nil, fmt.Errorf("transaction %s is %s", id, tx.State)
	}
	return tx, nil
}

func validateOwner(tx *Transaction, producer string, epoch int64) error {
	if tx.Expired {
		return fmt.Errorf("transaction %s is expired; call INIT_PRODUCER_ID first", tx.ID)
	}
	if producer == "" {
		return fmt.Errorf("missing transactional producer")
	}
	if tx.Producer != producer {
		return fmt.Errorf("transaction owner mismatch transactional_id=%s producer=%s requested=%s", tx.ID, tx.Producer, producer)
	}
	if epoch != tx.Epoch {
		return fmt.Errorf("producer fenced transactional_id=%s current_epoch=%d requested_epoch=%d", tx.ID, tx.Epoch, epoch)
	}
	return nil
}

func cloneMessageOperations(messages []MessageOperation) []MessageOperation {
	out := append([]MessageOperation(nil), messages...)
	for i := range out {
		out[i].Message.ControlBatchKey = append([]byte(nil), out[i].Message.ControlBatchKey...)
		out[i].Message.ControlBatchValue = append([]byte(nil), out[i].Message.ControlBatchValue...)
	}
	return out
}

func clone(tx *Transaction) *Transaction {
	if tx == nil {
		return nil
	}
	out := *tx
	out.Messages = cloneMessageOperations(tx.Messages)
	out.Offsets = append([]OffsetOperation(nil), tx.Offsets...)
	return &out
}

func snapshot(tx *Transaction) *Snapshot {
	if tx == nil {
		return nil
	}
	return &Snapshot{
		ID:        tx.ID,
		Producer:  tx.Producer,
		Epoch:     tx.Epoch,
		Revision:  tx.Revision,
		Ready:     tx.Ready,
		Expired:   tx.Expired,
		State:     tx.State,
		Messages:  cloneMessageOperations(tx.Messages),
		Offsets:   append([]OffsetOperation(nil), tx.Offsets...),
		CreatedAt: tx.CreatedAt,
		UpdatedAt: tx.UpdatedAt,
	}
}

func transactionFromSnapshot(snap *Snapshot) *Transaction {
	return &Transaction{
		ID:        snap.ID,
		Producer:  snap.Producer,
		Epoch:     snap.Epoch,
		Revision:  snap.Revision,
		Ready:     snap.Ready,
		Expired:   snap.Expired,
		State:     snap.State,
		Messages:  cloneMessageOperations(snap.Messages),
		Offsets:   append([]OffsetOperation(nil), snap.Offsets...),
		CreatedAt: snap.CreatedAt,
		UpdatedAt: snap.UpdatedAt,
	}
}

func producerIDForTransactionalID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "txn-" + hex.EncodeToString(sum[:8])
}
