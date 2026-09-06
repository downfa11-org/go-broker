package topic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/util"
)

const (
	topicMetadataFormatVersion = 3
	maxTopicMetadataBytes      = 16 << 20
	maxTopicNameBytes          = 249
	TopicMetadataFileName      = config.TopicMetadataFileName
)

// Definition is the durable broker contract required to recreate a topic.
type Definition struct {
	Name              string `json:"name"`
	Revision          uint64 `json:"revision"`
	LifecycleEpoch    uint64 `json:"lifecycle_epoch"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	Idempotent        bool   `json:"idempotent"`
	EventSourcing     bool   `json:"event_sourcing"`
	Policy            Policy `json:"policy"`
}

// Normalize validates and canonicalizes a durable topic definition.
func (d Definition) Normalize() (Definition, error) {
	if err := ValidateName(d.Name); err != nil {
		return d, err
	}
	if d.Partitions <= 0 {
		return d, fmt.Errorf("partitions must be > 0")
	}
	if d.ReplicationFactor == 0 {
		d.ReplicationFactor = DefaultReplicationFactor
	}
	if d.ReplicationFactor < 0 {
		return d, fmt.Errorf("replication_factor must be > 0")
	}
	if d.Revision == 0 {
		d.Revision = InitialDefinitionRevision
	}
	if d.LifecycleEpoch == 0 {
		d.LifecycleEpoch = InitialLifecycleEpoch
	}
	policy, err := d.Policy.Normalize()
	if err != nil {
		return d, err
	}
	if d.EventSourcing && d.Name != config.ConsumerOffsetsTopicName {
		if err := validateEventRetention(policy, true); err != nil {
			return d, err
		}
	}
	d.Policy = policy
	d.Policy = policy.Clone()
	return d, nil
}

func validateDefinitionVersionFields(version int, definition Definition) error {
	if definition.Revision == 0 {
		return fmt.Errorf("revision must be present in topic metadata version %d", version)
	}
	if definition.ReplicationFactor == 0 {
		return fmt.Errorf("replication_factor must be present in topic metadata version %d", version)
	}
	if definition.LifecycleEpoch == 0 {
		return fmt.Errorf("lifecycle_epoch must be present in topic metadata version %d", version)
	}
	return nil
}

// ValidateName restricts topic names to the portable on-disk name contract.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name is empty")
	}
	if len(name) > maxTopicNameBytes {
		return fmt.Errorf("topic name exceeds %d bytes", maxTopicNameBytes)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid topic name %q", name)
	}
	for _, char := range name {
		switch {
		case char >= 'a' && char <= 'z':
		case char >= 'A' && char <= 'Z':
		case char >= '0' && char <= '9':
		case char == '.', char == '_', char == '-', char == '=':
		default:
			return fmt.Errorf("topic name %q contains unsupported characters", name)
		}
	}
	return nil
}

type metadataPathProvider interface {
	TopicMetadataPath() string
}

type topicMetadataManifest struct {
	Version int          `json:"version"`
	Topics  []Definition `json:"topics"`
}

type topicMetadataStore struct {
	path string
}

func newTopicMetadataStore(cfg *config.Config, provider HandlerProvider) *topicMetadataStore {
	if cfg == nil || cfg.EnabledDistribution || provider == nil {
		return nil
	}
	pathProvider, ok := provider.(metadataPathProvider)
	if !ok {
		return nil
	}
	path := strings.TrimSpace(pathProvider.TopicMetadataPath())
	if path == "" {
		return nil
	}
	return &topicMetadataStore{path: filepath.Clean(path)}
}

func (s *topicMetadataStore) Load() (_ []Definition, err error) {
	if s == nil {
		return nil, nil
	}
	if err := validateTopicStorageRoot(filepath.Dir(s.path)); err != nil {
		return nil, err
	}
	// #nosec G304 -- the path is supplied by the broker-owned storage provider.
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		orphaned, scanErr := s.orphanedTopicDirectories(nil)
		if scanErr != nil {
			return nil, scanErr
		}
		if len(orphaned) > 0 {
			return nil, &orphanedTopicMetadataError{
				count: len(orphaned),
				err: fmt.Errorf(
					"topic metadata manifest is missing while persisted topic storage exists: %s",
					strings.Join(orphaned, ","),
				),
			}
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open topic metadata: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat topic metadata: %w", err)
	}
	if info.Size() > maxTopicMetadataBytes {
		return nil, fmt.Errorf("topic metadata size %d exceeds limit %d", info.Size(), maxTopicMetadataBytes)
	}

	data, err := io.ReadAll(io.LimitReader(file, maxTopicMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest topicMetadataManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode topic metadata: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode topic metadata trailing content")
	}
	if manifest.Version != topicMetadataFormatVersion {
		return nil, fmt.Errorf("unsupported topic metadata version %d; clean bootstrap required", manifest.Version)
	}

	seen := make(map[string]struct{}, len(manifest.Topics))
	definitions := make([]Definition, 0, len(manifest.Topics))
	for _, raw := range manifest.Topics {
		if err := validateDefinitionVersionFields(manifest.Version, raw); err != nil {
			return nil, fmt.Errorf("invalid topic metadata for %q: %w", raw.Name, err)
		}
		definition, err := raw.Normalize()
		if err != nil {
			return nil, fmt.Errorf("invalid topic metadata for %q: %w", raw.Name, err)
		}
		if _, exists := seen[definition.Name]; exists {
			return nil, fmt.Errorf("duplicate topic metadata for %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		definitions = append(definitions, definition)
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	orphaned, err := s.orphanedTopicDirectories(seen)
	if err != nil {
		return nil, err
	}
	if len(orphaned) > 0 {
		return nil, &orphanedTopicMetadataError{
			count: len(orphaned),
			err: fmt.Errorf(
				"topic metadata manifest omits persisted topic storage: %s",
				strings.Join(orphaned, ","),
			),
		}
	}
	if err := validateDeclaredTopicStorage(filepath.Dir(s.path), definitions); err != nil {
		return nil, err
	}
	return definitions, nil
}

func (s *topicMetadataStore) Save(definitions []Definition) (err error) {
	if s == nil {
		return nil
	}
	_, data, err := normalizeAndMarshalCurrentManifest(definitions)
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create topic metadata directory: %w", err)
	}
	tmp := s.path + ".tmp"
	defer func() {
		if removeErr := os.Remove(tmp); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove topic metadata temporary file: %w", removeErr))
		}
	}()

	// #nosec G304 -- the path is supplied by the broker-owned storage provider.
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open topic metadata temporary file: %w", err)
	}
	if err := writeMetadataFile(file, data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close topic metadata temporary file: %w", err)
	}
	if err := replaceCheckpointFile(tmp, s.path); err != nil {
		return fmt.Errorf("replace topic metadata: %w", err)
	}
	if err := syncTopicMetadataDirectoryFn(dir); err != nil {
		return &topicMetadataSaveError{
			committed: true,
			err:       fmt.Errorf("sync topic metadata directory: %w", err),
		}
	}
	return nil
}

func normalizeAndMarshalCurrentManifest(definitions []Definition) ([]Definition, []byte, error) {
	normalized := make([]Definition, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, raw := range definitions {
		definition, err := raw.Normalize()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid topic metadata for %q: %w", raw.Name, err)
		}
		if definition.Name == config.ConsumerOffsetsTopicName {
			if definition.Idempotent || definition.EventSourcing || !reflect.DeepEqual(definition.Policy, ConsumerMetadataPolicy()) {
				return nil, nil, fmt.Errorf("invalid topic metadata for %q: internal consumer metadata contract mismatch; clean bootstrap required", definition.Name)
			}
		}
		if _, exists := seen[definition.Name]; exists {
			return nil, nil, fmt.Errorf("duplicate topic metadata for %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		if err := validateCleanupPolicyForTopic(definition.Policy, nil, definition.EventSourcing); err != nil {
			return nil, nil, fmt.Errorf("invalid topic metadata for %q: %w", definition.Name, err)
		}
		normalized = append(normalized, definition)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Name < normalized[j].Name })
	data, err := json.MarshalIndent(topicMetadataManifest{Version: topicMetadataFormatVersion, Topics: normalized}, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("marshal topic metadata: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxTopicMetadataBytes {
		return nil, nil, fmt.Errorf("topic metadata size %d exceeds limit %d", len(data), maxTopicMetadataBytes)
	}
	return normalized, data, nil
}

func writeMetadataFile(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("write topic metadata temporary file: %w", err)
		}
		if written == 0 {
			return fmt.Errorf("write topic metadata temporary file: %w", io.ErrShortWrite)
		}
		data = data[written:]
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync topic metadata temporary file: %w", err)
	}
	return nil
}

var syncTopicMetadataDirectoryFn = syncTopicMetadataDirectory

func syncTopicMetadataDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	// #nosec G304 -- the path is supplied by the broker-owned storage provider.
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	return errors.Join(syncErr, dir.Close())
}

// RestoreTopics loads the standalone manifest and recreates its topic registry.
func (tm *TopicManager) RestoreTopics() error {
	if tm == nil || tm.metadataStore == nil {
		return nil
	}
	definitions, err := tm.metadataStore.Load()
	if err != nil {
		restoreErr := fmt.Errorf("load durable topic metadata: %w", err)
		tm.recordMetadataLoadResult(restoreErr, confirmedOrphanCount(err), 0)
		return restoreErr
	}
	if err := tm.RestoreDefinitions(definitions); err != nil {
		restoreErr := fmt.Errorf("restore durable topics: %w", err)
		tm.recordMetadataLoadResult(restoreErr, 0, 0)
		return restoreErr
	}
	tm.recordMetadataLoadResult(nil, 0, len(definitions))
	return nil
}

// RestoreDefinitions recreates authoritative topic definitions without
// rewriting the source metadata while recovery is in progress.
func (tm *TopicManager) RestoreDefinitions(definitions []Definition) error {
	normalized := make([]Definition, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, raw := range definitions {
		definition, err := raw.Normalize()
		if err != nil {
			return fmt.Errorf("invalid definition for %q: %w", raw.Name, err)
		}
		if definition.Name == config.ConsumerOffsetsTopicName {
			if definition.Idempotent || definition.EventSourcing {
				return fmt.Errorf("invalid definition for %q: internal consumer metadata mode must be non-idempotent and non-event-sourcing", definition.Name)
			}
			definition.Policy = ConsumerMetadataPolicy()
		}
		if definition.Name != config.ConsumerOffsetsTopicName {
			if err := validateCleanupPolicyForTopic(definition.Policy, tm.cfg, definition.EventSourcing); err != nil {
				return fmt.Errorf("invalid definition for %q: %w", definition.Name, err)
			}
		}
		if _, exists := seen[definition.Name]; exists {
			return fmt.Errorf("duplicate definition for %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		normalized = append(normalized, definition)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Name < normalized[j].Name })

	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, definition := range normalized {
		if existing := tm.topics[definition.Name]; existing != nil {
			current := existing.Definition()
			if current.Idempotent != definition.Idempotent || current.EventSourcing != definition.EventSourcing {
				return fmt.Errorf(
					"topic %q mode mismatch: idempotent=%t/%t event_sourcing=%t/%t",
					definition.Name,
					current.Idempotent,
					definition.Idempotent,
					current.EventSourcing,
					definition.EventSourcing,
				)
			}
			if definition.Partitions < current.Partitions {
				return fmt.Errorf(
					"topic %q partition metadata regressed: current=%d restored=%d",
					definition.Name,
					current.Partitions,
					definition.Partitions,
				)
			}
		}
	}

	created := make([]string, 0, len(normalized))
	for _, definition := range normalized {
		if err := tm.prepareTruncationRecoveryLocked(definition); err != nil {
			tm.rollbackRestoredTopicsLocked(created)
			return fmt.Errorf("restore topic %q lifecycle: %w", definition.Name, err)
		}
		if existing := tm.topics[definition.Name]; existing != nil {
			if err := existing.applyFullDefinition(definition, tm.hp, nil); err != nil {
				tm.rollbackRestoredTopicsLocked(created)
				return fmt.Errorf("restore topic %q: %w", definition.Name, err)
			}
			continue
		}

		restored, err := newTopicWithDefinition(definition, tm.hp, tm.cfg, tm.StreamManager)
		if err != nil {
			tm.rollbackRestoredTopicsLocked(created)
			return fmt.Errorf("restore topic %q: %w", definition.Name, err)
		}
		restored.SetDistributedCompactionGate(tm.compactionGate)
		restored.SetTransactionDecisionResolver(tm.txnResolver)
		tm.topics[definition.Name] = restored
		created = append(created, definition.Name)
	}
	return nil
}

func (tm *TopicManager) rollbackRestoredTopicsLocked(names []string) {
	for _, name := range names {
		restored := tm.topics[name]
		if restored == nil {
			continue
		}
		closePartiallyInitializedTopic(name, tm.hp, restored.Partitions)
		delete(tm.topics, name)
	}
}

// ExportDefinitions returns a deterministic detached snapshot of topic state.
func (tm *TopicManager) ExportDefinitions() []Definition {
	if tm == nil {
		return nil
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.definitionsLocked(nil, "")
}

func (tm *TopicManager) persistDefinitionLocked(override Definition) error {
	if tm.metadataStore == nil {
		return nil
	}
	definitions := tm.definitionsLocked(&override, "")
	if err := tm.metadataStore.Save(definitions); err != nil {
		if topicMetadataWriteCommitted(err) {
			tm.recordMetadataDurabilityWarningLocked(err)
			util.Warn("Topic metadata definition %q committed with durability uncertainty: %v", override.Name, err)
			return nil
		}
		return fmt.Errorf("persist topic definition %q: %w", override.Name, err)
	}
	tm.metadataDurabilityWarning = ""
	return nil
}

func (tm *TopicManager) persistRemovalLocked(name string) error {
	if tm.metadataStore == nil {
		return nil
	}
	if err := tm.metadataStore.Save(tm.definitionsLocked(nil, name)); err != nil {
		if topicMetadataWriteCommitted(err) {
			tm.recordMetadataDurabilityWarningLocked(err)
			util.Warn("Topic metadata deletion %q committed with durability uncertainty: %v", name, err)
			return nil
		}
		return fmt.Errorf("persist topic deletion %q: %w", name, err)
	}
	tm.metadataDurabilityWarning = ""
	return nil
}

func (tm *TopicManager) definitionsLocked(override *Definition, removed string) []Definition {
	definitions := make([]Definition, 0, len(tm.topics)+1)
	seen := make(map[string]struct{}, len(tm.topics)+len(tm.pendingTruncations))
	overrideAdded := false
	for name, current := range tm.topics {
		if name == removed {
			continue
		}
		if override != nil && name == override.Name {
			definitions = append(definitions, *override)
			seen[name] = struct{}{}
			overrideAdded = true
			continue
		}
		definitions = append(definitions, current.Definition())
		seen[name] = struct{}{}
	}
	for name, pending := range tm.pendingTruncations {
		if name == removed {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		if override != nil && name == override.Name {
			definitions = append(definitions, *override)
			overrideAdded = true
		} else {
			definitions = append(definitions, pending)
		}
		seen[name] = struct{}{}
	}
	if override != nil && !overrideAdded && override.Name != removed {
		definitions = append(definitions, *override)
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Name < definitions[j].Name })
	return definitions
}
