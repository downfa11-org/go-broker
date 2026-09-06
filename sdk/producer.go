package sdk

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
)

type BatchState struct {
	BatchID     string
	StartSeqNum uint64
	EndSeqNum   uint64
	Partition   int
	SentTime    time.Time
	Acked       bool
}

type partitionBuffer struct {
	mu           sync.Mutex
	msgs         []Message
	cond         *sync.Cond
	drainWake    chan struct{}
	drainWaiters []chan struct{}
	closed       bool
}

func (p *Producer) wireTLSConfig() *tls.Config {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.tlsConfig
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs:      make([]Message, 0),
		drainWake: make(chan struct{}, 1),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

type Producer struct {
	config *PublisherConfig
	client *ProducerClient

	partitions int
	buffers    []*partitionBuffer

	sendersWG sync.WaitGroup

	rr       atomic.Uint64
	inFlight []int32

	partitionSentMus  []sync.Mutex
	partitionSentSeqs []map[uint64]struct{}

	ackedCount    atomic.Uint64
	uniqueCount   atomic.Uint64
	attemptsCount atomic.Uint64

	partitionBatchStates []map[string]*BatchState
	partitionBatchMus    []sync.Mutex
	gcTicker             *time.Ticker

	partitionLeaders map[int]string
	partitionMu      sync.RWMutex

	done      chan struct{}
	closed    int32
	closeMu   sync.Mutex
	closeDone chan struct{}
	closeErr  error

	deliveryMu  sync.Mutex
	deliveryErr error

	bmMu         sync.Mutex
	bmTotalTime  map[int]time.Duration
	bmTotalCount map[int]int
	bmLatencies  []time.Duration
}

func NewProducer(cfg *PublisherConfig) (*Producer, error) {
	return NewProducerWithContext(context.Background(), cfg)
}

func NewProducerWithContext(ctx context.Context, cfg *PublisherConfig) (*Producer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("producer context must not be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("publisher config is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := validateSDKTopicName(cfg.Topic); err != nil {
		return nil, err
	}
	if cfg.EnableMetrics {
		initMetrics()
	}

	client, err := NewProducerClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create producer client: %w", err)
	}
	bootstrap := &Producer{config: cfg}
	if err := bootstrap.createConfiguredTopic(); err != nil {
		_ = client.Close()
		return nil, err
	}

	p := &Producer{
		config:           cfg,
		client:           client,
		partitions:       cfg.Partitions,
		buffers:          make([]*partitionBuffer, cfg.Partitions),
		done:             make(chan struct{}),
		closeDone:        make(chan struct{}),
		bmTotalTime:      make(map[int]time.Duration),
		bmTotalCount:     make(map[int]int),
		bmLatencies:      make([]time.Duration, 0),
		inFlight:         make([]int32, cfg.Partitions),
		gcTicker:         time.NewTicker(1 * time.Minute),
		partitionLeaders: make(map[int]string),
	}

	p.partitionSentSeqs = make([]map[uint64]struct{}, cfg.Partitions)
	p.partitionSentMus = make([]sync.Mutex, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		p.partitionSentSeqs[i] = make(map[uint64]struct{})
	}

	p.partitionBatchStates = make([]map[string]*BatchState, cfg.Partitions)
	p.partitionBatchMus = make([]sync.Mutex, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		p.partitionBatchStates[i] = make(map[string]*BatchState)
	}

	p.fetchMetadata()

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		brokerAddr := p.getPartitionLeaderAddr(i)
		if err := p.client.ConnectPartition(i, brokerAddr); err != nil {
			LogError("Failed to connect partition %d: %v", i, err)
		} else {
			connectedCount++
		}
		p.sendersWG.Add(1)
		go p.partitionSender(i)
	}
	if connectedCount == 0 {
		if closeErr := p.Close(); closeErr != nil {
			LogWarn("failed to clean up producer after connection failure: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to connect to any partition")
	}

	go p.batchStateGC()
	p.closeOnContext(ctx)
	return p, nil
}

func (p *Producer) closeOnContext(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			_ = p.Close()
		case <-p.done:
		}
	}()
}

func (p *Producer) fetchMetadata() {
	addrs := p.config.BrokerAddrs
	if len(addrs) == 0 {
		return
	}
	for _, addr := range addrs {
		conn, err := dialAuthenticatedWireConnection(
			context.Background(), addr, 5*time.Second,
			p.config.HandshakeTimeoutMS, p.config.CompressionType, p.wireTLSConfig(),
			p.config.Principal, p.config.AuthToken,
		)
		if err != nil {
			continue
		}
		cmd := fmt.Sprintf("METADATA topic=%s", p.config.Topic)
		if err := WriteWithLength(conn, []byte(cmd)); err != nil {
			_ = conn.Close()
			continue
		}
		resp, err := ReadWithLength(conn)
		_ = conn.Close()
		if err != nil {
			continue
		}
		respStr := strings.TrimSpace(string(resp))
		if !strings.HasPrefix(respStr, "OK") {
			continue
		}
		for _, part := range strings.Fields(respStr) {
			if strings.HasPrefix(part, "leaders=") {
				addrs := strings.Split(strings.TrimPrefix(part, "leaders="), ",")
				p.partitionMu.Lock()
				for i, a := range addrs {
					if a = strings.TrimSpace(a); a != "" {
						p.partitionLeaders[i] = a
					}
				}
				p.partitionMu.Unlock()
				return
			}
		}
		return
	}
}

func (p *Producer) getPartitionLeaderAddr(partition int) string {
	p.partitionMu.RLock()
	defer p.partitionMu.RUnlock()
	return p.partitionLeaders[partition]
}

func (p *Producer) nextPartition() int {
	partitionCount, ok := util.SafeIntToUint64(p.partitions)
	if !ok || partitionCount == 0 {
		return 0
	}
	index, ok := util.SafeUint64ToInt((p.rr.Add(1) - 1) % partitionCount)
	if !ok {
		return 0
	}
	return index
}

// TopicCleanupPolicy selects broker maintenance for a topic.
type TopicCleanupPolicy string

const (
	TopicCleanupDelete        TopicCleanupPolicy = "delete"
	TopicCleanupCompact       TopicCleanupPolicy = "compact"
	TopicCleanupDeleteCompact TopicCleanupPolicy = "delete,compact"
)

// TopicOptions defines optional CREATE settings for a topic.
type TopicOptions struct {
	Partitions        int
	CleanupPolicy     TopicCleanupPolicy
	RetentionHours    int
	RetentionBytes    int64
	MinInSyncReplicas int
	Partitioner       string
	AuthPolicy        string
	ReadACL           []string
	WriteACL          []string
}

func (p *Producer) CreateTopic(topic string, partitions int) error {
	return p.CreateTopicWithOptions(topic, TopicOptions{Partitions: partitions})
}

func (p *Producer) createConfiguredTopic() error {
	if p == nil || p.config == nil || !p.config.AutoCreateTopics {
		return nil
	}
	if err := p.CreateTopic(p.config.Topic, p.config.Partitions); err != nil {
		return fmt.Errorf("auto-create topic %q: %w", p.config.Topic, err)
	}
	return nil
}

// CreateTopicWithOptions creates or updates a topic with explicit policy options.
func (p *Producer) CreateTopicWithOptions(topic string, options TopicOptions) error {
	createCmd, err := buildCreateTopicCommand(topic, options, p.config.EnableIdempotence)
	if err != nil {
		return err
	}
	if len(p.config.BrokerAddrs) == 0 {
		return fmt.Errorf("no broker addresses available")
	}
	brokerAddr := p.config.BrokerAddrs[0]
	conn, err := dialAuthenticatedWireConnection(
		context.Background(), brokerAddr, 5*time.Second,
		p.config.HandshakeTimeoutMS, p.config.CompressionType, p.wireTLSConfig(),
		p.config.Principal, p.config.AuthToken,
	)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }()
	cmdBytes := []byte(createCmd)

	if err := WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return fmt.Errorf("unexpected create response: %s", respStr)
	}

	LogInfo("create topic %s partition %d", topic, options.Partitions)
	return nil
}

func buildCreateTopicCommand(topic string, options TopicOptions, idempotent bool) (string, error) {
	if err := validateSDKTopicName(topic); err != nil {
		return "", err
	}
	if options.Partitions <= 0 {
		return "", fmt.Errorf("partitions must be positive")
	}
	if options.RetentionHours < 0 {
		return "", fmt.Errorf("retention hours must be non-negative")
	}
	if options.RetentionBytes < 0 {
		return "", fmt.Errorf("retention bytes must be non-negative")
	}
	if options.MinInSyncReplicas < 0 {
		return "", fmt.Errorf("min in-sync replicas must be positive when set")
	}

	cleanupPolicy, err := normalizeSDKCleanupPolicy(options.CleanupPolicy)
	if err != nil {
		return "", err
	}
	switch options.Partitioner {
	case "", "hash_key", "round_robin":
	default:
		return "", fmt.Errorf("invalid partitioner %q", options.Partitioner)
	}
	switch options.AuthPolicy {
	case "", "open", "deny_write", "deny_read", "acl":
	default:
		return "", fmt.Errorf("invalid auth policy %q", options.AuthPolicy)
	}
	for _, acl := range append(append([]string(nil), options.ReadACL...), options.WriteACL...) {
		if !isSafeTopicOptionValue(acl, false) || strings.Contains(acl, ",") {
			return "", fmt.Errorf("invalid ACL principal %q", acl)
		}
	}

	command := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, options.Partitions)
	if idempotent {
		command += " idempotent=true"
	}
	if cleanupPolicy != "" {
		command += " cleanup_policy=" + cleanupPolicy
	}
	if options.RetentionHours != 0 {
		command += fmt.Sprintf(" retention_hours=%d", options.RetentionHours)
	}
	if options.RetentionBytes != 0 {
		command += fmt.Sprintf(" retention_bytes=%d", options.RetentionBytes)
	}
	if options.MinInSyncReplicas != 0 {
		command += fmt.Sprintf(" min_in_sync_replicas=%d", options.MinInSyncReplicas)
	}
	if options.Partitioner != "" {
		command += " partitioner=" + options.Partitioner
	}
	if options.AuthPolicy != "" {
		command += " auth_policy=" + options.AuthPolicy
	}
	if len(options.ReadACL) > 0 {
		command += " read_acl=" + strings.Join(options.ReadACL, ",")
	}
	if len(options.WriteACL) > 0 {
		command += " write_acl=" + strings.Join(options.WriteACL, ",")
	}
	return command, nil
}

func validateSDKTopicName(name string) error {
	if name == "" || len(name) > 249 || name == "." || name == ".." {
		return fmt.Errorf("invalid topic name %q", name)
	}
	for _, char := range name {
		switch {
		case char >= 'a' && char <= 'z':
		case char >= 'A' && char <= 'Z':
		case char >= '0' && char <= '9':
		case char == '.', char == '_', char == '-', char == '=':
		default:
			return fmt.Errorf("invalid topic name %q", name)
		}
	}
	return nil
}

func normalizeSDKCleanupPolicy(value TopicCleanupPolicy) (string, error) {
	switch strings.ToLower(strings.TrimSpace(string(value))) {
	case "":
		return "", nil
	case "delete":
		return "delete", nil
	case "compact":
		return "compact", nil
	case "delete,compact", "compact,delete":
		return "delete,compact", nil
	default:
		return "", fmt.Errorf("invalid cleanup policy %q", value)
	}
}

func isSafeTopicOptionValue(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	for _, r := range value {
		if r <= ' ' || r == '=' || r == '"' || r == '\\' {
			return false
		}
	}
	return true
}

// Send enqueues payload for delivery and returns the assigned sequence number.
func (p *Producer) Send(payload string) (uint64, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return 0, fmt.Errorf("send: %w", ErrProducerClosed)
	}

	p.closeMu.Lock()
	part := p.nextPartition()
	buf := p.buffers[part]
	p.closeMu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.closed {
		return 0, fmt.Errorf("partition %d buffer closed: %w", part, ErrProducerClosed)
	}

	if len(buf.msgs) >= p.config.BufferSize {
		return 0, fmt.Errorf("partition %d buffer full", part)
	}

	seqNum := p.client.NextSeqNum(part)
	producerID := p.client.ID
	if p.config.EnableIdempotence {
		producerID = fmt.Sprintf("%s-p%d", p.client.ID, part)
	}
	bm := Message{
		SeqNum:     seqNum,
		Payload:    payload,
		ProducerID: producerID,
		Epoch:      p.client.Epoch,
	}

	buf.msgs = append(buf.msgs, bm)
	buf.cond.Signal()

	return seqNum, nil
}

func (p *Producer) partitionSender(part int) {
	defer p.sendersWG.Done()

	buf := p.buffers[part]
	linger := time.Duration(p.config.LingerMS) * time.Millisecond

	timer := time.NewTimer(linger)
	defer timer.Stop()

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		select {
		case <-p.done:
			return
		default:
		}

		buf.mu.Lock()
		for len(buf.msgs) == 0 && len(buf.drainWaiters) == 0 && !buf.closed {
			buf.cond.Wait()
		}

		if len(buf.msgs) == 0 {
			waiters := buf.drainWaiters
			buf.drainWaiters = nil
			select {
			case <-buf.drainWake:
			default:
			}
			closed := buf.closed
			buf.mu.Unlock()

			for _, waiter := range waiters {
				close(waiter)
			}
			if closed {
				return
			}
			continue
		}

		var batch []Message
		if len(buf.msgs) >= p.config.BatchSize || len(buf.drainWaiters) > 0 || buf.closed {
			batch = p.extract(buf)
			buf.mu.Unlock()
		} else {
			buf.mu.Unlock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(linger)

			select {
			case <-timer.C:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = p.extractAny(buf)
				}
				buf.mu.Unlock()
			case <-buf.drainWake:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = p.extract(buf)
				}
				buf.mu.Unlock()
			case <-p.done:
				return
			}
		}

		if len(batch) > 0 {
			p.sendBatch(part, batch)
		}
	}
}

func (p *Producer) extract(buf *partitionBuffer) []Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := p.config.BatchSize
	if len(buf.msgs) < n {
		n = len(buf.msgs)
	}

	batch := make([]Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[n:]

	return batch
}

func (p *Producer) extractAny(buf *partitionBuffer) []Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := len(buf.msgs)
	batch := make([]Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[:0]

	return batch
}

// ─── Flush / Stats ────────────────────────────────────────────────────────────

func (p *Producer) Flush() error {
	timeout := p.flushTimeout()

	p.closeMu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeMu.Unlock()
		return p.deliveryError()
	}
	waiters := p.requestDrain(false)
	p.closeMu.Unlock()

	if !waitForDrain(waiters, timeout) {
		return errors.Join(fmt.Errorf("producer flush timeout after %v", timeout), p.deliveryError())
	}
	return p.deliveryError()
}

func (p *Producer) recordDeliveryFailure(err error) {
	if err == nil {
		return
	}
	p.deliveryMu.Lock()
	if p.deliveryErr == nil {
		p.deliveryErr = err
	}
	p.deliveryMu.Unlock()
}

func (p *Producer) deliveryError() error {
	p.deliveryMu.Lock()
	defer p.deliveryMu.Unlock()
	return p.deliveryErr
}

func (p *Producer) flushTimeout() time.Duration {
	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

// requestDrain must be called while closeMu is held so a concurrent Close cannot
// let a sender exit before its drain waiter is registered.
func (p *Producer) requestDrain(markClosed bool) []chan struct{} {
	waiters := make([]chan struct{}, 0, len(p.buffers))
	for _, buf := range p.buffers {
		waiter := make(chan struct{})
		buf.mu.Lock()
		if markClosed {
			buf.closed = true
		}
		buf.drainWaiters = append(buf.drainWaiters, waiter)
		buf.cond.Broadcast()
		select {
		case buf.drainWake <- struct{}{}:
		default:
		}
		buf.mu.Unlock()
		waiters = append(waiters, waiter)
	}
	return waiters
}

func waitForDrain(waiters []chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for _, waiter := range waiters {
		select {
		case <-waiter:
		case <-timer.C:
			return false
		}
	}
	return true
}

// FlushBenchmark waits until all expectedTotal messages are acknowledged or timeout expires.
func (p *Producer) FlushBenchmark(expectedTotal int) {
	expected, ok := util.SafeIntToUint64(expectedTotal)
	if !ok {
		LogError("invalid negative benchmark message count: %d", expectedTotal)
		return
	}
	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}

	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	start := time.Now()

	for time.Now().Before(deadline) {
		allInFlightClear := true
		for part := 0; part < p.partitions; part++ {
			if atomic.LoadInt32(&p.inFlight[part]) > 0 {
				LogDebug("Partition %d still has in-flight messages", part)
				allInFlightClear = false
				break
			}
		}

		if allInFlightClear {
			ackedSoFar := p.GetUniqueAckCount()
			totalPending := 0
			for part := 0; part < p.partitions; part++ {
				p.partitionBatchMus[part].Lock()
				totalPending += len(p.partitionBatchStates[part])
				p.partitionBatchMus[part].Unlock()
			}

			if ackedSoFar >= expected && totalPending == 0 {
				LogInfo("FlushBenchmark completed — all %d messages acknowledged (%.3fs)", expectedTotal, time.Since(start).Seconds())
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	LogWarn("FlushBenchmark timeout after %v. Only %d/%d messages acknowledged.", timeout, p.GetUniqueAckCount(), expectedTotal)
}

func (p *Producer) VerifySentSequences(expectedCount int) error {
	totalSent := 0
	for part := 0; part < p.partitions; part++ {
		p.partitionSentMus[part].Lock()
		totalSent += len(p.partitionSentSeqs[part])
		p.partitionSentMus[part].Unlock()
	}

	if totalSent != expectedCount {
		return fmt.Errorf("expected %d messages sent, got %d", expectedCount, totalSent)
	}

	LogInfo("All %d sequences sent successfully across all partitions", expectedCount)
	return nil
}

func (p *Producer) GetPartitionStats() []PartitionStat {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	stats := make([]PartitionStat, 0, p.partitions)
	for part := 0; part < p.partitions; part++ {
		count := p.bmTotalCount[part]
		totalTime := p.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = totalTime / time.Duration(count)
		}
		stats = append(stats, PartitionStat{
			PartitionID: part,
			BatchCount:  count,
			AvgDuration: avg,
		})
	}
	return stats
}

func (p *Producer) GetLatencies() []time.Duration {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	res := make([]time.Duration, len(p.bmLatencies))
	copy(res, p.bmLatencies)
	return res
}

func (p *Producer) GetUniqueAckCount() uint64 {
	return p.uniqueCount.Load()
}

func (p *Producer) GetAttemptsCount() uint64 {
	return p.attemptsCount.Load()
}

func (p *Producer) GetPartitionCount() int {
	return p.partitions
}

func (p *Producer) batchStateGC() {
	for {
		select {
		case <-p.gcTicker.C:
			now := time.Now()
			ackedCutoff := now.Add(-1 * time.Minute)
			staleCutoff := now.Add(-5 * time.Minute)

			for part := 0; part < p.partitions; part++ {
				p.partitionBatchMus[part].Lock()

				for id, st := range p.partitionBatchStates[part] {
					if st.Acked && st.SentTime.Before(ackedCutoff) {
						delete(p.partitionBatchStates[part], id)
						continue
					}

					if !st.Acked && st.SentTime.Before(staleCutoff) {
						LogWarn("GC: Dropping unacked stale batch: %s", id)
						delete(p.partitionBatchStates[part], id)
					}
				}
				p.partitionBatchMus[part].Unlock()
			}
		case <-p.done:
			return
		}
	}
}

func (p *Producer) Close() (result error) {
	p.closeMu.Lock()
	if p.closeDone == nil {
		p.closeDone = make(chan struct{})
	}
	if atomic.LoadInt32(&p.closed) == 1 {
		closeDone := p.closeDone
		p.closeMu.Unlock()
		<-closeDone
		p.closeMu.Lock()
		defer p.closeMu.Unlock()
		return p.closeErr
	}
	atomic.StoreInt32(&p.closed, 1)
	waiters := p.requestDrain(true)
	p.closeMu.Unlock()
	defer func() {
		p.closeMu.Lock()
		p.closeErr = result
		close(p.closeDone)
		p.closeMu.Unlock()
	}()

	timeout := p.flushTimeout()
	drained := waitForDrain(waiters, timeout)
	if !drained {
		LogWarn("Close drain timeout after %v", timeout)
	}

	p.gcTicker.Stop()
	close(p.done)
	clientErr := p.client.Close()
	p.sendersWG.Wait()

	if !drained {
		return errors.Join(fmt.Errorf("producer close: drain timeout after %v", timeout), clientErr, p.deliveryError())
	}
	return errors.Join(clientErr, p.deliveryError())
}
