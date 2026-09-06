package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

const DefaultMaxPollRecords = 8192
const STREAM_DATA_SIGNAL = "STREAM_DATA"
const transactionStateLockStripes = 256

type CommandHandler struct {
	TopicManager  *topic.TopicManager
	Config        *config.Config
	Coordinator   *coordinator.Coordinator
	StreamManager *stream.StreamManager
	Cluster       *clusterController.ClusterController
	ESHandler     *eventsource.Handler
	TxnManager    *transaction.Manager
	commands      []commandEntry

	coordCache               map[string]coordCacheEntry
	coordCacheMu             sync.RWMutex
	txnApplyMu               sync.Mutex
	topicLifecycleMu         sync.RWMutex
	topicCreateMu            sync.Mutex
	txnJournal               *transaction.Journal
	transactionStateSyncHook func(string) error
	transactionStateLocks    [transactionStateLockStripes]sync.Mutex
	partitionWriteLocks      sync.Map // map[string]*sync.Mutex
	partitionPreparedEpochs  sync.Map // map[string]partitionLeadershipFence
	replication              *partitionReplicationCoordinator
}

func (ch *CommandHandler) transactionStateLock(transactionalID string) *sync.Mutex {
	stripe := util.GenerateID(transactionalID) % transactionStateLockStripes
	return &ch.transactionStateLocks[stripe]
}

func (ch *CommandHandler) partitionWriteLock(topicName string, partitionID int) *sync.Mutex {
	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	lock, _ := ch.partitionWriteLocks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func transactionalIDExpiration(cfg *config.Config) time.Duration {
	if cfg == nil || cfg.TransactionalIDExpirationMS <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(cfg.TransactionalIDExpirationMS) * time.Millisecond
}

// commandEntry defines a single command routing rule.
type commandEntry struct {
	prefix      string
	exact       bool
	helpOrder   int
	internal    bool
	permissions []string
	handler     func(cmd string, ctx *ClientContext) string
}

func (entry commandEntry) name() string {
	return strings.TrimSpace(entry.prefix)
}

func (entry commandEntry) matches(input commandInput) bool {
	if entry.exact {
		return strings.EqualFold(input.Raw, entry.prefix)
	}
	return strings.HasPrefix(input.Upper, entry.prefix)
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    uint64
}

func NewCommandHandler(
	tm *topic.TopicManager,
	cfg *config.Config,
	cd *coordinator.Coordinator,
	sm *stream.StreamManager,
	cc *clusterController.ClusterController,
) *CommandHandler {
	ch := &CommandHandler{
		TopicManager:  tm,
		Config:        cfg,
		Coordinator:   cd,
		StreamManager: sm,
		coordCache:    make(map[string]coordCacheEntry),
		Cluster:       cc,
		ESHandler:     eventsource.NewHandler(tm),
		TxnManager:    transaction.NewManagerWithExpiration(transactionalIDExpiration(cfg)),
	}
	if tm != nil {
		tm.SetTransactionDecisionResolver(ch.TxnManager)
		tm.SetDeleteHook(ch.ESHandler.DeleteTopic)
		if cfg != nil && cfg.EnabledDistribution {
			tm.SetDistributedCompactionGate(ch.distributedCompactionAllowed)
		}
	}
	if cc != nil && cc.RaftManager != nil {
		if fsm := cc.RaftManager.GetFSM(); fsm != nil {
			fsm.SetTransactionManager(ch.TxnManager)
		}
	}
	if cc != nil {
		capacity := 1
		if cfg != nil && cfg.ChannelBufferSize > 0 {
			capacity = cfg.ChannelBufferSize
		}
		ch.replication = newPartitionReplicationCoordinator(capacity, clusterPartitionReplicationExecutor{handler: ch})
	}
	ch.commands = []commandEntry{
		{prefix: "AUTH ", exact: false, handler: func(cmd string, ctx *ClientContext) string { return ch.handleAuth(cmd, ctx) }},
		{prefix: "HELP", exact: true, helpOrder: 38, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHelp() }},
		{prefix: "LIST_CLUSTER", exact: true, helpOrder: 35, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListCluster() }},
		{prefix: "CLUSTER_STATUS", exact: true, helpOrder: 36, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleClusterStatus() }},
		{prefix: "ELECT_LEADER ", exact: false, helpOrder: 37, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleElectLeader(cmd, ctx) }},
		{prefix: "LIST", exact: true, helpOrder: 4, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleList(ctx) }},
		{prefix: "LIST_GROUPS", exact: true, helpOrder: 24, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListGroups() }},
		{prefix: "CREATE ", exact: false, helpOrder: 1, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCreate(cmd, ctx) }},
		{prefix: "ALTER_TOPIC_CONFIG ", exact: false, helpOrder: 40, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleAlterTopicConfig(cmd, ctx) }},
		{prefix: "DELETE ", exact: false, helpOrder: 2, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDelete(cmd, ctx) }},
		{prefix: "TRUNCATE ", exact: false, helpOrder: 3, permissions: []string{PermissionAdmin}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleTruncate(cmd, ctx) }},
		{prefix: "PUBLISH ", exact: false, helpOrder: 5, permissions: []string{PermissionTopicWrite}, handler: func(cmd string, ctx *ClientContext) string { return ch.handlePublish(cmd, ctx) }},
		{prefix: "CONSUME ", exact: false, helpOrder: 6, permissions: []string{PermissionTopicRead, PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.validateConsumeSyntax(cmd, cmd) }},
		{prefix: "STREAM ", exact: false, helpOrder: 7, permissions: []string{PermissionTopicRead, PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.validateStreamSyntax(cmd, cmd) }},
		{prefix: "REGISTER_GROUP ", exact: false, helpOrder: 22, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRegisterGroup(cmd) }},
		{prefix: "JOIN_GROUP ", exact: false, helpOrder: 8, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleJoinGroup(cmd, ctx) }},
		{prefix: "SYNC_GROUP ", exact: false, helpOrder: 9, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSyncGroup(cmd) }},
		{prefix: "LEAVE_GROUP ", exact: false, helpOrder: 10, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleLeaveGroup(cmd) }},
		{prefix: "FETCH_OFFSET ", exact: false, helpOrder: 14, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFetchOffset(cmd) }},
		{prefix: "LIST_OFFSETS", exact: true, helpOrder: 15, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListOffsets(cmd, ctx) }},
		{prefix: "LIST_OFFSETS ", exact: false, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListOffsets(cmd, ctx) }},
		{prefix: "GROUP_STATUS ", exact: false, helpOrder: 23, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleGroupStatus(cmd) }},
		{prefix: "DESCRIBE ", exact: false, helpOrder: 25, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleDescribeTopic(cmd, ctx) }},
		{prefix: "HEARTBEAT ", exact: false, helpOrder: 11, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleHeartbeat(cmd) }},
		{prefix: "COMMIT_OFFSET ", exact: false, helpOrder: 12, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCommitOffset(cmd) }},
		{prefix: "BATCH_COMMIT ", exact: false, helpOrder: 13, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleBatchCommit(cmd) }},
		{prefix: "INIT_PRODUCER_ID ", exact: false, helpOrder: 16, permissions: []string{PermissionTransaction}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleInitProducerID(cmd) }},
		{prefix: "BEGIN_TXN ", exact: false, helpOrder: 17, permissions: []string{PermissionTransaction}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleBeginTxn(cmd) }},
		{prefix: "TXN_PUBLISH ", exact: false, helpOrder: 18, permissions: []string{PermissionTransaction, PermissionTopicWrite}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleTxnPublish(cmd, ctx) }},
		{prefix: "SEND_OFFSETS_TO_TXN ", exact: false, helpOrder: 19, permissions: []string{PermissionTransaction, PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSendOffsetsToTxn(cmd) }},
		{prefix: "END_TXN ", exact: false, helpOrder: 20, permissions: []string{PermissionTransaction}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleEndTxn(cmd) }},
		{prefix: "TXN_STATUS ", exact: false, helpOrder: 21, permissions: []string{PermissionTransaction}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleTxnStatus(cmd) }},
		{prefix: "APPEND_STREAM ", exact: false, helpOrder: 26, permissions: []string{PermissionTopicWrite}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleAppendStream(cmd) }},
		{prefix: "STREAM_VERSION ", exact: false, helpOrder: 30, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string {
			return ch.handleEventSourceRoutedCommand(cmd, "STREAM_VERSION ", ch.ESHandler.HandleStreamVersion)
		}},
		{prefix: "SAVE_SNAPSHOT ", exact: false, helpOrder: 28, permissions: []string{PermissionTopicWrite}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleSaveSnapshot(cmd) }},
		{prefix: "READ_SNAPSHOT ", exact: false, helpOrder: 29, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string {
			return ch.handleEventSourceRoutedCommand(cmd, "READ_SNAPSHOT ", ch.ESHandler.HandleReadSnapshot)
		}},
		{prefix: "READ_STREAM ", exact: false, helpOrder: 27, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return STREAM_DATA_SIGNAL }},
		{prefix: "METADATA ", exact: false, helpOrder: 31, permissions: []string{PermissionTopicRead}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleMetadata(cmd) }},
		{prefix: "FIND_COORDINATOR ", exact: false, helpOrder: 32, permissions: []string{PermissionGroup}, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFindCoordinator(cmd) }},
		{prefix: "REPLICATE_MESSAGE ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleReplicateMessage(cmd) }},
		{prefix: "REPLICATE_SNAPSHOT ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleReplicateSnapshot(cmd) }},
		{prefix: "LIST_SNAPSHOTS ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleListSnapshots(cmd) }},
		{prefix: "FETCH_SNAPSHOT ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleFetchSnapshot(cmd) }},
		{prefix: "CATCHUP_SNAPSHOTS ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleCatchupSnapshots(cmd) }},
		{prefix: "RAFT_APPLY ", exact: false, internal: true, handler: func(cmd string, ctx *ClientContext) string { return ch.handleRaftApply(cmd) }},
	}
	return ch
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if protocol.IsErrorResponse(response) {
		status = "FAILURE"
	}
	cleanCmd := redactCommandSecrets(cmd)
	cleanResponse := redactCommandSecrets(strings.ReplaceAll(response, "\n", " "))
	util.Debug("status: '%s', command: '%s' to Response '%s'", status, cleanCmd, cleanResponse)
}

func redactCommandSecrets(s string) string {
	s = redactCommandTailValue(s, "message=")
	s = redactCommandTailValue(s, "payload=")
	keys := []string{"internal_token=", "auth_token=", "token="}
	for _, key := range keys {
		s = redactOneCommandSecret(s, key)
	}
	return s
}

func redactCommandTailValue(s, key string) string {
	idx := strings.Index(strings.ToLower(s), strings.ToLower(key))
	if idx == -1 {
		return s
	}
	return s[:idx] + key + "<redacted>"
}

func redactOneCommandSecret(s, key string) string {
	idx := strings.Index(s, key)
	if idx == -1 {
		return s
	}

	var b strings.Builder
	for idx != -1 {
		b.WriteString(s[:idx])
		b.WriteString(key)
		b.WriteString("<redacted>")
		rest := s[idx+len(key):]
		end := 0
		for end < len(rest) && rest[end] > ' ' {
			end++
		}
		s = rest[end:]
		idx = strings.Index(s, key)
	}
	b.WriteString(s)
	return b.String()
}

// HandleCommandContext applies a request-scoped cancellation and deadline while
// preserving the connection-level context for subsequent commands.
func (ch *CommandHandler) HandleCommandContext(requestCtx context.Context, rawCmd string, clientCtx *ClientContext) string {
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	if clientCtx == nil {
		return ch.HandleCommand(rawCmd, nil)
	}
	previous := clientCtx.RequestContext()
	clientCtx.SetRequestContext(requestCtx)
	defer clientCtx.SetRequestContext(previous)
	return ch.HandleCommand(rawCmd, clientCtx)
}

// HandleCommand processes non-streaming commands and returns a signal for streaming commands.
func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) (response string) {
	started := time.Now()
	input := decodeCommandInput(rawCmd)
	if input.Name == "DELETE" || input.Name == "TRUNCATE" {
		ch.topicLifecycleMu.Lock()
		defer ch.topicLifecycleMu.Unlock()
	} else {
		ch.topicLifecycleMu.RLock()
		defer ch.topicLifecycleMu.RUnlock()
	}
	if input.Name == "CREATE" {
		ch.topicCreateMu.Lock()
		defer ch.topicCreateMu.Unlock()
	}
	cmd := input.Raw
	commandName := ch.metricCommandNameInput(input)
	defer func() {
		metrics.RecordCommand(commandName, response, time.Since(started))
	}()

	if cmd == "" {
		resp := "ERROR: empty_command"
		ch.logCommandResult(rawCmd, resp)
		return resp
	}

	if name, ok := ch.internalCommandName(input); ok {
		if resp := ch.authorizeInternalCommand(name, input); resp != "" {
			return ch.fail(rawCmd, resp)
		}
	}

	if resp := ch.authorizeClientCommand(input, ctx); resp != "" {
		return ch.fail(rawCmd, resp)
	}
	if topicName := input.Args["topic"]; topicName != "" && ch.TopicManager != nil && ch.TopicManager.IsTruncationPending(topicName) {
		switch input.Name {
		case "TRUNCATE":
		default:
			return ch.fail(rawCmd, fmt.Sprintf("ERROR: topic_lifecycle_pending topic=%s operation=truncate", topicName))
		}
	}

	response = ch.handleCommandByType(input, ctx)
	ch.logCommandResult(rawCmd, response)
	return response
}

func (ch *CommandHandler) metricCommandName(cmd string) string {
	return ch.metricCommandNameInput(decodeCommandInput(cmd))
}

func (ch *CommandHandler) metricCommandNameInput(input commandInput) string {
	name := input.Name
	if name == "" {
		return "EMPTY"
	}
	if name == "STREAM" || name == "CONSUME" {
		return name
	}
	for _, entry := range ch.commands {
		if name == entry.name() {
			return name
		}
	}
	return "UNKNOWN"
}

// handleCommandByType dispatches to the matching command handler from the registry.
func (ch *CommandHandler) handleCommandByType(input commandInput, ctx *ClientContext) string {
	for _, entry := range ch.commands {
		if entry.matches(input) {
			return entry.handler(input.Raw, ctx)
		}
	}
	return fmt.Sprintf("ERROR: unknown_command command=%q", input.Raw)
}

func (ch *CommandHandler) internalCommandName(input commandInput) (string, bool) {
	for _, entry := range ch.commands {
		if entry.internal && entry.matches(input) {
			return entry.name(), true
		}
	}
	return "", false
}

func (ch *CommandHandler) authorizeInternalCommand(name string, input commandInput) string {
	if ch == nil || ch.Config == nil || !ch.Config.EnabledDistribution {
		return ""
	}
	token := ch.Config.InternalAuthToken
	if token == "" {
		return fmt.Sprintf("ERROR: internal_auth_not_configured command=%s", name)
	}
	if !constantTimeStringEqual(input.Args["internal_token"], token) {
		return fmt.Sprintf("ERROR: internal_command_unauthorized command=%s", name)
	}
	return ""
}

func (ch *CommandHandler) internalAuthPrefix() string {
	if ch != nil && ch.Config != nil && ch.Config.InternalAuthToken != "" {
		return "internal_token=" + ch.Config.InternalAuthToken + " "
	}
	return ""
}

func (ch *CommandHandler) fail(raw, msg string) string {
	ch.logCommandResult(raw, msg)
	return msg
}

func (ch *CommandHandler) errorResponse(msg string) string {
	return fmt.Sprintf("ERROR: broker_error reason=%q", msg)
}

func (ch *CommandHandler) replicationErrorResponse(offset uint64, err error) string {
	code := "replication_unavailable"
	if isReplicationFenceError(err) {
		code = "PARTITION_LEADER_FENCED"
	}
	return fmt.Sprintf("ERROR: %s offset=%d reason=%q", code, offset, err)
}

// Close releases resources held by the command handler (e.g., event-sourcing indexes and snapshots).
func (ch *CommandHandler) Close() error {
	if ch.StreamManager != nil {
		ch.StreamManager.Close()
	}
	if ch.replication != nil {
		ch.replication.close()
	}
	if ch.ESHandler != nil {
		return ch.ESHandler.Close()
	}
	return nil
}

func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)

	messageIdx := strings.Index(argsStr, "message=")

	if messageIdx != -1 {
		beforeMessage := argsStr[:messageIdx]
		parts := strings.Fields(beforeMessage)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
		result["message"] = strings.TrimSpace(argsStr[messageIdx+8:])
	} else {
		parts := strings.Fields(argsStr)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
	}
	return result
}
