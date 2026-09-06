package controller

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

var ErrStreamRejected = errors.New("stream rejected")

const (
	ReadIsolationCommitted   = "read_committed"
	ReadIsolationUncommitted = "read_uncommitted"
	MaxConsumeBatchRecords   = 1024
	MaxConsumeWait           = 30 * time.Second
)

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	if err := ctx.RequestContext().Err(); err != nil {
		return 0, err
	}
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	argsMap := parseKeyValueArgs(rawCmd[8:])
	if authResp := ch.authenticateInline(argsMap, ctx); authResp != "" {
		return 0, fmt.Errorf("%s", authResp)
	}
	if authResp := ch.authorizeClientPermissions("CONSUME", argsMap, ctx, PermissionTopicRead, PermissionGroup); authResp != "" {
		return 0, fmt.Errorf("%s", authResp)
	}
	if err := ch.validateConsumeArgs(argsMap); err != nil {
		return 0, err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return 0, err
	}
	cArgs.readBudget, err = util.NewBatchReadBudget(cArgs.TopicName, cArgs.PartitionID, wire.MaxFramePayload)
	if err != nil {
		return 0, err
	}
	previousOffsets := make(map[string]uint64, len(ctx.OffsetCache))
	for key, offset := range ctx.OffsetCache {
		previousOffsets[key] = offset
	}
	completed := false
	defer func() {
		if !completed {
			ctx.OffsetCache = previousOffsets
		}
	}()
	if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
		if err.Error() == "not partition leader" {
			return 0, nil
		}
		return 0, err
	}

	matchedTopics, err := ch.matchTopicPattern(cArgs.TopicName)
	if err != nil {
		return 0, err
	}

	if len(matchedTopics) == 0 {
		return 0, fmt.Errorf("no assigned topics match pattern '%s'", cArgs.TopicName)
	}

	totalStreamed := 0
	var allMessages []types.Message

	for _, tName := range matchedTopics {
		if totalStreamed >= cArgs.BatchSize {
			break
		}

		remainingBatch := cArgs.BatchSize - totalStreamed
		messages, err := ch.readFromTopic(tName, cArgs, ctx, remainingBatch)
		if err != nil {
			if ch.writeConsumeReadError(conn, err) {
				return totalStreamed, nil
			}
			return totalStreamed, err
		}
		if len(messages) > 0 {
			allMessages = append(allMessages, messages...)
			totalStreamed += len(messages)
		}
	}

	if totalStreamed == 0 && cArgs.WaitTimeout > 0 {
		startTime := time.Now()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for time.Since(startTime) < cArgs.WaitTimeout {
			select {
			case <-ctx.RequestContext().Done():
				return 0, ctx.RequestContext().Err()
			case <-ticker.C:
			}
			for _, tName := range matchedTopics {
				messages, err := ch.readFromTopic(tName, cArgs, ctx, cArgs.BatchSize)
				if err != nil {
					if ch.writeConsumeReadError(conn, err) {
						return 0, nil
					}
					return 0, err
				}
				if len(messages) > 0 {
					allMessages = append(allMessages, messages...)
					totalStreamed += len(messages)
					goto sendBatch
				}
			}
		}
	}

sendBatch:
	batchData, err := util.EncodeBatchMessages(cArgs.TopicName, cArgs.PartitionID, "1", false, allMessages)
	if err != nil {
		return 0, fmt.Errorf("failed to encode batch: %w", err)
	}

	if err := util.WriteWithLength(conn, batchData); err != nil {
		_ = conn.Close()
		return 0, fmt.Errorf("failed to stream batch: %w", err)
	}

	completed = true
	return totalStreamed, nil
}

func (ch *CommandHandler) writeConsumeReadError(conn net.Conn, err error) bool {
	var offsetErr *types.OffsetOutOfRangeError
	if !errors.As(err, &offsetErr) {
		return false
	}

	resp := fmt.Sprintf("ERROR: OFFSET_OUT_OF_RANGE requested=%d earliest=%d latest=%d", offsetErr.Requested, offsetErr.Earliest, offsetErr.Latest)
	if writeErr := util.WriteWithLength(conn, []byte(resp)); writeErr != nil {
		util.Error("failed to send offset out-of-range response: %v", writeErr)
	}
	return true
}
func (ch *CommandHandler) readFromTopic(topicName string, cArgs CommonArgs, ctx *ClientContext, batchSize int) ([]types.Message, error) {
	t, p, err := ch.getTopicAndPartition(topicName, cArgs.PartitionID)
	if err != nil {
		return nil, err
	}
	if authResp := ch.authorizeTopicRead(t.PolicySnapshot(), ctx); authResp != "" {
		return nil, fmt.Errorf("%s topic=%s", authResp, topicName)
	}

	cacheKey := fmt.Sprintf("%s-%d", topicName, cArgs.PartitionID)
	var currentOffset uint64
	if cached, ok := ctx.OffsetCache[cacheKey]; ok {
		currentOffset = cached
	} else {
		actualOffset, err := ch.resolveOffset(p, topicName, cArgs)
		if err != nil {
			return nil, err
		}
		currentOffset = actualOffset
	}

	budget := cArgs.readBudget
	if budget == nil {
		budget, err = util.NewBatchReadBudget(topicName, cArgs.PartitionID, wire.MaxFramePayload)
		if err != nil {
			return nil, err
		}
	}
	messages, err := budget.Read(currentOffset, batchSize, func(offset uint64, max int) ([]types.Message, error) {
		if err := ctx.RequestContext().Err(); err != nil {
			return nil, err
		}
		return readPartitionMessages(p, offset, max, cArgs.ReadIsolation)
	})
	if err != nil {
		util.Error("Failed to read messages from topic %s: %v", topicName, err)
		return nil, err
	}

	if len(messages) > 0 {
		lastMsg := messages[len(messages)-1]
		ctx.OffsetCache[cacheKey] = lastMsg.Offset + 1
	}

	return messages, nil
}

func (ch *CommandHandler) matchTopicPattern(pattern string) ([]string, error) {
	const maxPatternLength = 256
	if len(pattern) > maxPatternLength {
		return nil, fmt.Errorf("topic pattern exceeds maximum length of %d characters", maxPatternLength)
	}

	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		if ch.TopicManager.GetTopic(pattern) == nil {
			return nil, fmt.Errorf("topic '%s' does not exist", pattern)
		}
		return []string{pattern}, nil
	}

	escaped := regexp.QuoteMeta(pattern)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")
	regex, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		return nil, fmt.Errorf("invalid topic pattern: %w", err)
	}

	allTopics := ch.TopicManager.ListTopics()
	var matchedTopics []string
	for _, topic := range allTopics {
		if regex.MatchString(topic) {
			matchedTopics = append(matchedTopics, topic)
		}
	}

	sort.Strings(matchedTopics)
	if len(matchedTopics) == 0 {
		return nil, fmt.Errorf("no topics match pattern '%s'", pattern)
	}

	return matchedTopics, nil
}

func (ch *CommandHandler) HandleStreamCommand(conn net.Conn, rawCmd string, ctx *ClientContext) error {
	if len(rawCmd) < 7 {
		return fmt.Errorf("invalid STREAM command format")
	}

	argsMap := parseKeyValueArgs(rawCmd[7:])
	if authResp := ch.authenticateInline(argsMap, ctx); authResp != "" {
		return fmt.Errorf("%s", authResp)
	}
	if authResp := ch.authorizeClientPermissions("STREAM", argsMap, ctx, PermissionTopicRead, PermissionGroup); authResp != "" {
		return fmt.Errorf("%s", authResp)
	}
	if err := ch.validateStreamArgs(argsMap); err != nil {
		return err
	}
	cArgs, err := ch.parseCommonArgs(argsMap)
	if err != nil {
		return err
	}
	ctx.ConsumerGroup = cArgs.GroupName

	if err := ch.checkPartitionLeaderOrRedirect(conn, cArgs.TopicName, cArgs.PartitionID); err != nil {
		if err.Error() == "not partition leader" {
			return ErrStreamRejected
		}
		return err
	}

	ctx.Generation = cArgs.Generation
	ctx.MemberID = cArgs.MemberID

	t, p, err := ch.getTopicAndPartition(cArgs.TopicName, cArgs.PartitionID)
	if err != nil {
		return err
	}
	if authResp := ch.authorizeTopicRead(t.PolicySnapshot(), ctx); authResp != "" {
		return fmt.Errorf("%s topic=%s", authResp, cArgs.TopicName)
	}

	actualOffset, err := ch.resolveOffset(p, cArgs.TopicName, cArgs)
	if err != nil {
		return err
	}

	streamKey := fmt.Sprintf("%s:%d:%s", cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName)
	streamConn := stream.NewStreamConnection(conn, cArgs.TopicName, cArgs.PartitionID, cArgs.GroupName, actualOffset)
	streamConn.SetBatchSize(cArgs.BatchSize)
	streamConn.SetInterval(100 * time.Millisecond)

	streamConn.SetMessageSource(p.MessageNotification)

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		return readPartitionMessages(p, offset, max, cArgs.ReadIsolation)
	}

	return ch.StreamManager.AddStream(streamKey, streamConn, readFn)
}

func readPartitionMessages(p *topic.Partition, offset uint64, max int, isolation string) ([]types.Message, error) {
	if isolation == ReadIsolationUncommitted {
		return p.ReadMessages(offset, max)
	}
	return p.ReadCommitted(offset, max)
}

func (ch *CommandHandler) validateStreamSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[7:])
	if args["topic"] == "" || args["partition"] == "" || args["group"] == "" {
		return ch.fail(raw, "ERROR: invalid_stream_syntax")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return ch.fail(raw, "ERROR: "+err.Error())
	}
	return STREAM_DATA_SIGNAL
}

func (ch *CommandHandler) validateConsumeSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[8:])
	if args["topic"] == "" || args["partition"] == "" || args["offset"] == "" || args["member"] == "" {
		return ch.fail(raw, "ERROR: invalid_consume_syntax")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return ch.fail(raw, "ERROR: "+err.Error())
	}
	return STREAM_DATA_SIGNAL
}

// checkPartitionLeaderOrRedirect checks if this broker is the leader for the given partition.
// If not, writes a NOT_LEADER redirect with the partition leader's client address.
func (ch *CommandHandler) checkPartitionLeaderOrRedirect(conn net.Conn, topicName string, partitionID int) error {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.IsAuthorized(topicName, partitionID) {
		return nil
	}

	leaderAddr := ch.resolvePartitionLeaderAddr(topicName, partitionID)
	if leaderAddr == "" {
		errResp := fmt.Sprintf("ERROR: leader_not_found topic=%s partition=%d", topicName, partitionID)
		if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
			return fmt.Errorf("failed to send missing partition leader response: %w", err)
		}
		return fmt.Errorf("partition leader not found")
	}
	errResp := fmt.Sprintf("ERROR: NOT_LEADER leader=%s", leaderAddr)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send partition leader redirect: %w", err)
	}
	return fmt.Errorf("not partition leader")
}

// checkLeaderOrRedirect checks if this broker is the leader and writes a redirect error if not.
func (ch *CommandHandler) checkLeaderOrRedirect(conn net.Conn) error {
	if !ch.Config.EnabledDistribution || ch.Cluster == nil || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.RaftManager.IsLeader() {
		return nil
	}

	leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
	if leaderAddr == "" {
		return fmt.Errorf("no leader available")
	}

	serviceLeader := leaderAddr
	if host, _, splitErr := net.SplitHostPort(leaderAddr); splitErr == nil {
		if ch.Config.AdvertisedClientHost != "" {
			host = ch.Config.AdvertisedClientHost
		}
		port := ch.Config.BrokerPort
		if ch.Config.AdvertisedBrokerPort > 0 {
			port = ch.Config.AdvertisedBrokerPort
		}
		serviceLeader = net.JoinHostPort(host, strconv.Itoa(port))
	}

	errResp := fmt.Sprintf("ERROR: NOT_LEADER leader=%s", serviceLeader)
	util.Warn("leader redirect: %s", errResp)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send leader redirect: %w", err)
	}
	return fmt.Errorf("not leader")
}

func (ch *CommandHandler) getTopicAndPartition(topicName string, partitionID int) (*topic.Topic, *topic.Partition, error) {
	if ch.TopicManager == nil {
		return nil, nil, fmt.Errorf("topic_manager_not_available")
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return nil, nil, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	p, err := t.GetPartition(partitionID)
	if err != nil {
		return nil, nil, err
	}

	return t, p, nil
}

func (ch *CommandHandler) resolveConsumerGroup(groupName string) string {
	if groupName == "" || groupName == "-" {
		return "default-group"
	}
	return groupName
}

type CommonArgs struct {
	readBudget      *util.BatchReadBudget
	TopicName       string
	PartitionID     int
	GroupName       string
	MemberID        string
	Generation      int
	HasOffset       bool
	Offset          uint64
	BatchSize       int
	WaitTimeout     time.Duration
	AutoOffsetReset string
	ReadIsolation   string
}

func (ch *CommandHandler) parseCommonArgs(args map[string]string) (CommonArgs, error) {
	pID, err := strconv.Atoi(args["partition"])
	if err != nil && args["partition"] != "" {
		return CommonArgs{}, fmt.Errorf("invalid partition value: %s", args["partition"])
	}

	gen := -1
	genStr := args["generation"]
	if genStr != "" {
		g, err := strconv.Atoi(genStr)
		if err != nil {
			return CommonArgs{}, fmt.Errorf("invalid generation value: %s", genStr)
		}
		gen = g
	}

	offsetStr, hasOffsetKey := args["offset"]
	var offset uint64
	if hasOffsetKey && offsetStr != "" {
		val, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return CommonArgs{}, fmt.Errorf("invalid offset value: %s", offsetStr)
		}
		offset = val
	}

	batch := DefaultMaxPollRecords
	if raw, ok := args["batch"]; ok {
		b, err := strconv.Atoi(raw)
		if err != nil || b < 1 || b > MaxConsumeBatchRecords {
			return CommonArgs{}, fmt.Errorf("invalid batch: must be between 1 and %d", MaxConsumeBatchRecords)
		}
		batch = b
	}

	wait := 0 * time.Millisecond
	if raw, ok := args["wait_ms"]; ok {
		w, err := strconv.Atoi(raw)
		if err != nil || w < 0 || w > int(MaxConsumeWait/time.Millisecond) {
			return CommonArgs{}, fmt.Errorf("invalid wait_ms: must be between 0 and %d", MaxConsumeWait/time.Millisecond)
		}
		wait = time.Duration(w) * time.Millisecond
	}

	return CommonArgs{
		TopicName:       args["topic"],
		PartitionID:     pID,
		GroupName:       ch.resolveConsumerGroup(args["group"]),
		MemberID:        args["member"],
		Generation:      gen,
		HasOffset:       hasOffsetKey && offsetStr != "",
		Offset:          offset,
		BatchSize:       batch,
		WaitTimeout:     wait,
		AutoOffsetReset: strings.ToLower(args["autoOffsetReset"]),
		ReadIsolation:   normalizeReadIsolation(args["isolation"]),
	}, nil
}

func normalizeReadIsolation(value string) string {
	value = strings.ToLower(value)
	if value == "" {
		return ReadIsolationCommitted
	}
	return value
}

func (ch *CommandHandler) validateConsumeArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing_topic")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing_partition")
	}
	if args["offset"] == "" {
		return fmt.Errorf("missing_offset")
	}
	if args["member"] == "" {
		return fmt.Errorf("missing_member")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return err
	}
	return nil
}

func (ch *CommandHandler) validateStreamArgs(args map[string]string) error {
	if args["topic"] == "" {
		return fmt.Errorf("missing_topic")
	}
	if args["partition"] == "" {
		return fmt.Errorf("missing_partition")
	}
	if err := validateReadIsolation(args["isolation"]); err != nil {
		return err
	}
	return nil
}

func validateReadIsolation(value string) error {
	switch normalizeReadIsolation(value) {
	case ReadIsolationCommitted, ReadIsolationUncommitted:
		return nil
	default:
		return fmt.Errorf("invalid_isolation isolation=%s", value)
	}
}
