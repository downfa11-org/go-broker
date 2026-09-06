package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

type TransactionProducer struct {
	ProducerID string
	Epoch      int64
}

type TransactionStatus struct {
	State    string
	Messages int
	Offsets  int
}

func (bc *BrokerClient) InitTransactionProducer(transactionalID string) (TransactionProducer, error) {
	resp, err := bc.transactionCommand(fmt.Sprintf("INIT_PRODUCER_ID transactional_id=%s", transactionalID))
	if err != nil {
		return TransactionProducer{}, err
	}
	fields := responseFields(resp)
	epoch, err := strconv.ParseInt(fields["epoch"], 10, 64)
	if err != nil || fields["producerId"] == "" {
		return TransactionProducer{}, fmt.Errorf("invalid producer response: %s", resp)
	}
	return TransactionProducer{ProducerID: fields["producerId"], Epoch: epoch}, nil
}

func (bc *BrokerClient) BeginTransaction(transactionalID string, producer TransactionProducer) error {
	_, err := bc.transactionCommand(fmt.Sprintf("BEGIN_TXN transactional_id=%s producerId=%s epoch=%d", transactionalID, producer.ProducerID, producer.Epoch))
	return err
}

func (bc *BrokerClient) TransactionalPublish(transactionalID, topic string, partition int, producer TransactionProducer, sequence uint64, payload string) error {
	_, err := bc.transactionCommand(fmt.Sprintf("TXN_PUBLISH transactional_id=%s topic=%s partition=%d producerId=%s epoch=%d seqNum=%d message=%s", transactionalID, topic, partition, producer.ProducerID, producer.Epoch, sequence, payload))
	return err
}

func (bc *BrokerClient) SendOffsetsToTransaction(transactionalID, topic, group, member string, generation int, producer TransactionProducer, offsets map[int]uint64) error {
	pairs := make([]wire.OffsetPair, 0, len(offsets))
	for partition, offset := range offsets {
		pairs = append(pairs, wire.OffsetPair{Partition: partition, Offset: offset})
	}
	encodedOffsets, err := wire.EncodeOffsetPairs(pairs)
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("SEND_OFFSETS_TO_TXN transactional_id=%s producerId=%s epoch=%d topic=%s group=%s member=%s generation=%d offsets=%s", transactionalID, producer.ProducerID, producer.Epoch, topic, group, member, generation, encodedOffsets)
	_, err = bc.transactionCommand(cmd)
	return err
}

func (bc *BrokerClient) EndTransaction(transactionalID string, producer TransactionProducer, result string) error {
	_, err := bc.transactionCommand(fmt.Sprintf("END_TXN transactional_id=%s producerId=%s epoch=%d result=%s", transactionalID, producer.ProducerID, producer.Epoch, result))
	var brokerErr *wire.BrokerError
	if errors.As(err, &brokerErr) {
		return fmt.Errorf("%w: reason=%q sync_reason=%q", err, brokerErr.Fields["reason"], brokerErr.Fields["sync_reason"])
	}
	return err
}

func (bc *BrokerClient) GetTransactionStatus(transactionalID string) (TransactionStatus, error) {
	resp, err := bc.transactionCommand(fmt.Sprintf("TXN_STATUS transactional_id=%s", transactionalID))
	if err != nil {
		return TransactionStatus{}, err
	}
	fields := responseFields(resp)
	messages, msgErr := strconv.Atoi(fields["messages"])
	offsets, offsetErr := strconv.Atoi(fields["offsets"])
	if fields["state"] == "" || msgErr != nil || offsetErr != nil {
		return TransactionStatus{}, fmt.Errorf("invalid transaction status response: %s", resp)
	}
	return TransactionStatus{State: fields["state"], Messages: messages, Offsets: offsets}, nil
}

func (bc *BrokerClient) FindTransactionCoordinator(transactionalID string) (string, error) {
	resp, err := bc.SendCommand("", fmt.Sprintf("FIND_COORDINATOR transactional_id=%s", transactionalID), 5*time.Second)
	if err != nil {
		return "", err
	}
	coordinatorID := responseFields(resp)["coordinator_id"]
	if coordinatorID == "" {
		return "", fmt.Errorf("missing coordinator_id in response: %s", resp)
	}
	return coordinatorID, nil
}

func (bc *BrokerClient) transactionCommand(command string) (string, error) {
	resp, err := bc.SendCommand("", command, 10*time.Second)
	if err != nil {
		return "", err
	}
	if resp != "OK" && !strings.HasPrefix(resp, "OK ") {
		return "", fmt.Errorf("unexpected transaction response: %s", resp)
	}
	return resp, nil
}

func responseFields(resp string) map[string]string {
	fields := make(map[string]string)
	for _, field := range strings.Fields(resp) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			fields[key] = value
		}
	}
	return fields
}

// CreateTopic sends CREATE command to broker
func (bc *BrokerClient) CreateTopic(topic string, partitions int, idempotent bool) error {
	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d idempotent=%t", topic, partitions, idempotent)

	return bc.executeCommand("admin", createCmd)
}

// PublishIdempotent sends a message with idempotence metadata.
func (bc *BrokerClient) PublishIdempotent(topic, producerID string, seqNum uint64, epoch int64, payload, acks string, isIdempotent bool) error {
	return bc.PublishIdempotentToPartition(topic, producerID, -1, seqNum, epoch, payload, acks, isIdempotent)
}

func (bc *BrokerClient) PublishIdempotentToPartition(topic, producerID string, partition int, seqNum uint64, epoch int64, payload, acks string, isIdempotent bool) error {
	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s producerId=%s", topic, acks, producerID)
	if partition >= 0 {
		publishCmd += fmt.Sprintf(" partition=%d", partition)
	}
	publishCmd += fmt.Sprintf(" seqNum=%d epoch=%d", seqNum, epoch)
	if isIdempotent {
		publishCmd += " isIdempotent=true"
	}
	publishCmd += fmt.Sprintf(" message=%s", payload)

	if acks == "0" {
		addr, err := bc.getPrimaryAddr()
		if err != nil {
			return fmt.Errorf("publish idempotent failed: %w", err)
		}

		return sendOneWayWireCommand(addr, publishCmd)
	}

	return bc.executeCommand("admin", publishCmd)
}

// PublishSimple sends a message without idempotence
func (bc *BrokerClient) PublishSimple(topic, payload, acks string) error {
	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s message=%s", topic, acks, payload)

	if acks == "0" {
		addr, err := bc.getPrimaryAddr()
		if err != nil {
			return fmt.Errorf("publish simple failed: %w", err)
		}

		return sendOneWayWireCommand(addr, publishCmd)
	}

	return bc.executeCommand("admin", publishCmd)
}

// GetConsumerGroupStatus retrieves consumer group metadata from broker
func (bc *BrokerClient) GetConsumerGroupStatus(groupID string) (*ConsumerGroupStatus, error) {
	statusCmd := fmt.Sprintf("GROUP_STATUS group=%s", groupID)

	respStr, err := bc.SendCommand("admin", statusCmd, 2*time.Second)
	if err != nil {
		return nil, err
	}

	var status ConsumerGroupStatus
	if err := json.Unmarshal([]byte(respStr), &status); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return &status, nil
}

func (bc *BrokerClient) SendHeartbeat() error {
	bc.mu.Lock()
	memberID := bc.memberID
	topic := bc.topic
	consumerGroup := bc.consumerGroup
	generation := bc.generation
	bc.mu.Unlock()

	if memberID == "" {
		return fmt.Errorf("consumer not joined to group")
	}

	heartbeatCmd := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", topic, consumerGroup, memberID, generation)
	return bc.executeCommand("", heartbeatCmd)
}

func (bc *BrokerClient) DeleteTopic(topic string) error {
	deleteCmd := fmt.Sprintf("DELETE topic=%s", topic)
	return bc.executeCommand("admin", deleteCmd)
}

// CommitOffset commits an offset for a consumer group
func (bc *BrokerClient) CommitOffset(topic string, partition int, groupID string, offset uint64) error {
	bc.mu.Lock()
	generation := bc.generation
	memberID := bc.memberID
	bc.mu.Unlock()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
		topic, partition, groupID, offset, generation, memberID)
	return bc.executeCommand(topic, commitCmd)
}

// FetchCommittedOffset retrieves the committed offset for a consumer group
func (bc *BrokerClient) FetchCommittedOffset(topic string, partition int, groupID string) (uint64, error) {
	cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", topic, partition, groupID)

	respStr, err := bc.SendCommand("admin", cmd, 2*time.Second)
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(respStr, "OK") {
		for _, part := range strings.Fields(respStr) {
			if strings.HasPrefix(part, "offset=") {
				var offset uint64
				if n, err := fmt.Sscanf(part, "offset=%d", &offset); err != nil || n != 1 {
					return 0, fmt.Errorf("invalid offset response: %s", respStr)
				}
				return offset, nil
			}
		}
		return 0, fmt.Errorf("missing offset in response: %s", respStr)
	}

	return 0, fmt.Errorf("expected offset response, got: %s", respStr)
}

// joinGroup executes the JOIN_GROUP command and extracts generation and memberID.
func (bc *BrokerClient) JoinGroup(topic, group string) (int, string, error) {
	bc.mu.Lock()
	if bc.memberID == "" {
		bc.memberID = fmt.Sprintf("e2e-consumer-%d", time.Now().UnixNano())
	}
	initialMemberID := bc.memberID
	bc.mu.Unlock()

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", topic, group, initialMemberID)

	resp, err := bc.SendCommand("", joinCmd, 2*time.Second)
	if err != nil {
		return 0, "", fmt.Errorf("join group failed: %w", err)
	}

	var gen int
	var newMemberID string

	if strings.Contains(resp, "generation=") {
		parts := strings.Fields(resp)
		for _, part := range parts {
			if strings.HasPrefix(part, "generation=") {
				if n, scanErr := fmt.Sscanf(part, "generation=%d", &gen); scanErr != nil || n != 1 {
					util.Warn("JOIN_GROUP response did not contain valid generation info: %s", resp)
				}
			}
			if strings.HasPrefix(part, "member=") {
				// member=e2e-consumer-1765285832171409200-6241
				newMemberID = strings.TrimPrefix(part, "member=")
				if newMemberID == "" {
					util.Warn("JOIN_GROUP response contained empty member info: %s", resp)
				}
			}
		}
	}

	if newMemberID == "" {
		newMemberID = initialMemberID
	}

	bc.mu.Lock()
	bc.generation = gen
	bc.memberID = newMemberID
	bc.topic = topic
	bc.consumerGroup = group
	bc.mu.Unlock()

	return gen, newMemberID, nil
}

// syncGroup executes the SYNC_GROUP command to finalize partition assignment.
func (bc *BrokerClient) SyncGroup(topic, group string, generation int, memberID string) ([]int, error) {
	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d", topic, group, memberID, generation)

	resp, err := bc.SendCommand("", syncCmd, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("sync group failed: %w", err)
	}

	assignedPartitions := []int{}

	const assignmentPrefix = "assignments="
	assignmentIndex := strings.Index(resp, assignmentPrefix)

	if assignmentIndex != -1 {
		assignmentsStr := resp[assignmentIndex+len(assignmentPrefix):]
		start := strings.Index(assignmentsStr, "[")
		end := strings.Index(assignmentsStr, "]")

		if start != -1 && end != -1 && end > start {
			listStr := assignmentsStr[start+1 : end]
			listStr = strings.ReplaceAll(listStr, ",", " ")
			parts := strings.Fields(listStr)

			for _, pStr := range parts {
				pStr = strings.TrimSpace(pStr)
				if pStr != "" {
					var partitionID int
					if _, scanErr := fmt.Sscanf(pStr, "%d", &partitionID); scanErr == nil {
						assignedPartitions = append(assignedPartitions, partitionID)
					} else {
						return nil, fmt.Errorf("invalid partition ID format '%s': %w", pStr, scanErr)
					}
				}
			}
		}
	}

	return assignedPartitions, nil
}

// ConsumeMessages reads messages from a partition
func (bc *BrokerClient) ConsumeMessages(topic string, partition int, consumerGroup string, memberID string, generation int, timeout time.Duration) ([]string, error) {
	bc.requestMu.Lock()
	defer bc.requestMu.Unlock()
	if err := bc.connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	bc.mu.Lock()
	conn := bc.conn
	bc.mu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("connection not available after connect")
	}

	startOffset := 0
	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s autoOffsetReset=earliest member=%s generation=%d",
		topic, partition, startOffset, consumerGroup, memberID, generation)
	command, requestID, err := bc.writeWireCommand(consumeCmd)
	if err != nil {
		if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
			util.Warn("failed to reset read deadline after send failure: %v", resetErr)
		}
		return nil, fmt.Errorf("send consume command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}

	rawData, err := bc.readWirePayload(command, requestID)

	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
		util.Warn("failed to reset read deadline: %v", resetErr)
	}

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return []string{}, nil
		}
		return nil, fmt.Errorf("read batch message: %w", err)
	}

	if len(rawData) == 0 {
		return []string{}, nil
	}

	if len(rawData) >= 6 && string(rawData[:6]) == "ERROR:" {
		return nil, fmt.Errorf("broker error during consume: %s", string(rawData))
	}

	if wire.IsBatch(rawData) {
		batch, err := util.DecodeBatchMessages(rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode batch: %w", err)
		}

		var messages []string
		for _, msg := range batch.Messages {
			messages = append(messages, msg.Payload)
		}
		return messages, nil
	}

	respStr := string(rawData)
	if respStr == "OK" || strings.HasPrefix(respStr, "OK ") {
		return []string{}, nil
	}

	return nil, fmt.Errorf("unexpected response format: %s", respStr)
}

// ConsumeMessagesWithOffsets reads messages and their actual offsets from a partition
func (bc *BrokerClient) ConsumeMessagesWithOffsets(topic string, partition int, consumerGroup string, memberID string, generation int, timeout time.Duration) ([]string, []uint64, error) {
	startOffset, err := bc.FetchCommittedOffset(topic, partition, consumerGroup)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch committed offset: %w", err)
	}
	return bc.ConsumeMessagesAtOffsetWithOffsets(topic, partition, startOffset, consumerGroup, memberID, generation, timeout)
}

// ConsumeMessagesAtOffsetWithOffsets reads messages from an explicit logical
// offset without changing the client's broker selection first.
func (bc *BrokerClient) ConsumeMessagesAtOffsetWithOffsets(topic string, partition int, startOffset uint64, consumerGroup string, memberID string, generation int, timeout time.Duration) ([]string, []uint64, error) {
	if err := bc.connect(); err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}

	bc.requestMu.Lock()
	defer bc.requestMu.Unlock()
	bc.mu.Lock()
	conn := bc.conn
	bc.mu.Unlock()
	if conn == nil {
		return nil, nil, fmt.Errorf("connection not available after connect")
	}

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s autoOffsetReset=earliest member=%s generation=%d", topic, partition, startOffset, consumerGroup, memberID, generation)
	command, requestID, err := bc.writeWireCommand(consumeCmd)
	if err != nil {
		return nil, nil, fmt.Errorf("send consume command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, nil, fmt.Errorf("set read deadline: %w", err)
	}

	rawData, err := bc.readWirePayload(command, requestID)
	_ = conn.SetReadDeadline(time.Time{})

	if err != nil {
		return nil, nil, fmt.Errorf("read batch message with offsets: %w", err)
	}

	if wire.IsBatch(rawData) {
		batch, err := util.DecodeBatchMessages(rawData)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode batch: %w", err)
		}

		var messages []string
		var offsets []uint64
		for _, msg := range batch.Messages {
			messages = append(messages, msg.Payload)
			offsets = append(offsets, msg.Offset)
		}
		return messages, offsets, nil
	}

	return []string{}, []uint64{}, nil
}
