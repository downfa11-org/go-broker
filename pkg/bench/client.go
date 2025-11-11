package bench

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/util"
)

const AckTimeout = 5 * time.Second

type BenchClient struct {
	Addr        string
	EnableGzip  bool
	NumMessages int
	Topic       string
	Partitions  int
}

func (c *BenchClient) RunTopicCreationPhase() error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("connect to broker for topic creation: %w", err)
	}
	defer conn.Close()

	createPayload := fmt.Sprintf("CREATE %s %d", c.Topic, c.Partitions)
	createPayload = strings.TrimSpace(createPayload)

	if err := c.sendCommand(conn, c.Topic, createPayload); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			fmt.Printf("Topic '%s' already exists, continuing...\n", c.Topic)
			return nil
		}
		return fmt.Errorf("topic creation failed: %w", err)
	}

	return nil
}

// sendCommand sends an encoded topic/payload command and validates the response.
func (c *BenchClient) sendCommand(conn net.Conn, topic, payload string) error {
	cmdBytes := util.EncodeMessage(topic, payload)
	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(AckTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	upperResp := strings.ToUpper(resp)

	if !(strings.Contains(upperResp, "OK") ||
		strings.Contains(upperResp, "TOPIC") ||
		strings.Contains(resp, "✅") ||
		strings.Contains(upperResp, "CREATED") ||
		strings.Contains(upperResp, "EXISTS")) {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
}

// sendMessagesToPartition sends benchmark messages for a specific partition.
func (c *BenchClient) sendMessagesToPartition(producerID int, partitionID int, count int) error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("[P%d/Part%d] connection failed: %v", producerID, partitionID, err)
	}
	defer conn.Close()

	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("bench-msg-P%d-Part%d-Msg%d", producerID, partitionID, i)
		data := util.EncodeMessage(c.Topic, payload)

		msgBytes, err := server.CompressMessage(data, c.EnableGzip)
		if err != nil {
			return fmt.Errorf("[P%d/Part%d] compress failed: %v", producerID, partitionID, err)
		}

		if err := util.WriteWithLength(conn, msgBytes); err != nil {
			return fmt.Errorf("[P%d/Part%d] send failed: %v", producerID, partitionID, err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(AckTimeout)); err != nil {
			return fmt.Errorf("[P%d/Part%d] set read deadline failed: %v", producerID, partitionID, err)
		}

		resp, err := util.ReadWithLength(conn)
		if err != nil {
			return fmt.Errorf("[P%d/Part%d] read resp failed: %v", producerID, partitionID, err)
		}

		if len(resp) == 0 {
			return fmt.Errorf("[P%d/Part%d] empty response — possible connection issue", producerID, partitionID)
		}

		_ = conn.SetReadDeadline(time.Time{})
	}
	return nil
}

func (c *BenchClient) RunMessageProductionPhase(producerID int) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	msgsPerPartition := c.NumMessages / c.Partitions
	remainder := c.NumMessages % c.Partitions

	for p := 0; p < c.Partitions; p++ {
		messagesToSend := msgsPerPartition
		if p < remainder {
			messagesToSend++
		}

		if messagesToSend > 0 {
			wg.Add(1)
			go func(partitionID, count int) {
				defer wg.Done()
				if err := c.sendMessagesToPartition(producerID, partitionID, count); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(p, messagesToSend)
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("%d partition(s) failed, first error: %w", len(errs), errs[0])
	}
	return nil
}

// consumeMessagesFromPartition connects and sends a CONSUME command, then reads all expected messages.
func (c *BenchClient) consumeMessagesFromPartition(cid, count int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		fmt.Printf("[C%d] connection failed: %v\n", cid, err)
		return
	}
	defer conn.Close()

	partitionID := cid % c.Partitions
	startOffset := 0

	consumeCmd := fmt.Sprintf("CONSUME %s %d %d", c.Topic, partitionID, startOffset)
	consumeCmd = strings.TrimSpace(consumeCmd)
	cmdBytes := util.EncodeMessage(c.Topic, consumeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		fmt.Printf("[C%d] send command failed: %v\n", cid, err)
		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(AckTimeout * 2)); err != nil {
		fmt.Printf("[C%d] set read deadline failed: %v. Continuing...\n", cid, err)
	}

	consumedCount := 0
	for i := 0; i < count; i++ {
		msgBytes, err := util.ReadWithLength(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Printf("[C%d] Read message %d failed: %v\n", cid, i, err)
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(AckTimeout * 2)); err != nil {
			fmt.Printf("[C%d] Warning: Failed to reset read deadline: %v\n", cid, err)
		}

		if len(msgBytes) == 0 {
			continue
		}

		consumedCount++
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		fmt.Printf("Warning: Failed to clear read deadline: %v\n", err)
	}

	fmt.Printf("Consumer%d finished reading %d/%d messages.\n", cid, consumedCount, count)
}

func (b *BenchmarkRunner) RunConcurrentProducerPhase() error {
	var pWg sync.WaitGroup
	var producerErrors []error
	var mu sync.Mutex

	for i := 0; i < b.NumProducers; i++ {
		pWg.Add(1)
		go func(pid int) {
			defer pWg.Done()
			client := &BenchClient{
				Addr:        b.Addr,
				EnableGzip:  b.EnableGzip,
				NumMessages: b.MessagesPerProducer,
				Topic:       b.Topic,
				Partitions:  b.Partitions,
			}

			if err := client.RunMessageProductionPhase(pid); err != nil {
				mu.Lock()
				producerErrors = append(producerErrors, fmt.Errorf("producer %d error: %w", pid, err))
				mu.Unlock()
			}
		}(i)
	}
	pWg.Wait()

	if len(producerErrors) > 0 {
		return fmt.Errorf("%d producer(s) failed, first error: %w", len(producerErrors), producerErrors[0])
	}
	return nil
}

// RunConsumerPhase executes the consumption benchmark workflow.
func (c *BenchClient) RunConsumerPhase(numConsumers int) error {
	var wg sync.WaitGroup

	totalMessagesProduced := c.NumMessages
	msgsPerConsumer := totalMessagesProduced / numConsumers

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go c.consumeMessagesFromPartition(i, msgsPerConsumer, &wg)
	}

	wg.Wait()
	return nil
}
