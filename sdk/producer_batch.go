package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
)

func (p *Producer) sendBatch(part int, batch []Message) {
	if len(batch) == 0 {
		return
	}

	atomic.AddInt32(&p.inFlight[part], 1)
	defer atomic.AddInt32(&p.inFlight[part], -1)

	var batchStart, batchEnd uint64
	if len(batch) > 0 {
		batchStart = batch[0].SeqNum
		batchEnd = batch[len(batch)-1].SeqNum
	}

	shortID := p.client.ID[:8]
	batchID := fmt.Sprintf("%s-%d-p%d-%d-%d", shortID, p.client.Epoch, part, batchStart, batchEnd)

	p.partitionBatchMus[part].Lock()
	p.partitionBatchStates[part][batchID] = &BatchState{
		BatchID:     batchID,
		StartSeqNum: batchStart,
		EndSeqNum:   batchEnd,
		Partition:   part,
		SentTime:    time.Now(),
		Acked:       false,
	}
	p.partitionBatchMus[part].Unlock()

	data, err := EncodeBatchMessages(p.config.Topic, part, p.config.Acks, p.config.EnableIdempotence, batch)
	if err != nil {
		LogError("encode batch failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.recordDeliveryFailure(fmt.Errorf("encode batch %s: %w", batchID, err))
		return
	}

	sendStart := time.Now()
	ackResp, err := p.sendWithRetryForBatch(data, part, batch[0], batch[len(batch)-1])
	if err != nil {
		LogError("send failed: %v", err)
		if p.config.EnableMetrics {
			producerSendErrors.WithLabelValues(p.config.Topic).Add(float64(len(batch)))
		}
		p.cleanupBatchState(part, batchID)
		if isNonRetryableProducerError(err) {
			p.recordDeliveryFailure(fmt.Errorf("deliver batch %s: %w", batchID, err))
			return
		}
		p.handleSendFailure(part, batch)
		return
	}

	if p.config.EnableMetrics {
		producerBatchLatency.WithLabelValues(p.config.Topic).Observe(time.Since(sendStart).Seconds())
	}

	batchCount, ok := util.SafeIntToUint64(len(batch))
	if !ok {
		LogError("invalid negative batch length: %d", len(batch))
		return
	}
	p.attemptsCount.Add(batchCount)

	switch ackResp.Status {
	case "OK":
		p.partitionSentMus[part].Lock()
		for _, m := range batch {
			p.partitionSentSeqs[part][m.SeqNum] = struct{}{}
		}
		p.partitionSentMus[part].Unlock()
		p.markBatchAckedByID(part, batchID, len(batch))
		if p.config.EnableMetrics {
			producerMessagesSent.WithLabelValues(p.config.Topic).Add(float64(len(batch)))
		}
	case "PARTIAL":
		LogWarn("Partial success for batch %s", batchID)
		if p.config.EnableMetrics {
			producerSendErrors.WithLabelValues(p.config.Topic).Inc()
		}
		p.cleanupBatchState(part, batchID)
		p.handlePartialFailure(part, batch, ackResp)
	default:
		if p.config.EnableMetrics {
			producerSendErrors.WithLabelValues(p.config.Topic).Inc()
		}
		p.cleanupBatchState(part, batchID)
		p.recordDeliveryFailure(fmt.Errorf("deliver batch %s: unexpected acknowledgement status %q", batchID, ackResp.Status))
	}
}

func isNonRetryableProducerError(err error) bool {
	if err == nil {
		return false
	}
	var brokerErr *BrokerError
	if errors.As(err, &brokerErr) {
		return !brokerErr.Retryable
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "stale_producer_epoch") || strings.Contains(msg, "stale producer epoch")
}

func (p *Producer) cleanupBatchState(part int, batchID string) {
	p.partitionBatchMus[part].Lock()
	delete(p.partitionBatchStates[part], batchID)
	p.partitionBatchMus[part].Unlock()
}

func (p *Producer) handleSendFailure(part int, batch []Message) {
	if len(batch) == 0 {
		return
	}

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	p.partitionSentMus[part].Lock()
	var retryBatch []Message
	for _, msg := range batch {
		if _, exists := p.partitionSentSeqs[part][msg.SeqNum]; !exists {
			msg.Retry = true
			retryBatch = append(retryBatch, msg)
		}
	}
	p.partitionSentMus[part].Unlock()

	allMsgs := append(buf.msgs, retryBatch...)
	sort.Slice(allMsgs, func(i, j int) bool {
		if allMsgs[i].Retry && !allMsgs[j].Retry {
			return true
		}
		if !allMsgs[i].Retry && allMsgs[j].Retry {
			return false
		}
		return allMsgs[i].SeqNum < allMsgs[j].SeqNum
	})

	buf.msgs = allMsgs
	buf.cond.Signal()
}

func (p *Producer) handlePartialFailure(part int, batch []Message, ackResp *AckResponse) {
	lastSuccessSeq := ackResp.SeqEnd

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	var retryBatch []Message
	for _, msg := range batch {
		if msg.SeqNum > lastSuccessSeq {
			retryBatch = append(retryBatch, msg)
		}
	}

	if len(retryBatch) > 0 {
		allMsgs := append(buf.msgs, retryBatch...)
		sort.Slice(allMsgs, func(i, j int) bool {
			return allMsgs[i].SeqNum < allMsgs[j].SeqNum
		})
		buf.msgs = allMsgs
		buf.cond.Signal()
	}
}

func (p *Producer) sendWithRetry(payload []byte, part int) (*AckResponse, error) {
	return p.sendWithRetryForBatch(payload, part, Message{}, Message{})
}

func (p *Producer) sendWithRetryForBatch(payload []byte, part int, first, last Message) (*AckResponse, error) {
	maxAttempts := p.config.MaxRetries + 1
	backoff := p.config.RetryBackoffMS

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-p.done:
			return nil, fmt.Errorf("producer is closed")
		default:
		}

		conn := p.client.GetConn(part)
		if conn == nil {
			brokerAddr := p.getPartitionLeaderAddr(part)
			if err := p.client.ReconnectPartition(part, brokerAddr); err != nil {
				lastErr = fmt.Errorf("reconnect failed: %w", err)
				if !p.waitForRetry(backoff) {
					return nil, fmt.Errorf("producer is closed")
				}
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
			conn = p.client.GetConn(part)
			if conn == nil {
				lastErr = fmt.Errorf("no connection after reconnect")
				if !p.waitForRetry(backoff) {
					return nil, fmt.Errorf("producer is closed")
				}
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
		}

		if err := conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.WriteTimeoutMS) * time.Millisecond)); err != nil {
			lastErr = fmt.Errorf("set write deadline failed: %w", err)
			if !p.waitForRetry(backoff) {
				return nil, fmt.Errorf("producer is closed")
			}
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if err := WriteWithLength(conn, payload); err != nil {
			lastErr = fmt.Errorf("write failed: %w", err)
			brokerAddr := p.getPartitionLeaderAddr(part)
			_ = p.client.ReconnectPartition(part, brokerAddr)
			if !p.waitForRetry(backoff) {
				return nil, fmt.Errorf("producer is closed")
			}
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if p.config.Acks == "0" {
			return &AckResponse{Status: "OK"}, nil
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.AckTimeoutMS) * time.Millisecond))
		resp, err := ReadWithLength(conn)
		_ = conn.SetReadDeadline(time.Time{})

		if err != nil {
			lastErr = fmt.Errorf("read ack failed: %w", err)
			if isNonRetryableProducerError(err) {
				return nil, lastErr
			}
			brokerAddr := p.getPartitionLeaderAddr(part)
			_ = p.client.ReconnectPartition(part, brokerAddr)
			if !p.waitForRetry(backoff) {
				return nil, fmt.Errorf("producer is closed")
			}
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		ackResp, err := p.parseAckResponseForBatch(resp, part, first, last)
		if err != nil {
			lastErr = err
			if isNonRetryableProducerError(err) {
				return nil, lastErr
			}
			brokerAddr := p.getPartitionLeaderAddr(part)
			_ = p.client.ReconnectPartition(part, brokerAddr)
			if !p.waitForRetry(backoff) {
				return nil, fmt.Errorf("producer is closed")
			}
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		return ackResp, nil
	}
	return nil, lastErr
}

func (p *Producer) parseAckResponseForBatch(resp []byte, part int, first, last Message) (*AckResponse, error) {
	ackResp, err := p.parseAckResponse(resp)
	if err != nil || p.config.Acks == "0" {
		return ackResp, err
	}
	if ackResp.Status != "OK" {
		return nil, fmt.Errorf("unexpected batch acknowledgement status %q", ackResp.Status)
	}
	if ackResp.ProducerID != first.ProducerID {
		return nil, fmt.Errorf("ack producer mismatch for partition %d: expected %q got %q", part, first.ProducerID, ackResp.ProducerID)
	}
	if ackResp.ProducerEpoch != first.Epoch {
		return nil, fmt.Errorf("ack epoch mismatch for partition %d: expected %d got %d", part, first.Epoch, ackResp.ProducerEpoch)
	}
	if ackResp.SeqStart != first.SeqNum || ackResp.SeqEnd != last.SeqNum {
		return nil, fmt.Errorf("ack sequence range mismatch for partition %d: expected %d-%d got %d-%d", part, first.SeqNum, last.SeqNum, ackResp.SeqStart, ackResp.SeqEnd)
	}
	return ackResp, nil
}

func (p *Producer) waitForRetry(backoffMS int) bool {
	timer := time.NewTimer(time.Duration(backoffMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-p.done:
		return false
	}
}

func (p *Producer) markBatchAckedByID(part int, batchID string, batchLen int) {
	batchCount, ok := util.SafeIntToUint64(batchLen)
	if !ok {
		LogError("invalid negative acknowledged batch length: %d", batchLen)
		return
	}
	p.partitionBatchMus[part].Lock()
	state, ok := p.partitionBatchStates[part][batchID]
	if !ok || state.Acked {
		p.partitionBatchMus[part].Unlock()
		return
	}

	state.Acked = true
	p.uniqueCount.Add(batchCount)

	delete(p.partitionBatchStates[part], batchID)
	p.partitionBatchMus[part].Unlock()

	for {
		current := p.ackedCount.Load()
		if state.EndSeqNum <= current {
			break
		}
		if p.ackedCount.CompareAndSwap(current, state.EndSeqNum) {
			break
		}
	}

	p.partitionSentMus[part].Lock()
	for seq := range p.partitionSentSeqs[part] {
		if seq <= state.EndSeqNum {
			delete(p.partitionSentSeqs[part], seq)
		}
	}
	p.partitionSentMus[part].Unlock()

	elapsed := time.Since(state.SentTime)
	p.bmMu.Lock()
	p.bmTotalCount[part] += 1
	p.bmTotalTime[part] += elapsed
	if p.config.EnableBenchmark {
		p.bmLatencies = append(p.bmLatencies, elapsed)
	}
	p.bmMu.Unlock()
}

func (p *Producer) parseAckResponse(resp []byte) (*AckResponse, error) {
	var ackResp AckResponse
	if err := json.Unmarshal(resp, &ackResp); err != nil {
		return nil, fmt.Errorf("invalid ack format: %w", err)
	}

	if ackResp.Leader != "" && ackResp.Leader != p.client.GetLeaderAddr() {
		p.client.UpdateLeader(ackResp.Leader)
	}

	if ackResp.Status == "ERROR" {
		return &ackResp, fmt.Errorf("broker error: %s", ackResp.ErrorMsg)
	}

	if p.config.EnableIdempotence {
		if ackResp.ProducerID == "" {
			return nil, fmt.Errorf("incomplete ack: missing ProducerID")
		}
		if ackResp.ProducerEpoch != p.client.Epoch {
			return nil, fmt.Errorf("epoch mismatch: expected %d, got %d", p.client.Epoch, ackResp.ProducerEpoch)
		}
	}

	return &ackResp, nil
}
