package sdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ProducerClient struct {
	ID               string
	globalSeqNum     atomic.Uint64
	partitionSeqNums sync.Map // int -> *atomic.Uint64

	Epoch     int64
	mu        sync.RWMutex
	conns     atomic.Pointer[[]net.Conn]
	config    *PublisherConfig
	tlsConfig *tls.Config

	leader atomic.Pointer[leaderInfo]
	closed atomic.Bool
}

func NewProducerClient(config *PublisherConfig) (*ProducerClient, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	pc := &ProducerClient{
		ID:     uuid.New().String(),
		Epoch:  time.Now().UnixNano(),
		config: config,
	}

	pc.leader.Store(&leaderInfo{
		addr:    "",
		updated: time.Time{},
	})

	if config.UseTLS {
		cert, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		pc.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	return pc, nil
}

func (pc *ProducerClient) NextSeqNum(partition int) uint64 {
	if !pc.config.EnableIdempotence {
		return pc.globalSeqNum.Add(1)
	}

	val, _ := pc.partitionSeqNums.LoadOrStore(partition, &atomic.Uint64{})
	return val.(*atomic.Uint64).Add(1)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string) error {
	if pc.closed.Load() {
		return fmt.Errorf("producer client is closed")
	}
	if idx < 0 {
		return fmt.Errorf("invalid partition index: %d", idx)
	}

	if pc.config.UseTLS && pc.tlsConfig == nil {
		return fmt.Errorf("TLS enabled but certificate not loaded")
	}
	conn, err := dialAuthenticatedWireConnection(
		context.Background(), addr, 5*time.Second,
		pc.config.HandshakeTimeoutMS, pc.config.CompressionType, pc.tlsConfig,
		pc.config.Principal, pc.config.AuthToken,
	)
	if err != nil {
		return err
	}

	var currentConns []net.Conn
	if ptr := pc.conns.Load(); ptr != nil {
		currentConns = *ptr
	}

	newSize := idx + 1
	if len(currentConns) > newSize {
		newSize = len(currentConns)
	}

	tmp := make([]net.Conn, newSize)
	copy(tmp, currentConns)
	tmp[idx] = conn

	pc.conns.Store(&tmp)
	return nil
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	ptr := pc.conns.Load()
	if ptr == nil {
		return nil
	}
	conns := *ptr
	if part >= 0 && part < len(conns) {
		return conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.closed.Store(true)

	ptr := pc.conns.Swap(nil)
	if ptr == nil {
		return nil
	}

	conns := *ptr
	for _, c := range conns {
		if c != nil {
			_ = c.Close()
		}
	}

	return nil
}

func (pc *ProducerClient) GetLeaderAddr() string {
	info := pc.leader.Load()
	if info == nil || info.addr == "" {
		return ""
	}
	return info.addr
}

func (pc *ProducerClient) UpdateLeader(leaderAddr string) {
	old := pc.leader.Load()
	if old != nil && old.addr == leaderAddr {
		return
	}

	pc.leader.Store(&leaderInfo{
		addr:    leaderAddr,
		updated: time.Now(),
	})
}

func (pc *ProducerClient) selectBroker() string {
	if pc.config == nil || len(pc.config.BrokerAddrs) == 0 {
		return ""
	}

	staleness := pc.config.LeaderStaleness
	if staleness <= 0 {
		staleness = 30 * time.Second
	}

	info := pc.leader.Load()
	if info != nil && info.addr != "" && time.Since(info.updated) < staleness {
		return info.addr
	}

	return pc.config.BrokerAddrs[0]
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string) error {
	if addr == "" {
		addr = pc.selectBroker()
	}
	if addr == "" {
		return fmt.Errorf("no broker address available for partition %d", idx)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr)
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if idx < 0 {
		return fmt.Errorf("invalid partition index: %d", idx)
	}

	oldPtr := pc.conns.Load()
	if oldPtr != nil {
		conns := *oldPtr
		if idx < len(conns) && conns[idx] != nil {
			_ = conns[idx].Close()
			replacement := append([]net.Conn(nil), conns...)
			replacement[idx] = nil
			pc.conns.Store(&replacement)
		}
	}
	if addr == "" {
		addr = pc.selectBroker()
	}
	if addr == "" {
		return fmt.Errorf("no broker address available for partition %d", idx)
	}

	return pc.connectPartitionLocked(idx, addr)
}
