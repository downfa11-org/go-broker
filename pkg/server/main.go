package server

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	"github.com/cursus-io/cursus/pkg/cluster"
	client "github.com/cursus-io/cursus/pkg/cluster/client"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/observability"
	wireprotocol "github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

const (
	defaultMaxWorkers      = 1000
	defaultIdleTimeout     = 60 * time.Second
	readDeadlinePoll       = 5 * time.Second
	DefaultHealthCheckPort = 9080
)

// RunServer starts the broker with optional TLS and gzip
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
	return RunServerContext(context.Background(), cfg, tm, dm, cd, sm)
}

// RunServerContext starts the broker and shuts it down when ctx is canceled.
func RunServerContext(ctx context.Context, cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator, sm *stream.StreamManager) error {
	if ctx == nil {
		return fmt.Errorf("server context must not be nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	addr := fmt.Sprintf(":%d", cfg.BrokerPort)
	var ln net.Listener
	var err error
	if cfg.UseTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cfg.TLSCert},
			MinVersion:   tls.VersionTLS12,
		}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	defer func() { _ = ln.Close() }()
	go closeListenerOnDone(ctx, ln)
	util.Info("🧩 Broker listening on %s (TLS=%v, Compression=%v)", addr, cfg.UseTLS, cfg.CompressionType)

	if cd != nil {
		cd.Start()
		util.Info("🔄 Coordinator started with heartbeat monitoring")
	}

	var cc *clusterController.ClusterController
	var rm *replication.RaftReplicationManager
	var clusterClient *client.TCPClusterClient
	var discoveryListener net.Listener
	defer func() {
		if discoveryListener != nil {
			_ = discoveryListener.Close()
		}
		if rm != nil {
			if shutdownErr := rm.Shutdown(); shutdownErr != nil {
				util.Error("raft shutdown failed: %v", shutdownErr)
			}
		}
	}()
	if cfg.EnabledDistribution {
		brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
		localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.RaftPort)
		raftServerID := brokerID

		var err error
		clusterClient = client.NewSecureTCPClusterClient(cfg.InternalAuthToken, cfg.InternalClientTLSConfig())
		rm, err = replication.NewRaftReplicationManager(ctx, cfg, raftServerID, tm, cd, *clusterClient)
		if err != nil {
			return fmt.Errorf("failed to create raft replication manager: %w", err)
		}

		clientHost := cfg.AdvertisedClientHost
		if clientHost == "" {
			clientHost = cfg.AdvertisedHost
		}
		clientPort := cfg.AdvertisedBrokerPort
		if clientPort == 0 {
			clientPort = cfg.BrokerPort
		}
		clientAddr := fmt.Sprintf("%s:%d", clientHost, clientPort)

		sd := clusterController.NewServiceDiscovery(rm, brokerID, localAddr, clientAddr)
		discoveryAddr := fmt.Sprintf(":%d", cfg.DiscoveryPort)
		cs := cluster.NewSecureClusterServer(sd, cfg.InternalAuthToken, cfg.InternalServerTLSConfig())
		discoveryListener, err = cs.Start(discoveryAddr)
		if err != nil {
			return fmt.Errorf("start discovery server: %w", err)
		}
		go closeListenerOnDone(ctx, discoveryListener)

		cc = clusterController.NewClusterController(ctx, cfg, rm, sd, brokerID, localAddr)

		// Start background heartbeats to all cluster members
		clusterClient.StartHeartbeat(
			ctx,
			cfg.StaticClusterMembers,
			brokerID,
			localAddr,
			cfg.DiscoveryPort,
			func() []fsm.ISRCatchupProof {
				if manager := rm.GetISRManager(); manager != nil {
					return manager.BuildCatchupProofs()
				}
				return nil
			},
		)

		// Every node should attempt to join the cluster via seeds
		go func() {
			util.Info("🚀 Attempting to join cluster via seeds...")
			// Wait a bit for Raft to initialize
			time.Sleep(2 * time.Second)

			if err := clusterClient.JoinCluster(cfg.StaticClusterMembers, brokerID, localAddr, cfg.DiscoveryPort); err != nil {
				util.Warn("⚠️ Join cluster attempt failed: %v. This is normal if already part of the cluster.", err)
			} else {
				util.Info("✅ Successfully joined cluster")
			}

			// Register self with ClientAddr — try local first, then forward to leader
			go func() {
				for i := 0; i < 15; i++ {
					time.Sleep(3 * time.Second)
					// Try direct Raft apply (works if we're the leader)
					if err := sd.Register(); err == nil {
						util.Info("✅ Registered with client address %s", clientAddr)
						return
					}
					// Forward via RAFT_APPLY to leader
					if cc != nil && cc.Router != nil {
						brokerJSON, _ := json.Marshal(map[string]interface{}{
							"id": brokerID, "addr": localAddr, "client_addr": clientAddr,
							"status": "active", "lifecycle_protocol": fsm.BrokerProtocolVersionCurrent,
						})
						raftCmd := fmt.Sprintf("RAFT_APPLY %stype=REGISTER payload=%s", internalAuthPrefix(cfg), string(brokerJSON))
						if resp, err := cc.Router.ForwardToLeader(raftCmd); err == nil && !wireprotocol.IsErrorResponse(resp) {
							util.Info("✅ Registered via leader with client address %s", clientAddr)
							return
						}
					}
				}
			}()
		}()

		go func() {
			util.Info("🔄 Starting cluster leader election monitor...")
			for isLeader := range rm.LeaderCh() {
				if isLeader {
					util.Info("🎉 Became cluster leader! Syncing all members with FSM.")
					if regErr := sd.Register(); regErr != nil {
						util.Error("❌ Failed to register as leader: %v", regErr)
					}
					// Immediate reconcile ensures all Raft members are in FSM
					sd.Reconcile()
				} else {
					util.Info("💀 Lost cluster leadership.")
				}
			}
		}()

		util.Info("🌐 Distributed clustering enabled (brokerID=%s, localAddr=%s)", brokerID, localAddr)
	}

	globalCH := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	defer func() {
		if err := globalCH.Close(); err != nil {
			util.Error("Failed to close command handler: %v", err)
		}
	}()
	if !cfg.EnabledDistribution {
		journalPath := filepath.Join(cfg.LogDir, "__transaction_state.journal")
		if err := globalCH.ConfigureTransactionJournal(journalPath); err != nil {
			return fmt.Errorf("initialize standalone transaction journal: %w", err)
		}
	}
	if cd != nil {
		cd.SetGroupSessionCallbacks(globalCH.IsGroupCoordinator, globalCH.ExpireGroupMembers)
		cd.SetGroupObservationBatchResolver(globalCH.ResolveGroupCoordinators)
	}
	if cc != nil {
		cc.SetLocalProcessor(globalCH)
		cc.StartReplicaCatchup(ctx, clusterClient, globalCH.ApplyReplicaCatchup)
	}
	if cfg.EnabledDistribution && cfg.InternalBrokerPort > 0 {
		shutdownInternal, err := startInternalBrokerListener(ctx, cfg, globalCH)
		if err != nil {
			return err
		}
		defer shutdownInternal()
	}
	if err := globalCH.RecoverPreparedTransactions(); err != nil {
		return fmt.Errorf("failed to recover prepared transactions: %w", err)
	}

	healthState := NewHealthState()
	addStorageReadinessChecks(healthState, tm, dm)
	if cd != nil {
		addConsumerMetadataReadinessCheck(healthState, cd)
	}
	if cfg.EnabledDistribution {
		healthState.AddCheck("cluster_leader", func(context.Context) error {
			if cc == nil {
				return fmt.Errorf("cluster controller unavailable")
			}
			_, leaderErr := cc.GetClusterLeader()
			return leaderErr
		})
		healthState.AddCheck("topic_materialization", func(context.Context) error {
			if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
				return fmt.Errorf("topic materialization state unavailable")
			}
			return cc.RaftManager.GetFSM().TopicMaterializationReadinessError()
		})
	}

	runtimeCollector := observability.NewCollector(tm, cd, dm, sm, cc, healthState, globalCH.TxnManager)
	if cfg.EnableExporter {
		metricsServer, startErr := metrics.StartMetricsServer(cfg.ExporterPort, runtimeCollector)
		if startErr != nil {
			return fmt.Errorf("start metrics exporter: %w", startErr)
		}
		defer shutdownHTTPServer(metricsServer)
		util.Info("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		util.Info("📉 Exporter disabled")
	}

	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	healthServer, healthErr := startHealthCheckServer(healthPort, healthState)
	if healthErr != nil {
		return fmt.Errorf("start health server: %w", healthErr)
	}
	defer shutdownHTTPServer(healthServer)

	workerCount := maxClientConnections(cfg)
	workerCh := make(chan net.Conn, workerCount)
	connectionSlots := newConnectionLimiter(workerCount)
	var workerWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for conn := range workerCh {
				handleConn(ctx, conn, globalCH)
			}
		}()
	}
	defer func() {
		healthState.SetReady(false)
		cancel()
		close(workerCh)
		workerWG.Wait()
	}()

	var temporaryDelay time.Duration
	for {
		healthState.SetReady(true)
		if err := connectionSlots.Acquire(ctx); err != nil {
			return err
		}
		conn, err := ln.Accept()
		if err != nil {
			connectionSlots.Release()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("broker listener closed: %w", err)
			}
			healthState.SetReady(false)
			if temporaryDelay == 0 {
				temporaryDelay = 5 * time.Millisecond
			} else {
				temporaryDelay *= 2
			}
			if maximum := time.Second; temporaryDelay > maximum {
				temporaryDelay = maximum
			}
			util.Warn("accept error; retrying in %s: %v", temporaryDelay, err)
			time.Sleep(temporaryDelay)
			continue
		}
		temporaryDelay = 0
		conn = newLimitedConnection(conn, connectionSlots.Release)
		select {
		case workerCh <- conn:
		case <-ctx.Done():
			_ = conn.Close()
			connectionSlots.Release()
			return ctx.Err()
		}
	}
}

func closeListenerOnDone(ctx context.Context, ln net.Listener) {
	<-ctx.Done()
	_ = ln.Close()
}

func startInternalBrokerListener(ctx context.Context, cfg *config.Config, cmdHandler *controller.CommandHandler) (func(), error) {
	addr := fmt.Sprintf(":%d", cfg.InternalBrokerPort)
	var ln net.Listener
	var err error
	if cfg.InternalUseTLS {
		ln, err = tls.Listen("tcp", addr, cfg.InternalServerTLSConfig())
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to start internal broker listener on %s: %w", addr, err)
	}

	util.Info("🔒 Internal broker listener started on %s (mTLS=%v)", addr, cfg.InternalUseTLS)
	internalCtx, cancel := context.WithCancel(ctx)
	workerCount := maxClientConnections(cfg)
	workerCh := make(chan net.Conn, workerCount)
	connectionSlots := newConnectionLimiter(workerCount)
	var workerWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for conn := range workerCh {
				handleInternalConn(internalCtx, conn, cmdHandler)
			}
		}()
	}
	var acceptWG sync.WaitGroup
	acceptWG.Add(1)
	go func() {
		defer close(workerCh)
		defer acceptWG.Done()
		for {
			if err := connectionSlots.Acquire(internalCtx); err != nil {
				return
			}
			conn, err := ln.Accept()
			if err != nil {
				connectionSlots.Release()
				select {
				case <-internalCtx.Done():
					return
				default:
					util.Error("⚠️ Internal accept error: %v", err)
					continue
				}
			}
			conn = newLimitedConnection(conn, connectionSlots.Release)
			select {
			case workerCh <- conn:
			default:
				util.Warn("⚠️ Internal worker pool saturated; closing connection from %s", conn.RemoteAddr())
				_ = conn.Close()
			}
		}
	}()
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			cancel()
			_ = ln.Close()
			acceptWG.Wait()
			workerWG.Wait()
		})
	}
	return shutdown, nil
}

func handleInternalConn(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler) {
	handleConnWithContext(ctx, conn, cmdHandler, controller.NewInternalClientContext("default-group", 0))
}

func observeClientConnection() func() {
	metrics.ClientConnectionsTotal.Inc()
	metrics.ClientConnectionsActive.Inc()
	return metrics.ClientConnectionsActive.Dec
}

// handleConn processes a connection using a shared CommandHandler.
func handleConn(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler) {
	defer observeClientConnection()()
	handleConnWithContext(ctx, conn, cmdHandler, controller.NewClientContext("default-group", 0))
}

func handleConnWithContext(ctx context.Context, conn net.Conn, cmdHandler *controller.CommandHandler, cmdCtx *controller.ClientContext) {
	isStreamed := false
	defer func() {
		if !isStreamed {
			_ = conn.Close()
		}
	}()

	clientCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopContextClose := context.AfterFunc(clientCtx, func() { _ = conn.Close() })
	defer stopContextClose()
	cmdCtx.SetRequestContext(clientCtx)
	idleTimeout := clientIdleTimeout(cmdHandler.Config)
	lastActivity := time.Now()
	if err := conn.SetDeadline(lastActivity.Add(idleTimeout)); err != nil {
		return
	}
	wireConnection, responseConn, err := negotiateServerConnection(conn)
	if err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})

	for {
		select {
		case <-clientCtx.Done():
			return
		default:
		}
		deadline := time.Now().Add(readDeadlinePoll)
		idleDeadline := lastActivity.Add(idleTimeout)
		if idleDeadline.Before(deadline) {
			deadline = idleDeadline
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			util.Error("⚠️ SetReadDeadline error: %v", err)
			return
		}

		request, err := readWireRequest(wireConnection)
		if err != nil {
			select {
			case <-clientCtx.Done():
				return
			default:
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() && time.Since(lastActivity) < idleTimeout {
					continue
				}
				return
			}
		}

		lastActivity = time.Now()
		if err := conn.SetWriteDeadline(lastActivity.Add(time.Minute)); err != nil {
			return
		}
		responseConn.setRequest(request)
		shouldExit, err := processMessage(request.Payload, cmdHandler, cmdCtx, responseConn)
		if err != nil {
			return
		}
		if shouldExit {
			if request.Command == wire.CommandStream {
				isStreamed = true
			}
			return
		}
	}
}

type connectionLimiter struct {
	slots chan struct{}
}

type limitedConnection struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func newLimitedConnection(conn net.Conn, release func()) net.Conn {
	return &limitedConnection{Conn: conn, release: release}
}

func (c *limitedConnection) Close() error {
	c.closeOnce.Do(func() {
		if c.Conn != nil {
			c.closeErr = c.Conn.Close()
		}
		if c.release != nil {
			c.release()
		}
	})
	return c.closeErr
}

func newConnectionLimiter(limit int) *connectionLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &connectionLimiter{slots: make(chan struct{}, limit)}
}

func (l *connectionLimiter) Acquire(ctx context.Context) error {
	if l == nil {
		return fmt.Errorf("connection limiter is nil")
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *connectionLimiter) Release() {
	if l == nil {
		return
	}
	select {
	case <-l.slots:
	default:
	}
}

func maxClientConnections(cfg *config.Config) int {
	if cfg != nil && cfg.MaxClientConnections > 0 {
		return cfg.MaxClientConnections
	}
	return defaultMaxWorkers
}

func clientIdleTimeout(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.ClientIdleTimeoutMS > 0 {
		return time.Duration(cfg.ClientIdleTimeoutMS) * time.Millisecond
	}
	return defaultIdleTimeout
}

func internalAuthPrefix(cfg *config.Config) string {
	if cfg != nil && cfg.InternalAuthToken != "" {
		return "internal_token=" + cfg.InternalAuthToken + " "
	}
	return ""
}

func initializeConnection(cfg *config.Config, tm *topic.TopicManager, cd *coordinator.Coordinator, sm *stream.StreamManager, cc *clusterController.ClusterController) (*controller.CommandHandler, *controller.ClientContext) {
	cmdHandler := controller.NewCommandHandler(tm, cfg, cd, sm, cc)
	ctx := controller.NewClientContext("default-group", 0)
	return cmdHandler, ctx
}

func processMessage(data []byte, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	if isBatchMessage(data) {
		if ctx != nil && ctx.Internal && cmdHandler.Config != nil && cmdHandler.Config.InternalAuthToken != "" && !cmdHandler.Config.InternalUseTLS {
			writeResponse(conn, "ERROR: internal_batch_requires_token_wrapper")
			return false, nil
		}
		resp, err := cmdHandler.HandleBatchMessage(data, conn, ctx)
		if err != nil {
			return false, err
		}
		if !suppressBatchPublishResponse(data, ctx) {
			writeResponse(conn, decorateServerResponse(resp, ctx))
		}
		return false, nil
	}

	rawInput, isRawCommand := parseRawTextCommand(data)
	if strings.HasPrefix(strings.ToUpper(rawInput), "INTERNAL_BATCH ") {
		return handleInternalBatchMessage(rawInput, cmdHandler, ctx, conn)
	}
	if isRawCommand {
		if resp := authorizeInternalListenerCommand(rawInput, cmdHandler, ctx); resp != "" {
			writeResponse(conn, resp)
			return false, nil
		}
		return handleCommandMessage(rawInput, cmdHandler, ctx, conn)
	}

	util.Debug("[%s] Received unrecognized input (len=%d)", conn.RemoteAddr().String(), len(rawInput))
	writeResponse(conn, decorateServerResponse("ERROR: malformed_input reason=command_payload_required", ctx))
	return true, nil
}

func parseRawTextCommand(data []byte) (string, bool) {
	rawInput := strings.Trim(string(data), " \t\n\r")
	return rawInput, isCommand(rawInput)
}

func authorizeInternalListenerCommand(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext) string {
	if ctx == nil || !ctx.Internal || cmdHandler == nil || cmdHandler.Config == nil {
		return ""
	}
	if cmdHandler.Config.InternalUseTLS {
		return ""
	}
	token := strings.TrimSpace(cmdHandler.Config.InternalAuthToken)
	if token == "" {
		return "ERROR: internal_auth_not_configured command=INTERNAL_LISTENER"
	}
	if parseInternalCommandArgs(payload)["internal_token"] != token {
		return "ERROR: internal_command_unauthorized command=INTERNAL_LISTENER"
	}
	return ""
}

func handleInternalBatchMessage(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	if ctx == nil || !ctx.Internal {
		writeResponse(conn, "ERROR: internal_command_unauthorized command=INTERNAL_BATCH")
		return false, nil
	}
	if resp := authorizeInternalListenerCommand(payload, cmdHandler, ctx); resp != "" {
		writeResponse(conn, resp)
		return false, nil
	}
	encoded := parseInternalCommandArgs(payload)["payload"]
	if encoded == "" {
		writeResponse(conn, "ERROR: missing_payload command=INTERNAL_BATCH")
		return false, nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		writeResponse(conn, fmt.Sprintf("ERROR: invalid_payload command=INTERNAL_BATCH reason=%q", err.Error()))
		return false, nil
	}
	resp, err := cmdHandler.HandleBatchMessage(data, conn, ctx)
	if err != nil {
		return false, err
	}
	writeResponse(conn, resp)
	return false, nil
}

func parseInternalCommandArgs(payload string) map[string]string {
	args := map[string]string{}
	for _, field := range strings.Fields(payload) {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			args[key] = value
		}
	}
	return args
}
func handleCommandMessage(payload string, cmdHandler *controller.CommandHandler, ctx *controller.ClientContext, conn net.Conn) (bool, error) {
	resp := cmdHandler.HandleCommand(payload, ctx)
	if resp == controller.STREAM_DATA_SIGNAL {
		switch {
		case strings.HasPrefix(strings.ToUpper(payload), "STREAM "):
			if err := cmdHandler.HandleStreamCommand(conn, payload, ctx); err != nil {
				if errors.Is(err, controller.ErrStreamRejected) {
					return false, nil
				}
				writeResponse(conn, commandErrorResponse(err, ctx))
				return false, nil
			}
			return true, nil
		case strings.HasPrefix(strings.ToUpper(payload), "CONSUME "):
			if _, err := cmdHandler.HandleConsumeCommand(conn, payload, ctx); err != nil {
				writeResponse(conn, commandErrorResponse(err, ctx))
			}
			return false, nil
		case strings.HasPrefix(strings.ToUpper(payload), "READ_STREAM "):
			cmdHandler.HandleReadStreamCommand(conn, payload)
			return false, nil
		default:
			writeResponse(conn, decorateServerResponse("ERROR: unknown_command", ctx))
			return false, nil
		}
	}
	if resp == "" {
		resp = "ERROR: empty_command_response"
	}
	if !suppressPublishResponse(payload, ctx) {
		writeResponse(conn, decorateServerResponse(resp, ctx))
	}
	return false, nil
}

func suppressPublishResponse(payload string, ctx *controller.ClientContext) bool {
	if ctx != nil && ctx.Internal {
		return false
	}
	requestHeader := payload
	if messageIndex := strings.Index(requestHeader, "message="); messageIndex >= 0 {
		requestHeader = requestHeader[:messageIndex]
	}
	fields := strings.Fields(strings.TrimSpace(requestHeader))
	if len(fields) == 0 || !strings.EqualFold(fields[0], "PUBLISH") {
		return false
	}
	for _, field := range fields[1:] {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key != "acks" {
			continue
		}
		selection, err := ackpolicy.Parse(value)
		return err == nil && selection.Mode == ackpolicy.None
	}
	return false
}

func suppressBatchPublishResponse(data []byte, ctx *controller.ClientContext) bool {
	if ctx != nil && ctx.Internal {
		return false
	}
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		return false
	}
	selection, err := ackpolicy.Parse(batch.Acks)
	return err == nil && selection.Mode == ackpolicy.None
}

func commandErrorResponse(err error, ctx *controller.ClientContext) string {
	resp := err.Error()
	if !wireprotocol.IsErrorResponse(resp) {
		resp = fmt.Sprintf("ERROR: command_failed reason=%q", resp)
	}
	return decorateServerResponse(resp, ctx)
}

func decorateServerResponse(resp string, ctx *controller.ClientContext) string {
	return wireprotocol.EnrichErrorResponse(resp)
}

// isBatchMessage checks if the data is in binary batch format
func isBatchMessage(data []byte) bool {
	return wire.IsBatch(data)
}

func isCommand(s string) bool {
	return wireprotocol.IsTextCommand(s)
}

// writeResponseWithTimeout adds write timeout
func writeResponseWithTimeout(conn net.Conn, msg string, timeout time.Duration) {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		util.Error("⚠️ SetWriteDeadline error: %v", err)
		return
	}
	defer func() {
		if err := conn.SetWriteDeadline(time.Time{}); err != nil {
			util.Error("Failed to reset write deadline: %v", err)
		}
	}()

	if err := util.WriteWithLength(conn, []byte(msg)); err != nil {
		util.Error("⚠️ Write response error: %v", err)
	}
}

func writeResponse(conn net.Conn, msg string) {
	if err := util.WriteWithLength(conn, []byte(msg)); err != nil {
		util.Error("⚠️ Write response error: %v", err)
	}
}
