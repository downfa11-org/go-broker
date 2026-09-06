package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/util"
	"gopkg.in/yaml.v3"
)

var (
	defaultConfig *Config
	defaultOnce   sync.Once
)

type SASLUser struct {
	Principal   string   `yaml:"principal" json:"principal"`
	Token       string   `yaml:"token" json:"token"`
	Permissions []string `yaml:"permissions,omitempty" json:"permissions,omitempty"`
}

type ConsumerGroupConfig struct {
	Name            string         `yaml:"name" json:"name"`
	ConsumerCount   int            `yaml:"consumer_count" json:"consumer_count"`
	Topics          []string       `yaml:"topics" json:"topics"`
	TopicPartitions map[string]int `yaml:"topic_partitions" json:"topic_partitions"`
}

type Config struct {
	BrokerPort      int           `yaml:"broker_port" json:"broker.port"`
	HealthCheckPort int           `yaml:"health_check_port" json:"health.check.port"`
	EnableExporter  bool          `yaml:"enable_exporter" json:"enable.exporter"`
	ExporterPort    int           `yaml:"exporter_port" json:"exporter.port"`
	LogLevel        util.LogLevel `yaml:"log_level" json:"log_level"`

	// disk storage
	LogDir              string `yaml:"log_dir" json:"log.dir"`
	DiskFlushBatchSize  int    `yaml:"disk_flush_batch_size" json:"disk.flush.batch.size"`
	DiskFlushIntervalMS int    `yaml:"disk_flush_interval_ms" json:"disk.flush.interval.ms"`
	DiskWriteTimeoutMS  int    `yaml:"disk_write_timeout_ms" json:"disk.write.timeout.ms"`
	LingerMS            int    `yaml:"linger_ms" json:"linger.ms"`
	CompressionType     string `yaml:"compression_type" json:"compression.type"` // "none", "gzip", "snappy", "lz4"

	// log segment
	CleanupInterval           int     `yaml:"log_cleanup_interval" json:"log.cleanup.interval"`
	SegmentSize               uint64  `yaml:"log_segment_bytes" json:"log.segment.bytes"`
	SegmentRollTimeMS         int     `yaml:"log_segment_roll_ms" json:"log.segment.roll.ms"`
	IndexSize                 uint64  `yaml:"log_index_size_bytes" json:"log.index.size.bytes"`
	IndexIntervalBytes        int     `yaml:"log_index_interval_bytes" json:"log.index.interval.bytes"`
	CleanupPolicy             string  `yaml:"log_cleanup_policy" json:"log.cleanup.policy"`
	RetentionHours            int     `yaml:"log_retention_hours" json:"log.retention.hours"`
	RetentionBytes            int64   `yaml:"log_retention_bytes" json:"log.retention.bytes"`
	RetentionCheckIntervalMS  int     `yaml:"log_retention_check_interval_ms" json:"log.retention.check.interval.ms"`
	CompactionCheckIntervalMS int     `yaml:"log_compaction_check_interval_ms" json:"log.compaction.check.interval.ms"`
	MinCleanableDirtyRatio    float64 `yaml:"log_min_cleanable_dirty_ratio" json:"log.min.cleanable.dirty.ratio"`

	// internal channels
	ChannelBufferSize          int `yaml:"channel_buffer_size" json:"channel.buffer.size"`
	PartitionChannelBufSize    int `yaml:"partition_channel_buffer_size" json:"partition.channel.buffer.size"`
	ConsumerChannelBufSize     int `yaml:"consumer_channel_buffer_size" json:"consumer.channel.buffer.size"`
	BroadcastChannelBufferSize int `yaml:"broadcast_channel_buffer_size" json:"broadcast.channel.buffer.size"`

	// distributed cluster
	EnabledDistribution    bool     `yaml:"enabled_distribution" json:"distribution.enabled"`
	InternalAuthToken      string   `yaml:"internal_auth_token" json:"distribution.internal_auth_token"`
	InternalBrokerPort     int      `yaml:"internal_broker_port" json:"distribution.internal_broker_port"`
	RaftPort               int      `yaml:"raft_port" json:"distribution.raft.port"`
	RaftSnapshotIntervalMS int      `yaml:"raft_snapshot_interval_ms" json:"distribution.raft.snapshot.interval.ms"`
	RaftSnapshotThreshold  uint64   `yaml:"raft_snapshot_threshold" json:"distribution.raft.snapshot.threshold"`
	RaftTrailingLogs       uint64   `yaml:"raft_trailing_logs" json:"distribution.raft.trailing.logs"`
	DiscoveryPort          int      `yaml:"discovery_port" json:"distribution.discovery.port"`
	RaftPeers              []string `yaml:"raft_peers" json:"distribution.raft.peers"`
	StaticClusterMembers   []string `yaml:"static_cluster_members" json:"distribution.static_cluster_members"`
	BootstrapCluster       bool     `yaml:"bootstrap_cluster" json:"distribution.bootstrap"`

	AdvertisedHost           string `yaml:"advertised_host" json:"distribution.advertised_host"`
	AdvertisedBrokerPort     int    `yaml:"advertised_broker_port" json:"distribution.advertised_broker_port"`
	AdvertisedClientHost     string `yaml:"advertised_client_host" json:"distribution.advertised_client_host"`
	MinInSyncReplicas        int    `yaml:"min_insync_replicas" json:"min.insync.replicas"`
	DefaultReplicationFactor int    `yaml:"default_replication_factor" json:"default.replication.factor"`

	// idempotency
	EnableIdempotence           bool `yaml:"enable_idempotence" json:"enable.idempotence"`
	ProducerStateTTLMS          int  `yaml:"producer_state_ttl_ms" json:"producer.state.ttl.ms"`
	TransactionalIDExpirationMS int  `yaml:"transactional_id_expiration_ms" json:"transactional.id.expiration.ms"`

	// consumer
	ConsumerSessionTimeoutMS int                   `yaml:"consumer_session_timeout_ms" json:"consumer.session.timeout.ms"`
	ConsumerHeartbeatCheckMS int                   `yaml:"consumer_heartbeat_check_ms" json:"consumer.heartbeat.check.ms"`
	StaticConsumerGroups     []ConsumerGroupConfig `yaml:"static_consumer_groups" json:"static_consumer_groups"`

	// network
	MaxClientConnections        int `yaml:"max_client_connections" json:"max.client.connections"`
	ClientIdleTimeoutMS         int `yaml:"client_idle_timeout_ms" json:"client.idle.timeout.ms"`
	ShutdownTimeoutMS           int `yaml:"shutdown_timeout_ms" json:"shutdown.timeout.ms"`
	MaxInFlightRequests         int `yaml:"max_inflight_requests" json:"max.inflight.requests"`
	MaxRequestBytes             int `yaml:"max_request_bytes" json:"max.request.bytes"`
	MaxInternalInFlightRequests int `yaml:"max_internal_inflight_requests" json:"max.internal.inflight.requests"`
	MaxInternalRequestBytes     int `yaml:"max_internal_request_bytes" json:"max.internal.request.bytes"`

	// stream
	MaxStreamConnections int           `yaml:"max_stream_connections" json:"max.stream.connections"`
	StreamTimeout        time.Duration `yaml:"stream_timeout" json:"stream.timeout"`

	// security
	UseTLS                        bool `yaml:"use_tls" json:"tls.enable"`
	TLSCert                       tls.Certificate
	TLSCertPath                   string `yaml:"tls_cert_path" json:"tls.cert_path"`
	TLSKeyPath                    string `yaml:"tls_key_path" json:"tls.key_path"`
	InternalUseTLS                bool   `yaml:"internal_use_tls" json:"internal_tls.enable"`
	AllowInsecureClusterTransport bool   `yaml:"allow_insecure_cluster_transport" json:"distribution.allow_insecure_transport"`
	InternalTLSCertPath           string `yaml:"internal_tls_cert_path" json:"internal_tls.cert_path"`
	InternalTLSKeyPath            string `yaml:"internal_tls_key_path" json:"internal_tls.key_path"`
	InternalTLSCAPath             string `yaml:"internal_tls_ca_path" json:"internal_tls.ca_path"`
	InternalTLSServerName         string `yaml:"internal_tls_server_name" json:"internal_tls.server_name"`
	InternalTLSCert               tls.Certificate
	InternalTLSClientCAPool       *x509.CertPool
	InternalTLSRootCAPool         *x509.CertPool
	EnableSASL                    bool       `yaml:"enable_sasl" json:"sasl.enable"`
	SASLUsers                     []SASLUser `yaml:"sasl_users" json:"sasl.users"`
}

func DefaultConfig() *Config {
	defaultOnce.Do(func() {
		defaultConfig = &Config{
			BrokerPort:      9000,
			HealthCheckPort: 9080,
			EnableExporter:  true,
			ExporterPort:    9100,
			LogLevel:        util.LogLevelInfo,

			// disk storage
			LogDir:              "broker-logs",
			DiskFlushBatchSize:  50,
			DiskFlushIntervalMS: 500,
			DiskWriteTimeoutMS:  10,
			LingerMS:            50,
			CompressionType:     "none",

			// log segment & retention
			CleanupInterval:           300,
			SegmentSize:               1 * 1024 * 1024 * 1024,  // 1GB
			SegmentRollTimeMS:         7 * 24 * 60 * 60 * 1000, // 7day
			IndexSize:                 10 * 1024 * 1024,        // 10MB
			IndexIntervalBytes:        4096,
			CleanupPolicy:             "delete",
			RetentionHours:            168,
			RetentionBytes:            -1,
			RetentionCheckIntervalMS:  300000,
			CompactionCheckIntervalMS: 300000,
			MinCleanableDirtyRatio:    0.5,

			// internal channels
			ChannelBufferSize:          1024,
			PartitionChannelBufSize:    10000,
			ConsumerChannelBufSize:     1000,
			BroadcastChannelBufferSize: 10000,

			// distributed cluster
			EnabledDistribution:      false,
			InternalAuthToken:        "",
			InternalBrokerPort:       0,
			RaftPort:                 9001,
			RaftSnapshotIntervalMS:   120000,
			RaftSnapshotThreshold:    8192,
			RaftTrailingLogs:         10240,
			DiscoveryPort:            8000,
			RaftPeers:                []string{},
			StaticClusterMembers:     []string{},
			BootstrapCluster:         false,
			AdvertisedHost:           "localhost",
			AdvertisedBrokerPort:     0,
			MinInSyncReplicas:        2,
			DefaultReplicationFactor: 3,

			// idempotency
			EnableIdempotence:           false,
			ProducerStateTTLMS:          30 * 60 * 1000,
			TransactionalIDExpirationMS: 7 * 24 * 60 * 60 * 1000,

			// consumer
			ConsumerSessionTimeoutMS: 10000,
			ConsumerHeartbeatCheckMS: 5000,

			// network
			MaxClientConnections:        1000,
			ClientIdleTimeoutMS:         60000,
			ShutdownTimeoutMS:           DefaultShutdownTimeoutMS,
			MaxInFlightRequests:         DefaultMaxInFlightRequests,
			MaxRequestBytes:             DefaultMaxRequestBytes,
			MaxInternalInFlightRequests: DefaultMaxInFlightRequests,
			MaxInternalRequestBytes:     DefaultMaxRequestBytes,

			// stream
			MaxStreamConnections: 1000,
			StreamTimeout:        30 * time.Minute,
		}
	})

	cfgCopy := *defaultConfig
	if defaultConfig.RaftPeers != nil {
		cfgCopy.RaftPeers = make([]string, len(defaultConfig.RaftPeers))
		copy(cfgCopy.RaftPeers, defaultConfig.RaftPeers)
	}
	if defaultConfig.StaticClusterMembers != nil {
		cfgCopy.StaticClusterMembers = make([]string, len(defaultConfig.StaticClusterMembers))
		copy(cfgCopy.StaticClusterMembers, defaultConfig.StaticClusterMembers)
	}
	if defaultConfig.SASLUsers != nil {
		cfgCopy.SASLUsers = make([]SASLUser, len(defaultConfig.SASLUsers))
		copy(cfgCopy.SASLUsers, defaultConfig.SASLUsers)
		for i := range cfgCopy.SASLUsers {
			cfgCopy.SASLUsers[i].Permissions = append([]string(nil), defaultConfig.SASLUsers[i].Permissions...)
		}
	}
	if defaultConfig.StaticConsumerGroups != nil {
		cfgCopy.StaticConsumerGroups = make([]ConsumerGroupConfig, len(defaultConfig.StaticConsumerGroups))
		copy(cfgCopy.StaticConsumerGroups, defaultConfig.StaticConsumerGroups)
	}
	return &cfgCopy
}

func LoadConfig() (*Config, error) {
	cfg := DefaultConfig()
	configPath := flag.String("config", "", "Path to YAML/JSON config file")

	flag.IntVar(&cfg.BrokerPort, "port", cfg.BrokerPort, "Broker port")
	flag.IntVar(&cfg.HealthCheckPort, "health-port", cfg.HealthCheckPort, "Health port")
	flag.BoolVar(&cfg.EnableExporter, "exporter", cfg.EnableExporter, "Enable exporter")
	flag.IntVar(&cfg.ExporterPort, "exporter-port", cfg.ExporterPort, "Exporter port")
	logLevelStr := flag.String("log-level", "info", "Log level")

	// disk storage
	flag.StringVar(&cfg.LogDir, "log-dir", cfg.LogDir, "Log directory")
	flag.IntVar(&cfg.DiskFlushBatchSize, "disk-flush-batch", cfg.DiskFlushBatchSize, "Disk flush batch")
	flag.IntVar(&cfg.DiskFlushIntervalMS, "disk-flush-interval-ms", cfg.DiskFlushIntervalMS, "Disk sync interval in milliseconds")
	flag.IntVar(&cfg.DiskWriteTimeoutMS, "disk-write-timeout", cfg.DiskWriteTimeoutMS, "Disk write timeout")
	flag.IntVar(&cfg.LingerMS, "linger-ms", cfg.LingerMS, "Linger ms")
	flag.StringVar(&cfg.CompressionType, "compression-type", "none", "Compression type (none, gzip, snappy, lz4)")

	// log segment & retention
	flag.IntVar(&cfg.CleanupInterval, "cleanup-interval", cfg.CleanupInterval, "Cleanup seconds")
	segmentSizeInt64, ok := util.SafeUint64ToInt64(cfg.SegmentSize)
	if !ok {
		return nil, fmt.Errorf("default segment size %d exceeds int64 max", cfg.SegmentSize)
	}
	flag.Int64Var(&segmentSizeInt64, "segment-size", segmentSizeInt64, "Segment size")
	flag.IntVar(&cfg.SegmentRollTimeMS, "segment-roll-time-ms", cfg.SegmentRollTimeMS, "Segment roll time")
	indexSizeInt64, ok := util.SafeUint64ToInt64(cfg.IndexSize)
	if !ok {
		return nil, fmt.Errorf("default index size %d exceeds int64 max", cfg.IndexSize)
	}
	flag.Int64Var(&indexSizeInt64, "index-size", indexSizeInt64, "Max index file size")
	flag.IntVar(&cfg.IndexIntervalBytes, "index-interval-bytes", cfg.IndexIntervalBytes, "Index interval bytes")
	flag.StringVar(&cfg.CleanupPolicy, "cleanup-policy", cfg.CleanupPolicy, "Cleanup policy (delete, compact, or delete,compact)")
	flag.IntVar(&cfg.RetentionHours, "retention-hours", cfg.RetentionHours, "Retention hours")
	flag.Int64Var(&cfg.RetentionBytes, "retention-bytes", cfg.RetentionBytes, "Retention bytes")
	flag.IntVar(&cfg.RetentionCheckIntervalMS, "retention-check-interval", cfg.RetentionCheckIntervalMS, "Retention check interval")
	flag.IntVar(&cfg.CompactionCheckIntervalMS, "compaction-check-interval", cfg.CompactionCheckIntervalMS, "Compaction check interval ms")
	flag.Float64Var(&cfg.MinCleanableDirtyRatio, "min-cleanable-dirty-ratio", cfg.MinCleanableDirtyRatio, "Min cleanable dirty ratio (0.1 ~ 0.9)")

	// internal channels
	flag.IntVar(&cfg.ChannelBufferSize, "channel-buffer", cfg.ChannelBufferSize, "Channel buffer")
	flag.IntVar(&cfg.PartitionChannelBufSize, "partition-ch-buffer", cfg.PartitionChannelBufSize, "Partition buffer")
	flag.IntVar(&cfg.ConsumerChannelBufSize, "consumer-ch-buffer", cfg.ConsumerChannelBufSize, "Consumer buffer")
	flag.IntVar(&cfg.BroadcastChannelBufferSize, "broadcast-ch-buffer", cfg.BroadcastChannelBufferSize, "Broadcast channel buffer size")

	// distributed cluster
	flag.BoolVar(&cfg.EnabledDistribution, "enable-distribution", cfg.EnabledDistribution, "Enable distributed clustering")
	flag.StringVar(&cfg.InternalAuthToken, "internal-auth-token", cfg.InternalAuthToken, "Shared token for broker-to-broker internal text commands")
	flag.IntVar(&cfg.InternalBrokerPort, "internal-broker-port", cfg.InternalBrokerPort, "Dedicated broker-to-broker internal command port")
	flag.IntVar(&cfg.RaftPort, "raft-port", cfg.RaftPort, "Raft port for replication")
	flag.IntVar(&cfg.RaftSnapshotIntervalMS, "raft-snapshot-interval-ms", cfg.RaftSnapshotIntervalMS, "Raft snapshot check interval in milliseconds")
	flag.Uint64Var(&cfg.RaftSnapshotThreshold, "raft-snapshot-threshold", cfg.RaftSnapshotThreshold, "Outstanding Raft log entries required before snapshotting")
	flag.Uint64Var(&cfg.RaftTrailingLogs, "raft-trailing-logs", cfg.RaftTrailingLogs, "Raft log entries retained after snapshotting")
	flag.IntVar(&cfg.DiscoveryPort, "discovery-port", cfg.DiscoveryPort, "Discovery service port")
	raftPeersFlag := flag.String("raft-peers", "", "Raft peer addresses (comma-separated)")
	flag.BoolVar(&cfg.BootstrapCluster, "bootstrap-cluster", cfg.BootstrapCluster, "Bootstrap Raft cluster")
	flag.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "Advertised host for discovery")
	flag.IntVar(&cfg.MinInSyncReplicas, "min-insync-replicas", cfg.MinInSyncReplicas, "Minimum in-sync replicas for writes")
	flag.IntVar(&cfg.DefaultReplicationFactor, "default-replication-factor", cfg.DefaultReplicationFactor, "Default replication factor for new topics")

	// idempotency
	flag.BoolVar(&cfg.EnableIdempotence, "enable-idempotence", cfg.EnableIdempotence, "Enable producer idempotency")
	flag.IntVar(&cfg.ProducerStateTTLMS, "producer-state-ttl-ms", cfg.ProducerStateTTLMS, "Producer idempotency state TTL in milliseconds")
	flag.IntVar(&cfg.TransactionalIDExpirationMS, "transactional-id-expiration-ms", cfg.TransactionalIDExpirationMS, "Completed transactional.id expiration in milliseconds")

	// consumer
	flag.IntVar(&cfg.ConsumerSessionTimeoutMS, "consumer-session-timeout", cfg.ConsumerSessionTimeoutMS, "Session timeout")
	flag.IntVar(&cfg.ConsumerHeartbeatCheckMS, "consumer-heartbeat-check", cfg.ConsumerHeartbeatCheckMS, "Heartbeat check")
	flag.IntVar(&cfg.MaxClientConnections, "max-client-connections", cfg.MaxClientConnections, "Maximum concurrently serviced client connections")
	flag.IntVar(&cfg.ClientIdleTimeoutMS, "client-idle-timeout-ms", cfg.ClientIdleTimeoutMS, "Idle client connection timeout in milliseconds")
	flag.IntVar(&cfg.ShutdownTimeoutMS, "shutdown-timeout-ms", cfg.ShutdownTimeoutMS, "Maximum signal-triggered broker shutdown time in milliseconds")
	flag.IntVar(&cfg.MaxInFlightRequests, "max-inflight-requests", cfg.MaxInFlightRequests, "Maximum concurrently admitted client requests")
	flag.IntVar(&cfg.MaxRequestBytes, "max-request-bytes", cfg.MaxRequestBytes, "Total encoded plus decoded client request byte budget")
	flag.IntVar(&cfg.MaxInternalInFlightRequests, "max-internal-inflight-requests", cfg.MaxInternalInFlightRequests, "Maximum concurrently admitted internal requests")
	flag.IntVar(&cfg.MaxInternalRequestBytes, "max-internal-request-bytes", cfg.MaxInternalRequestBytes, "Total encoded plus decoded internal request byte budget")

	// stream
	flag.IntVar(&cfg.MaxStreamConnections, "max-stream-connections", cfg.MaxStreamConnections, "Max stream connections")
	flag.DurationVar(&cfg.StreamTimeout, "stream-timeout", cfg.StreamTimeout, "Stream timeout")

	// security
	flag.BoolVar(&cfg.UseTLS, "tls", cfg.UseTLS, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", cfg.TLSCertPath, "TLS cert")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", cfg.TLSKeyPath, "TLS key")
	flag.BoolVar(&cfg.InternalUseTLS, "internal-tls", cfg.InternalUseTLS, "Enable mutual TLS on the internal broker listener")
	flag.BoolVar(&cfg.AllowInsecureClusterTransport, "allow-insecure-cluster-transport", cfg.AllowInsecureClusterTransport, "Explicitly allow plaintext Raft and discovery transport")
	flag.StringVar(&cfg.InternalTLSCertPath, "internal-tls-cert", cfg.InternalTLSCertPath, "Internal listener TLS certificate")
	flag.StringVar(&cfg.InternalTLSKeyPath, "internal-tls-key", cfg.InternalTLSKeyPath, "Internal listener TLS private key")
	flag.StringVar(&cfg.InternalTLSCAPath, "internal-tls-ca", cfg.InternalTLSCAPath, "CA used to verify internal broker certificates")
	flag.StringVar(&cfg.InternalTLSServerName, "internal-tls-server-name", cfg.InternalTLSServerName, "Server name used by broker-to-broker mTLS clients")
	flag.BoolVar(&cfg.EnableSASL, "enable-sasl", cfg.EnableSASL, "Enable SASL-style token authentication for client commands")

	flag.Parse()

	switch strings.ToLower(*logLevelStr) {
	case "debug":
		cfg.LogLevel = util.LogLevelDebug
	case "warn", "warning":
		cfg.LogLevel = util.LogLevelWarn
	case "error":
		cfg.LogLevel = util.LogLevelError
	default:
		cfg.LogLevel = util.LogLevelInfo
	}

	if env := os.Getenv("CONFIG_PATH"); env != "" && *configPath == "" {
		*configPath = env
	}

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("configured file %s does not exist", *configPath)
			} else {
				return nil, fmt.Errorf("failed to read config file %s: %w", *configPath, err)
			}
		} else {
			if strings.HasSuffix(*configPath, ".json") {
				if err := json.Unmarshal(data, cfg); err != nil {
					return nil, err
				}
			} else {
				if err := yaml.Unmarshal(data, cfg); err != nil {
					return nil, err
				}
			}
		}
	}

	if *raftPeersFlag != "" {
		parts := strings.Split(*raftPeersFlag, ",")
		cfg.RaftPeers = make([]string, 0, len(parts))
		for _, s := range parts {
			if s = strings.TrimSpace(s); s != "" {
				cfg.RaftPeers = append(cfg.RaftPeers, s)
			}
		}
	}

	if segmentSizeInt64 <= 0 {
		cfg.SegmentSize = DefaultConfig().SegmentSize
	} else {
		segmentSize, valid := util.SafeInt64ToUint64(segmentSizeInt64)
		if !valid {
			return nil, fmt.Errorf("segment size %d must be non-negative", segmentSizeInt64)
		}
		cfg.SegmentSize = segmentSize
	}
	if indexSizeInt64 <= 0 {
		cfg.IndexSize = DefaultConfig().IndexSize
	} else {
		indexSize, valid := util.SafeInt64ToUint64(indexSizeInt64)
		if !valid {
			return nil, fmt.Errorf("index size %d must be non-negative", indexSizeInt64)
		}
		cfg.IndexSize = indexSize
	}

	overrideEnvInt(&cfg.BrokerPort, "BROKER_PORT")
	overrideEnvInt(&cfg.HealthCheckPort, "HEALTH_CHECK_PORT")
	overrideEnvBool(&cfg.EnableExporter, "ENABLE_EXPORTER")
	overrideEnvString(&cfg.LogDir, "LOG_DIR")
	overrideEnvInt(&cfg.ExporterPort, "EXPORTER_PORT")

	overrideEnvInt(&cfg.DiskFlushBatchSize, "DISK_FLUSH_BATCH")
	overrideEnvInt(&cfg.DiskFlushIntervalMS, "DISK_FLUSH_INTERVAL_MS")
	overrideEnvInt(&cfg.LingerMS, "LINGER_MS")
	overrideEnvString(&cfg.CompressionType, "COMPRESSION_TYPE")

	overrideEnvUint64(&cfg.SegmentSize, "LOG_SEGMENT_BYTES")
	overrideEnvUint64(&cfg.IndexSize, "LOG_INDEX_SIZE_BYTES")
	overrideEnvInt(&cfg.IndexIntervalBytes, "LOG_INDEX_INTERVAL_BYTES")
	overrideEnvString(&cfg.CleanupPolicy, "LOG_CLEANUP_POLICY")
	overrideEnvInt(&cfg.RetentionHours, "LOG_RETENTION_HOURS")
	overrideEnvInt64(&cfg.RetentionBytes, "LOG_RETENTION_BYTES")
	overrideEnvInt(&cfg.RetentionCheckIntervalMS, "LOG_RETENTION_CHECK_INTERVAL_MS")
	overrideEnvInt(&cfg.CompactionCheckIntervalMS, "LOG_COMPACTION_CHECK_INTERVAL_MS")
	overrideEnvFloat64(&cfg.MinCleanableDirtyRatio, "LOG_MIN_CLEANABLE_DIRTY_RATIO")

	overrideEnvInt(&cfg.PartitionChannelBufSize, "PARTITION_CH_BUFFER")
	overrideEnvInt(&cfg.ConsumerChannelBufSize, "CONSUMER_CH_BUFFER")
	overrideEnvInt(&cfg.BroadcastChannelBufferSize, "BROADCAST_CH_BUFFER")
	overrideEnvInt(&cfg.MaxClientConnections, "MAX_CLIENT_CONNECTIONS")
	overrideEnvInt(&cfg.ClientIdleTimeoutMS, "CLIENT_IDLE_TIMEOUT_MS")
	overrideEnvInt(&cfg.ShutdownTimeoutMS, "SHUTDOWN_TIMEOUT_MS")
	overrideEnvInt(&cfg.MaxInFlightRequests, "MAX_INFLIGHT_REQUESTS")
	overrideEnvInt(&cfg.MaxRequestBytes, "MAX_REQUEST_BYTES")
	overrideEnvInt(&cfg.MaxInternalInFlightRequests, "MAX_INTERNAL_INFLIGHT_REQUESTS")
	overrideEnvInt(&cfg.MaxInternalRequestBytes, "MAX_INTERNAL_REQUEST_BYTES")

	overrideEnvBool(&cfg.EnabledDistribution, "ENABLE_DISTRIBUTION")
	overrideEnvString(&cfg.InternalAuthToken, "INTERNAL_AUTH_TOKEN")
	overrideEnvInt(&cfg.InternalBrokerPort, "INTERNAL_BROKER_PORT")
	overrideEnvString(&cfg.AdvertisedHost, "ADVERTISED_HOST")
	overrideEnvInt(&cfg.AdvertisedBrokerPort, "ADVERTISED_BROKER_PORT")
	overrideEnvString(&cfg.AdvertisedClientHost, "ADVERTISED_CLIENT_HOST")
	overrideEnvInt(&cfg.RaftPort, "RAFT_PORT")
	overrideEnvInt(&cfg.RaftSnapshotIntervalMS, "RAFT_SNAPSHOT_INTERVAL_MS")
	overrideEnvUint64(&cfg.RaftSnapshotThreshold, "RAFT_SNAPSHOT_THRESHOLD")
	overrideEnvUint64(&cfg.RaftTrailingLogs, "RAFT_TRAILING_LOGS")
	overrideEnvInt(&cfg.DiscoveryPort, "DISCOVERY_PORT")
	overrideEnvStringSlice(&cfg.RaftPeers, "RAFT_PEERS")
	overrideEnvStringSlice(&cfg.StaticClusterMembers, "STATIC_CLUSTER_MEMBERS")
	overrideEnvBool(&cfg.BootstrapCluster, "BOOTSTRAP_CLUSTER")
	overrideEnvInt(&cfg.MinInSyncReplicas, "MIN_INSYNC_REPLICAS")
	overrideEnvInt(&cfg.DefaultReplicationFactor, "DEFAULT_REPLICATION_FACTOR")

	overrideEnvBool(&cfg.EnableIdempotence, "ENABLE_IDEMPOTENCE")
	overrideEnvInt(&cfg.ProducerStateTTLMS, "PRODUCER_STATE_TTL_MS")
	overrideEnvInt(&cfg.TransactionalIDExpirationMS, "TRANSACTIONAL_ID_EXPIRATION_MS")

	overrideEnvInt(&cfg.ConsumerSessionTimeoutMS, "CONSUMER_SESSION_TIMEOUT")
	overrideEnvInt(&cfg.ConsumerHeartbeatCheckMS, "CONSUMER_HEARTBEAT_CHECK")
	overrideEnvBool(&cfg.UseTLS, "USE_TLS")
	overrideEnvString(&cfg.TLSCertPath, "TLS_CERT_PATH")
	overrideEnvString(&cfg.TLSKeyPath, "TLS_KEY_PATH")
	overrideEnvBool(&cfg.InternalUseTLS, "INTERNAL_USE_TLS")
	overrideEnvBool(&cfg.AllowInsecureClusterTransport, "ALLOW_INSECURE_CLUSTER_TRANSPORT")
	overrideEnvString(&cfg.InternalTLSCertPath, "INTERNAL_TLS_CERT_PATH")
	overrideEnvString(&cfg.InternalTLSKeyPath, "INTERNAL_TLS_KEY_PATH")
	overrideEnvString(&cfg.InternalTLSCAPath, "INTERNAL_TLS_CA_PATH")
	overrideEnvString(&cfg.InternalTLSServerName, "INTERNAL_TLS_SERVER_NAME")
	overrideEnvBool(&cfg.EnableSASL, "ENABLE_SASL")
	if err := overrideEnvSASLUsers(&cfg.SASLUsers, "SASL_USERS"); err != nil {
		return nil, err
	}

	cfg.Normalize()
	util.SetLevel(cfg.LogLevel)

	if err := cfg.ValidateClusterTransport(); err != nil {
		return nil, err
	}
	if err := cfg.ValidateClientAuthentication(); err != nil {
		return nil, err
	}
	if err := cfg.loadInternalTLSConfig(); err != nil {
		return nil, err
	}

	if cfg.UseTLS {
		if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
			return nil, fmt.Errorf("TLS enabled but missing cert/key path")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS cert: %w", err)
		}
		cfg.TLSCert = cert
	}

	return cfg, nil
}

// ValidateClientAuthentication requires an explicit least-privilege contract
// for every configured principal.
func (cfg *Config) ValidateClientAuthentication() error {
	if cfg == nil || !cfg.EnableSASL {
		return nil
	}
	if len(cfg.SASLUsers) == 0 {
		return fmt.Errorf("sasl_users must contain at least one principal when enable_sasl=true")
	}
	allowed := map[string]struct{}{
		"admin": {}, "topic.read": {}, "topic.write": {}, "group": {}, "transaction": {}, "*": {},
	}
	principals := make(map[string]struct{}, len(cfg.SASLUsers))
	for _, user := range cfg.SASLUsers {
		principal := strings.TrimSpace(user.Principal)
		if principal == "" || strings.TrimSpace(user.Token) == "" {
			return fmt.Errorf("sasl user principal and token are required")
		}
		if _, duplicate := principals[principal]; duplicate {
			return fmt.Errorf("duplicate sasl principal %q", principal)
		}
		principals[principal] = struct{}{}
		if len(user.Permissions) == 0 {
			return fmt.Errorf("sasl principal %q requires at least one permission", principal)
		}
		seen := make(map[string]struct{}, len(user.Permissions))
		for _, permission := range user.Permissions {
			permission = strings.ToLower(strings.TrimSpace(permission))
			if _, ok := allowed[permission]; !ok {
				return fmt.Errorf("sasl principal %q has invalid permission %q", principal, permission)
			}
			if _, duplicate := seen[permission]; duplicate {
				return fmt.Errorf("sasl principal %q repeats permission %q", principal, permission)
			}
			seen[permission] = struct{}{}
		}
	}
	return nil
}

// ValidateClusterTransport rejects accidental plaintext cluster deployments.
func (cfg *Config) ValidateClusterTransport() error {
	if cfg == nil || !cfg.EnabledDistribution {
		return nil
	}
	if strings.TrimSpace(cfg.InternalAuthToken) == "" {
		return fmt.Errorf("internal_auth_token is required when enabled_distribution=true")
	}
	if strings.ContainsAny(cfg.InternalAuthToken, " \t\r\n") {
		return fmt.Errorf("internal_auth_token must not contain whitespace")
	}
	if !cfg.InternalUseTLS && !cfg.AllowInsecureClusterTransport {
		return fmt.Errorf("distributed mode requires internal TLS; set allow_insecure_cluster_transport only for isolated test environments")
	}
	return nil
}

func (cfg *Config) loadInternalTLSConfig() error {
	if !cfg.InternalUseTLS {
		return nil
	}
	if cfg.InternalBrokerPort <= 0 {
		return fmt.Errorf("internal_use_tls requires internal_broker_port")
	}
	if cfg.InternalTLSCertPath == "" || cfg.InternalTLSKeyPath == "" || cfg.InternalTLSCAPath == "" {
		return fmt.Errorf("internal TLS enabled but missing cert/key/ca path")
	}
	cert, err := tls.LoadX509KeyPair(cfg.InternalTLSCertPath, cfg.InternalTLSKeyPath)
	if err != nil {
		return fmt.Errorf("failed to load internal TLS cert: %w", err)
	}
	// #nosec G304 -- internal CA path is broker operator supplied configuration.
	caPEM, err := os.ReadFile(cfg.InternalTLSCAPath)
	if err != nil {
		return fmt.Errorf("failed to read internal TLS CA: %w", err)
	}
	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("failed to parse internal TLS CA")
	}
	rootCAPool := x509.NewCertPool()
	if !rootCAPool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("failed to parse internal TLS root CA")
	}
	cfg.InternalTLSCert = cert
	cfg.InternalTLSClientCAPool = clientCAPool
	cfg.InternalTLSRootCAPool = rootCAPool
	return nil
}

func (cfg *Config) InternalServerTLSConfig() *tls.Config {
	if cfg == nil || !cfg.InternalUseTLS {
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cfg.InternalTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    cfg.InternalTLSClientCAPool,
		MinVersion:   tls.VersionTLS12,
	}
}

func (cfg *Config) InternalClientTLSConfig() *tls.Config {
	if cfg == nil || !cfg.InternalUseTLS {
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cfg.InternalTLSCert},
		RootCAs:      cfg.InternalTLSRootCAPool,
		ServerName:   cfg.InternalTLSServerName,
		MinVersion:   tls.VersionTLS12,
	}
}
