package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
)

const DefaultMaxInFlightRequests = 32
const DefaultMaxRequestBytes = 128 << 20

func (cfg *Config) Normalize() {
	if cfg.BrokerPort <= 0 {
		cfg.BrokerPort = 9000
	}
	if cfg.HealthCheckPort <= 0 {
		cfg.HealthCheckPort = 9080
	}
	if cfg.ExporterPort <= 0 {
		cfg.ExporterPort = 9100
	}

	// disk storage
	if strings.TrimSpace(cfg.LogDir) == "" {
		cfg.LogDir = "broker-logs"
	}
	if cfg.DiskFlushBatchSize <= 0 {
		cfg.DiskFlushBatchSize = 50
	}
	if cfg.DiskWriteTimeoutMS <= 0 {
		cfg.DiskWriteTimeoutMS = 10
	}
	if cfg.DiskFlushIntervalMS <= 0 {
		util.Warn("Invalid DiskFlushIntervalMS (%d), defaulting to 1000ms", cfg.DiskFlushIntervalMS)
		cfg.DiskFlushIntervalMS = 1000
	}
	if cfg.LingerMS < 0 {
		cfg.LingerMS = 0
	}
	if cfg.CompressionType == "" {
		cfg.CompressionType = "none"
	}
	switch cfg.CompressionType {
	case "none", "gzip", "snappy", "lz4":
	default:
		util.Warn("Invalid compression_type '%s', defaulting to 'none'", cfg.CompressionType)
		cfg.CompressionType = "none"
	}

	// log segment & retention
	if normalized, ok := NormalizeCleanupPolicy(cfg.CleanupPolicy); ok {
		cfg.CleanupPolicy = normalized
	} else {
		util.Warn("Invalid cleanup_policy '%s', defaulting to 'delete'", cfg.CleanupPolicy)
		cfg.CleanupPolicy = CleanupPolicyDelete
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 300
	}
	if cfg.SegmentSize < 1024 {
		cfg.SegmentSize = 1 << 20 // 1MB
	}
	if cfg.SegmentRollTimeMS < 0 {
		cfg.SegmentRollTimeMS = 0
	}
	if cfg.IndexSize < 1024*1024 {
		cfg.IndexSize = 10 * 1024 * 1024
	}
	if cfg.IndexIntervalBytes <= 0 {
		cfg.IndexIntervalBytes = 4096
	}
	if cfg.RetentionHours <= 0 {
		cfg.RetentionHours = 168
	}
	if cfg.RetentionBytes == 0 {
		cfg.RetentionBytes = -1
	}
	if cfg.RetentionCheckIntervalMS <= 0 {
		cfg.RetentionCheckIntervalMS = 300000
	}
	if cfg.CompactionCheckIntervalMS <= 0 {
		cfg.CompactionCheckIntervalMS = 300000
	}
	if cfg.MinCleanableDirtyRatio <= 0 || cfg.MinCleanableDirtyRatio >= 1.0 {
		cfg.MinCleanableDirtyRatio = 0.5
	}

	// internal channels
	if cfg.ChannelBufferSize <= 0 {
		cfg.ChannelBufferSize = 100000
	}
	if cfg.PartitionChannelBufSize <= 0 {
		cfg.PartitionChannelBufSize = 10000
	}
	if cfg.ConsumerChannelBufSize <= 0 {
		cfg.ConsumerChannelBufSize = 1000
	}
	if cfg.BroadcastChannelBufferSize <= 0 {
		cfg.BroadcastChannelBufferSize = 10000
	}

	// distributed cluster
	if cfg.InternalBrokerPort < 0 {
		cfg.InternalBrokerPort = 0
	}
	if cfg.RaftPort <= 0 {
		cfg.RaftPort = 9001
	}
	if cfg.DiscoveryPort <= 0 {
		cfg.DiscoveryPort = 8000
	}
	if strings.TrimSpace(cfg.AdvertisedHost) == "" {
		cfg.AdvertisedHost = "localhost"
	}
	if cfg.MinInSyncReplicas <= 0 {
		cfg.MinInSyncReplicas = 2
	}

	if cfg.ProducerStateTTLMS <= 0 {
		cfg.ProducerStateTTLMS = 30 * 60 * 1000
	}
	if cfg.TransactionalIDExpirationMS <= 0 {
		cfg.TransactionalIDExpirationMS = 7 * 24 * 60 * 60 * 1000
	}

	// consumer
	if cfg.ConsumerSessionTimeoutMS <= 0 {
		cfg.ConsumerSessionTimeoutMS = 10000
	}
	if cfg.ConsumerHeartbeatCheckMS <= 0 {
		cfg.ConsumerHeartbeatCheckMS = 5000
	}
	if cfg.ConsumerHeartbeatCheckMS >= cfg.ConsumerSessionTimeoutMS {
		cfg.ConsumerHeartbeatCheckMS = cfg.ConsumerSessionTimeoutMS / 2
	}
	if cfg.MaxClientConnections <= 0 {
		cfg.MaxClientConnections = 1000
	}
	if cfg.ClientIdleTimeoutMS <= 0 {
		cfg.ClientIdleTimeoutMS = 60000
	}
	if cfg.MaxInFlightRequests <= 0 {
		cfg.MaxInFlightRequests = DefaultMaxInFlightRequests
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.MaxInternalInFlightRequests <= 0 {
		cfg.MaxInternalInFlightRequests = DefaultMaxInFlightRequests
	}
	if cfg.MaxInternalRequestBytes <= 0 {
		cfg.MaxInternalRequestBytes = DefaultMaxRequestBytes
	}
	for i := range cfg.StaticConsumerGroups {
		g := &cfg.StaticConsumerGroups[i]
		if strings.TrimSpace(g.Name) == "" {
			g.Name = "default-group"
		}
		if g.ConsumerCount <= 0 {
			g.ConsumerCount = 1
		}
		if len(g.Topics) == 0 {
			g.Topics = []string{"default-topic"}
		}
		if g.TopicPartitions == nil {
			g.TopicPartitions = map[string]int{}
		}
		for _, t := range g.Topics {
			if g.TopicPartitions[t] <= 0 {
				g.TopicPartitions[t] = 1
			}
		}
	}

	// stream
	if cfg.MaxStreamConnections <= 0 {
		cfg.MaxStreamConnections = 1000
	}
	if cfg.StreamTimeout <= 0 {
		cfg.StreamTimeout = 30 * time.Minute
	}
}

func overrideEnvInt(target *int, key string) {
	if v := os.Getenv(key); v != "" {
		*target = util.ParseInt(v, *target)
	}
}

func overrideEnvInt64(target *int64, key string) {
	if v := os.Getenv(key); v != "" {
		*target = util.ParseInt64(v, *target)
	}
}

func overrideEnvUint64(target *uint64, key string) {
	if v := os.Getenv(key); v != "" {
		if u, err := strconv.ParseUint(v, 10, 64); err == nil {
			*target = u
		}
	}
}

func overrideEnvFloat64(target *float64, key string) {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			*target = f
		}
	}
}

func overrideEnvBool(target *bool, key string) {
	if v := os.Getenv(key); v != "" {
		*target = util.ParseBool(v, *target)
	}
}

func overrideEnvString(target *string, key string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
}

func overrideEnvStringSlice(target *[]string, key string) {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, s := range parts {
			s = strings.TrimSpace(s)
			if s != "" {
				result = append(result, s)
			}
		}
		*target = result
	}
}

func overrideEnvSASLUsers(target *[]SASLUser, key string) error {
	if v := os.Getenv(key); v != "" {
		entries := strings.Split(v, ",")
		users := make([]SASLUser, 0, len(entries))
		for _, entry := range entries {
			parts := strings.SplitN(strings.TrimSpace(entry), ":", 3)
			principal := strings.TrimSpace(parts[0])
			if len(parts) != 3 || principal == "" || strings.TrimSpace(parts[1]) == "" {
				return fmt.Errorf("invalid %s entry %q: expected principal:token:permission1|permission2", key, entry)
			}
			permissions := strings.Split(parts[2], "|")
			for i := range permissions {
				permissions[i] = strings.TrimSpace(permissions[i])
			}
			users = append(users, SASLUser{Principal: principal, Token: strings.TrimSpace(parts[1]), Permissions: permissions})
		}
		*target = users
	}
	return nil
}
