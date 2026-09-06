package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestEventHistoryDoesNotInheritDestructiveRetention(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionHours = 1
	cfg.RetentionBytes = 1
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.SegmentRollTimeMS = 0
	cfg.SegmentSize = 128

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	for version := uint64(1); version <= 5; version++ {
		require.NoError(t, manager.PublishToPartitionWithAck("events", 0, &types.Message{Key: "order", AggregateVersion: version, Payload: "event-payload"}))
	}
	require.NoError(t, manager.CreateTopic("events", 2, false, true))
	manager.GetTopic("events").ApplyPolicy(DefaultPolicy())
	for partition := 0; partition < 2; partition++ {
		raw, err := dm.GetHandler("events", partition)
		require.NoError(t, err)
		handler := raw.(*disk.DiskHandler)
		hours, bytes := handler.RetentionPolicy()
		require.Equal(t, -1, hours)
		require.Equal(t, int64(-1), bytes)
		handler.EnforceRetention(cfg)
		require.Zero(t, handler.GetFirstOffset())
	}
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	dm = disk.NewDiskManager(cfg)
	defer dm.CloseAllHandlers()
	manager = NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.RestoreTopics())
	defer closeTopicManager(manager)
	for partition := 0; partition < 2; partition++ {
		raw, err := dm.GetHandler("events", partition)
		require.NoError(t, err)
		handler := raw.(*disk.DiskHandler)
		handler.EnforceRetention(cfg)
		require.Zero(t, handler.GetFirstOffset())
	}
	messages, err := manager.ReadTopicPartition("events", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 5)
}

func TestEventHistoryRejectsExplicitRetentionLimits(t *testing.T) {
	for _, distributed := range []bool{false, true} {
		cfg := config.DefaultConfig()
		cfg.EnabledDistribution = distributed
		cfg.LogDir = t.TempDir()
		dm := disk.NewDiskManager(cfg)
		defer dm.CloseAllHandlers()
		manager := NewTopicManager(cfg, dm, nil)
		defer closeTopicManager(manager)
		policy := DefaultPolicy()
		policy.RetentionHours = 1
		require.ErrorContains(t, manager.CreateTopicWithPolicy("events", 1, false, true, policy), "complete event history")
		require.Nil(t, manager.GetTopic("events"))
		require.NoError(t, manager.CreateTopic("events", 1, false, true))
		policy.RetentionHours = 0
		policy.RetentionBytes = 1
		require.ErrorContains(t, manager.CreateTopicWithPolicy("events", 1, false, true, policy), "complete event history")
		require.Zero(t, manager.GetTopic("events").Policy.RetentionBytes)
	}
}
