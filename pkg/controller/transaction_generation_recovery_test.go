package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestPreparedTransactionRecoversAfterProcessAndGenerationChange(t *testing.T) {
	for _, applied := range []bool{false, true} {
		for _, advanced := range []bool{false, true} {
			t.Run(fmt.Sprintf("applied=%t/advanced=%t", applied, advanced), func(t *testing.T) {
				cfg := config.DefaultConfig()
				cfg.LogDir = t.TempDir()
				journal := filepath.Join(cfg.LogDir, "transactions.journal")
				open := func() (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator, func()) {
					dm := disk.NewDiskManager(cfg)
					tm := topic.NewTopicManager(cfg, dm, nil)
					require.NoError(t, tm.RestoreTopics())
					cd := coordinator.NewCoordinator(context.Background(), cfg, tm)
					ch := NewCommandHandler(tm, cfg, cd, nil, nil)
					var once sync.Once
					stop := func() { once.Do(func() { _ = ch.Close(); cd.Stop(); tm.Stop(); dm.CloseAllHandlers() }) }
					t.Cleanup(stop)
					if tm.GetTopic("events") == nil {
						require.NoError(t, tm.CreateTopic("events", 1, false, false))
					}
					require.NoError(t, ch.ConfigureTransactionJournal(journal))
					return ch, tm, cd, stop
				}
				ch, tm, cd, stop := open()
				require.NoError(t, cd.RegisterGroup("events", "workers", 1))
				_, err := cd.AddConsumer("workers", "old-member")
				require.NoError(t, err)
				generation := cd.GetGeneration("workers")
				initAndStageTransaction(t, ch, "tx", "events", "workers", "old-member", generation, 13)
				current, err := ch.TxnManager.Status("tx")
				require.NoError(t, err)
				prepared, err := ch.prepareTransaction(current)
				require.NoError(t, err)
				require.NotZero(t, prepared.Offsets[0].RegistrationEpoch)
				if applied {
					require.NoError(t, ch.applyTransaction(prepared))
				}
				require.Empty(t, readCommittedPayloads(t, tm, "events"))
				stop()

				restarted, tm, cd, _ := open()
				_, err = cd.AddConsumer("workers", "new-member")
				require.NoError(t, err)
				require.NotEmpty(t, cd.ValidateOwnershipFailure("workers", "old-member", generation, 0))
				if advanced {
					require.NoError(t, cd.CommitOffset("workers", "events", 0, 21))
				}
				require.NoError(t, restarted.RecoverPreparedTransactions())
				require.NoError(t, restarted.RecoverPreparedTransactions())
				final, err := restarted.TxnManager.Status("tx")
				require.NoError(t, err)
				require.Equal(t, transaction.StateCommitted, final.State)
				require.Equal(t, []string{"payload-tx"}, readCommittedPayloads(t, tm, "events"))
				offset, ok := cd.GetOffset("workers", "events", 0)
				require.True(t, ok)
				want := uint64(13)
				if advanced {
					want = 21
				}
				require.Equal(t, want, offset)
			})
		}
	}
}

func TestTransactionPrepareRejectsRevokedOwnershipWithoutChangingState(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	gen := prepareTransactionGroup(t, tm, cd, "events", "workers", "old-member")
	initAndStageTransaction(t, ch, "tx", "events", "workers", "old-member", gen, 13)
	current, err := ch.TxnManager.Status("tx")
	require.NoError(t, err)
	_, err = cd.AddConsumer("workers", "new-member")
	require.NoError(t, err)
	_, err = ch.prepareTransaction(current)
	require.Error(t, err)
	status, err := ch.TxnManager.Status("tx")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, status.State)
	require.Empty(t, readCommittedPayloads(t, tm, "events"))
}

func TestPreparedTransactionRejectsRecreatedGroup(t *testing.T) {
	ch, tm, cd, _ := newDiskBackedTransactionHandler(t)
	gen := prepareTransactionGroup(t, tm, cd, "events", "workers", "old-member")
	initAndStageTransaction(t, ch, "tx", "events", "workers", "old-member", gen, 13)
	current, err := ch.TxnManager.Status("tx")
	require.NoError(t, err)
	_, err = ch.prepareTransaction(current)
	require.NoError(t, err)
	require.NoError(t, cd.RemoveConsumer("workers", "old-member"))
	require.NoError(t, cd.DeleteGroup("workers"))
	require.NoError(t, cd.RegisterGroup("events", "workers", 1))
	require.ErrorContains(t, ch.RecoverPreparedTransactions(), "incarnation mismatch")
	_, exists := cd.GetOffset("workers", "events", 0)
	require.False(t, exists)
	require.Empty(t, readCommittedPayloads(t, tm, "events"))
}
