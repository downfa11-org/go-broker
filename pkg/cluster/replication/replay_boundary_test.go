package replication

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestAwaitRecoveredPartitionReplayRequiresPersistedBoundary(t *testing.T) {
	for _, test := range []struct {
		name         string
		commit, term uint64
		ready        bool
	}{
		{name: "no committed entries"},
		{name: "historical committed prefix", commit: 1, term: 2},
		{name: "entire persisted log committed", commit: 2, term: 2, ready: true},
		{name: "uncommitted tail replaced", commit: 1, term: 3, ready: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := raft.NewInmemStore()
			require.NoError(t, store.StoreLogs([]*raft.Log{
				{Index: 1, Term: test.term, Type: raft.LogCommand},
				{Index: 2, Term: 2, Type: raft.LogCommand},
			}))
			stats := &mutableRaftStats{stats: map[string]string{
				"last_snapshot_index": "0", "last_log_index": "2",
				"commit_index": strconv.FormatUint(test.commit, 10),
			}}
			f := &fakeRecoveredPartitionFSM{pending: true, applied: test.commit}
			err := awaitRecoveredPartitionReplay(context.Background(), stats, store, f, "broker-1", 30*time.Millisecond, replayBoundary{index: 2, term: 2})
			if test.ready {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "without progress")
			}
			require.Equal(t, test.ready, f.wasFinalized())
		})
	}
}

func TestPersistedReplayBoundaryIncludesSnapshot(t *testing.T) {
	store := raft.NewInmemStore()
	snapshots := raft.NewInmemSnapshotStore()
	boundary, err := persistedReplayBoundary(store, snapshots)
	require.NoError(t, err)
	require.Equal(t, replayBoundary{}, boundary)
	require.NoError(t, store.StoreLog(&raft.Log{Index: 4, Term: 2}))
	boundary, err = persistedReplayBoundary(store, snapshots)
	require.NoError(t, err)
	require.Equal(t, replayBoundary{index: 4, term: 2}, boundary)
	sink, err := snapshots.Create(raft.SnapshotVersionMax, 5, 3, raft.Configuration{}, 0, nil)
	require.NoError(t, err)
	require.NoError(t, sink.Close())
	boundary, err = persistedReplayBoundary(store, snapshots)
	require.NoError(t, err)
	require.Equal(t, replayBoundary{index: 5, term: 3}, boundary)
	require.NoError(t, store.StoreLog(&raft.Log{Index: 6, Term: 4}))
	boundary, err = persistedReplayBoundary(store, snapshots)
	require.NoError(t, err)
	require.Equal(t, replayBoundary{index: 6, term: 4}, boundary)
}

func TestReplayBoundaryAcceptsNewerCommittedSnapshot(t *testing.T) {
	boundary := replayBoundary{index: 10, term: 2}
	for _, term := range []string{"", "2", "3"} {
		ready, err := boundary.committed(raft.NewInmemStore(), map[string]string{"last_snapshot_term": term}, 5, 5)
		if term == "" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, term == "3", ready)
	}
}
