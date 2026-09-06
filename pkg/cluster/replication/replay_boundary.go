package replication

import (
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
)

type replayBoundary struct {
	index uint64
	term  uint64
}

func persistedReplayBoundary(store raft.LogStore, snapshots raft.SnapshotStore) (replayBoundary, error) {
	var boundary replayBoundary
	index, err := store.LastIndex()
	if err != nil {
		return boundary, err
	}
	if index != 0 {
		var entry raft.Log
		if err := store.GetLog(index, &entry); err != nil {
			return boundary, err
		}
		boundary = replayBoundary{index: index, term: entry.Term}
	}
	stored, err := snapshots.List()
	if err != nil {
		return boundary, err
	}
	for _, snapshot := range stored {
		if snapshot.Index > boundary.index {
			boundary = replayBoundary{index: snapshot.Index, term: snapshot.Term}
		}
	}
	return boundary, nil
}

func (b replayBoundary) committed(store raft.LogStore, stats map[string]string, commitIndex, snapshotIndex uint64) (bool, error) {
	if commitIndex >= b.index {
		return true, nil
	}
	if commitIndex == 0 {
		return false, nil
	}
	var term uint64
	if commitIndex == snapshotIndex {
		var err error
		term, err = strconv.ParseUint(stats["last_snapshot_term"], 10, 64)
		if err != nil {
			return false, fmt.Errorf("read recovery snapshot term: %w", err)
		}
	} else {
		var entry raft.Log
		if err := store.GetLog(commitIndex, &entry); err != nil {
			return false, err
		}
		term = entry.Term
	}
	// A committed newer-term entry proves an old, uncommitted tail was superseded.
	return term > b.term, nil
}
