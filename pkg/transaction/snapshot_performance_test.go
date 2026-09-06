package transaction

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSnapshotByIDReturnsIsolatedState(t *testing.T) {
	m := &Manager{txns: map[string]*Transaction{"one": {ID: "one", Messages: []MessageOperation{{Message: types.Message{ControlBatchKey: []byte{1}, ControlBatchValue: []byte{2}}}}}}}
	snap, ok := m.SnapshotByID("one")
	require.True(t, ok)
	snap.Messages[0].Message.ControlBatchKey[0] = 9
	snap.Messages[0].Message.ControlBatchValue[0] = 9
	again, ok := m.SnapshotByID("one")
	require.True(t, ok)
	require.Equal(t, byte(1), again.Messages[0].Message.ControlBatchKey[0])
	require.Equal(t, byte(2), again.Messages[0].Message.ControlBatchValue[0])
	missing, ok := m.SnapshotByID("missing")
	require.False(t, ok)
	require.Nil(t, missing)
}

func TestLargeLiveJournalDoesNotCompactEveryAppend(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "journal"))
	require.NoError(t, err)
	state := make(map[string]*Snapshot)
	for i := 0; i < journalCompactionRecords; i++ {
		id := fmt.Sprint(i)
		state[id] = testJournalSnapshot(id, 1, StateCommitted)
	}
	require.NoError(t, j.Rewrite(state))
	require.False(t, j.shouldCompactLocked())
	require.NoError(t, j.Append(testJournalSnapshot("0", 2, StateCommitted)))
	require.False(t, j.shouldCompactLocked())
	require.Equal(t, journalCompactionRecords+1, j.records)
	loaded, err := j.Load()
	require.NoError(t, err)
	require.Len(t, loaded, journalCompactionRecords)
	require.Equal(t, uint64(2), loaded["0"].Revision)
}

func BenchmarkTransactionSnapshotSelection(b *testing.B) {
	m := &Manager{txns: make(map[string]*Transaction)}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprint(i)
		m.txns[id] = &Transaction{ID: id, Messages: []MessageOperation{{Message: types.Message{Payload: "event"}}}}
	}
	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.SnapshotByID("0")
		}
	})
	b.Run("whole_map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.ExportState()
		}
	})
}
