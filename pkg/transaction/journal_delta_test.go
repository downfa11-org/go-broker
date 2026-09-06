package transaction

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestJournalAppendsOnlyNewMessagesAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	snap := testJournalSnapshot("growing", 1, StateOpen)
	snap.Messages = []MessageOperation{{Topic: "events", Message: types.Message{Payload: strings.Repeat("a", 1<<20)}}}
	require.NoError(t, j.Append(snap))
	before := j.validEnd
	snap.Revision++
	snap.Messages = append(snap.Messages, MessageOperation{Topic: "events", Message: types.Message{Payload: "second", ControlBatchKey: []byte{1}}})
	snap.Offsets = []OffsetOperation{{Topic: "input", Group: "workers", Offset: 12}}
	require.NoError(t, j.Append(snap))
	require.Less(t, j.validEnd-before, int64(4096))
	t.Logf("bytes appended for second message: %d", j.validEnd-before)
	reopened, err := OpenJournal(path)
	require.NoError(t, err)
	state, err := reopened.Load()
	require.NoError(t, err)
	require.Equal(t, snap, state[snap.ID])
	snap.Messages[1].Message.ControlBatchKey[0] = 9
	require.Equal(t, byte(1), j.latest[snap.ID].Messages[1].Message.ControlBatchKey[0])
}

func TestJournalDeltaSurvivesCompactionAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	snap := testJournalSnapshot("compacted", 1, StateOpen)
	for i := 0; i < journalCompactionRecords+5; i++ {
		snap.Messages = append(snap.Messages, MessageOperation{Message: types.Message{Payload: "event"}})
		snap.Revision++
		require.NoError(t, j.Append(snap))
	}
	require.Less(t, j.records, journalCompactionRecords)
	j, err = OpenJournal(path)
	require.NoError(t, err)
	state, err := j.Load()
	require.NoError(t, err)
	require.Equal(t, snap, state[snap.ID])
	snap.State, snap.Ready = StateCommitting, true
	snap.Revision++
	require.NoError(t, j.Append(snap))
	_, payloads := readDeltaJournal(t, path)
	var last journalRecord
	require.NoError(t, json.Unmarshal(payloads[len(payloads)-1], &last))
	require.Equal(t, journalDeltaVersion, last.Version)
	require.Empty(t, last.Transaction.Messages)
	require.Equal(t, len(snap.Messages), *last.MessagePrefix)
	state, err = j.Load()
	require.NoError(t, err)
	require.Equal(t, snap, state[snap.ID])
}

func TestJournalDeltaFallsBackForChangedHistoryOrIdentity(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"payload":  func(s *Snapshot) { s.Messages[0].Message.Payload = "changed" },
		"control":  func(s *Snapshot) { s.Messages[0].Message.ControlBatchValue[0]++ },
		"shorter":  func(s *Snapshot) { s.Messages = nil; s.State = StateAborted },
		"producer": func(s *Snapshot) { s.Producer = "replacement"; s.Revision = 1 },
		"epoch":    func(s *Snapshot) { s.Epoch++ },
		"created":  func(s *Snapshot) { s.CreatedAt = s.CreatedAt.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal")
			j, err := OpenJournal(path)
			require.NoError(t, err)
			snap := testJournalSnapshot("changed", 3, StateOpen)
			snap.Messages = []MessageOperation{{Message: types.Message{Payload: "before", ControlBatchValue: []byte{1}}}}
			require.NoError(t, j.Append(snap))
			mutate(snap)
			require.NoError(t, j.Append(snap))
			_, payloads := readDeltaJournal(t, path)
			var last journalRecord
			require.NoError(t, json.Unmarshal(payloads[1], &last))
			require.Equal(t, journalFormatVersion, last.Version)
			state, err := j.Load()
			require.NoError(t, err)
			require.Equal(t, snap, state[snap.ID])
		})
	}
}

func TestJournalDeltaRejectsMissingOrMismatchedBasis(t *testing.T) {
	snap := testJournalSnapshot("chain", 1, StateOpen)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: "first"}}}
	first, basis, err := encodeJournalUpdate(snap, nil, journalBase{})
	require.NoError(t, err)
	next := *snap
	next.Messages = append(append([]MessageOperation(nil), snap.Messages...), MessageOperation{Message: types.Message{Payload: "second"}})
	next.Revision++
	second, _, err := encodeJournalUpdate(&next, snap, basis)
	require.NoError(t, err)
	for name, mutate := range map[string]func(*journalRecord){
		"digest":         func(r *journalRecord) { r.BaseDigest = strings.Repeat("0", 64) },
		"count":          func(r *journalRecord) { *r.MessagePrefix++ },
		"missing count":  func(r *journalRecord) { r.MessagePrefix = nil },
		"negative count": func(r *journalRecord) { *r.MessagePrefix = -1 },
		"producer":       func(r *journalRecord) { r.Transaction.Producer = "wrong" },
		"epoch":          func(r *journalRecord) { r.Transaction.Epoch++ },
		"missing base":   func(r *journalRecord) {},
	} {
		t.Run(name, func(t *testing.T) {
			var record journalRecord
			require.NoError(t, json.Unmarshal(second, &record))
			mutate(&record)
			bad, err := json.Marshal(record)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "journal")
			data := frameJournalPayload(first)
			if name == "missing base" {
				data = nil
			}
			data = append(data, frameJournalPayload(bad)...)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			j, err := OpenJournal(path)
			require.NoError(t, err)
			_, err = j.Load()
			require.ErrorContains(t, err, "delta base mismatch")
			require.False(t, j.loaded)
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), info.Size())
		})
	}
	path := filepath.Join(t.TempDir(), "journal")
	data := append(frameJournalPayload(first), frameJournalPayload(second)...)
	data = append(data, frameJournalPayload(second)...)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	j, err := OpenJournal(path)
	require.NoError(t, err)
	_, err = j.Load()
	require.ErrorContains(t, err, "delta base mismatch")
}

func TestJournalDeltaRepairAndRetryRetainsBasis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	snap := testJournalSnapshot("partial", 1, StateOpen)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: "one"}}}
	require.NoError(t, j.Append(snap))
	firstEnd := j.validEnd
	snap.Messages = append(snap.Messages, MessageOperation{Message: types.Message{Payload: "two"}})
	require.NoError(t, j.Append(snap))
	data, _ := readDeltaJournal(t, path)
	for _, end := range []int{int(firstEnd) + 2, len(data) - 5, len(data) - 1} {
		require.NoError(t, os.WriteFile(path, data[:end], 0o600))
		j, err = OpenJournal(path)
		require.NoError(t, err)
		state, err := j.Load()
		require.NoError(t, err)
		require.Len(t, state[snap.ID].Messages, 1)
		require.Equal(t, firstEnd, j.validEnd)
		require.NoError(t, j.Append(snap))
		state, err = j.Load()
		require.NoError(t, err)
		require.Equal(t, snap, state[snap.ID])
	}
}

func TestJournalFailedRewriteReloadsDurableBasis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	snap := testJournalSnapshot("durable", 1, StateOpen)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: "one"}}}
	require.NoError(t, j.Append(snap))
	j.path = filepath.Join(t.TempDir(), "missing", "journal")
	require.Error(t, j.Rewrite(map[string]*Snapshot{"unwritten": testJournalSnapshot("unwritten", 1, StateOpen)}))
	require.False(t, j.loaded)
	j.path = path
	snap.Messages = append(snap.Messages, MessageOperation{Message: types.Message{Payload: "two"}})
	require.NoError(t, j.Append(snap))
	state, err := j.Load()
	require.NoError(t, err)
	require.NotContains(t, state, "unwritten")
	require.Equal(t, snap, state[snap.ID])
}

func TestJournalDeltaFullStateSizeAccounting(t *testing.T) {
	snap := testJournalSnapshot("sizes", 1, StateOpen)
	for i := 0; i < 10; i++ {
		previous := snapshot(transactionFromSnapshot(snap))
		_, basis, err := encodeJournalUpdate(previous, nil, journalBase{})
		require.NoError(t, err)
		snap.Messages = append(snap.Messages, MessageOperation{Topic: "<&>", Message: types.Message{Payload: "\"한글\n<", ControlBatchValue: []byte{0, 1, 2}}})
		_, nextBasis, err := encodeJournalUpdate(snap, previous, basis)
		require.NoError(t, err)
		all, err := json.Marshal(snap.Messages)
		require.NoError(t, err)
		require.Equal(t, len(all)-2, nextBasis.messageBytes)
		full, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
		require.NoError(t, err)
		headroom := maxJournalRecordBytes - len(full)
		require.NoError(t, validateJournalStateSize(snap, len(snap.Messages), nextBasis.messageBytes+headroom))
		require.Error(t, validateJournalStateSize(snap, len(snap.Messages), nextBasis.messageBytes+headroom+1))
	}
	previous := snapshot(transactionFromSnapshot(snap))
	_, basis, err := encodeJournalUpdate(previous, nil, journalBase{})
	require.NoError(t, err)
	basis.messageBytes = maxJournalRecordBytes
	_, _, err = encodeJournalUpdate(snap, previous, basis)
	require.ErrorContains(t, err, "exceeds journal limit")
	payload, goodBasis, err := encodeJournalUpdate(snap, previous, journalBase{digest: basis.digest, messageBytes: 1})
	require.NoError(t, err)
	require.NotEmpty(t, goodBasis.digest)
	_, _, err = decodeJournalUpdate(payload, map[string]*Snapshot{snap.ID: previous}, map[string]journalBase{snap.ID: basis})
	require.ErrorContains(t, err, "exceeds journal limit")
}

func TestJournalFailedAppendDoesNotAdvanceBasis(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	j, err := OpenJournal(path)
	require.NoError(t, err)
	snap := testJournalSnapshot("retry", 1, StateOpen)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: "one"}}}
	require.NoError(t, j.Append(snap))
	basis, end := j.bases[snap.ID], j.validEnd
	snap.Messages = append(snap.Messages, MessageOperation{Message: types.Message{Payload: "two"}})
	j.path = t.TempDir()
	require.Error(t, j.Append(snap))
	require.Equal(t, basis, j.bases[snap.ID])
	require.Equal(t, end, j.validEnd)
	j.path = path
	require.NoError(t, j.Append(snap))
	state, err := j.Load()
	require.NoError(t, err)
	require.Equal(t, snap, state[snap.ID])
}

func BenchmarkJournalRecordEncoding(b *testing.B) {
	previous := testJournalSnapshot("benchmark", 1, StateOpen)
	for i := 0; i < 512; i++ {
		previous.Messages = append(previous.Messages, MessageOperation{Message: types.Message{Payload: strings.Repeat("x", 4096)}})
	}
	_, basis, err := encodeJournalUpdate(previous, nil, journalBase{})
	require.NoError(b, err)
	next := *previous
	next.Messages = append(append([]MessageOperation(nil), previous.Messages...), MessageOperation{Message: types.Message{Payload: "next"}})
	next.Revision++
	for name, encode := range map[string]func() ([]byte, error){
		"full_snapshot": func() ([]byte, error) {
			return json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: &next})
		},
		"append_delta": func() ([]byte, error) {
			payload, _, err := encodeJournalUpdate(&next, previous, basis)
			return payload, err
		},
	} {
		b.Run(name, func(b *testing.B) {
			payload, err := encode()
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := encode()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(payload)), "record-B")
		})
	}
}

func readDeltaJournal(t *testing.T, path string) ([]byte, [][]byte) {
	t.Helper()
	// #nosec G304 -- path is a journal owned by this test.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var payloads [][]byte
	for offset := 0; offset < len(data); {
		require.GreaterOrEqual(t, len(data)-offset, journalRecordOverhead)
		size := int(binary.BigEndian.Uint32(data[offset:]))
		require.LessOrEqual(t, size, len(data)-offset-journalRecordOverhead)
		payloads = append(payloads, data[offset+4:offset+4+size])
		offset += size + journalRecordOverhead
	}
	return data, payloads
}

func frameJournalPayload(payload []byte) []byte {
	data := make([]byte, len(payload)+journalRecordOverhead)
	binary.BigEndian.PutUint32(data, uint32(len(payload))) // #nosec G115 -- bounded test fixture.
	copy(data[4:], payload)
	binary.BigEndian.PutUint32(data[4+len(payload):], crc32.ChecksumIEEE(payload))
	return data
}
