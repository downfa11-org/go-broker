package transaction

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/util"
)

func TestDecodeJournalSnapshotRejectsUnversionedAndUnknownVersions(t *testing.T) {
	snapshot := testJournalSnapshot("tx-versioned", 1, StateCommitted)
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeJournalUpdate(payload, nil, nil); err == nil {
		t.Fatal("expected an unversioned journal snapshot to fail")
	}

	payload, err = json.Marshal(journalRecord{Version: journalDeltaVersion + 1, Transaction: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeJournalUpdate(payload, nil, nil); err == nil {
		t.Fatal("expected an unsupported journal version to fail")
	}
}
func TestJournalLoadsLatestSnapshotPerTransactionalID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	first := testJournalSnapshot("tx-a", 1, StateOpen)
	second := testJournalSnapshot("tx-a", 2, StateCommitting)
	other := testJournalSnapshot("tx-b", 1, StateCommitted)
	for _, snap := range []*Snapshot{first, second, other, second} {
		if err := journal.Append(snap); err != nil {
			t.Fatal(err)
		}
	}

	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected two transactions, got %d", len(loaded))
	}
	if got := loaded["tx-a"]; got == nil || got.Revision != 2 || got.State != StateCommitting {
		t.Fatalf("unexpected tx-a snapshot: %+v", got)
	}
	if got := loaded["tx-b"]; got == nil || got.State != StateCommitted {
		t.Fatalf("unexpected tx-b snapshot: %+v", got)
	}
}

func TestJournalRepairsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded["tx-a"] == nil {
		t.Fatal("valid journal record was not recovered")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("expected incomplete tail to be truncated to %d, got %d", before.Size(), after.Size())
	}
}

func TestJournalAppendDiscardsUnacknowledgedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(); err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 20, '{'}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := journal.Append(testJournalSnapshot("tx-b", 1, StateCommitted)); err != nil {
		t.Fatal(err)
	}
	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded["tx-a"] == nil || loaded["tx-b"] == nil {
		t.Fatalf("unexpected recovered snapshots: %+v", loaded)
	}
}
func TestJournalRejectsCorruptNonTailRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-b", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[5] ^= 0xff
	// #nosec G703 -- path is the journal file created beneath t.TempDir.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(); err == nil {
		t.Fatal("expected non-tail checksum corruption to fail recovery")
	}
}

func testJournalSnapshot(id string, revision uint64, state State) *Snapshot {
	revisionSeconds, ok := util.SafeUint64ToInt64(revision)
	if !ok {
		panic("test revision exceeds int64")
	}
	now := time.Unix(1_700_000_000+revisionSeconds, 0).UTC()
	return &Snapshot{
		ID:        id,
		Producer:  "producer-" + id,
		Epoch:     1,
		Revision:  revision,
		State:     state,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
