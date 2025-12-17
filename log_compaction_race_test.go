package raft

import (
	"os"
	"testing"
)

// TestGetEntriesAfterIndexBeyondEnd reproduces the panic in issue #7810
// https://github.com/seaweedfs/seaweedfs/issues/7810
//
// The issue occurs when:
// 1. Leader sets peer's prevLogIndex to its current log index (e.g., 1000)
// 2. Log compaction runs, removing entries and updating startIndex
// 3. peer.flush() calls getEntriesAfter(prevLogIndex) with the stale index
// 4. The index is now beyond the end of the compacted log, causing a panic
//
// The expected behavior is to return nil (triggering snapshot fallback)
// instead of panicking.
func TestGetEntriesAfterIndexBeyondEnd(t *testing.T) {
	path := getLogPath()
	log := newLog()
	log.ApplyFunc = func(e *LogEntry, c Command) (interface{}, error) {
		return nil, nil
	}
	if err := log.open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.close()
	defer os.Remove(path)

	// Create 1000 log entries
	for i := uint64(1); i <= 1000; i++ {
		e, _ := newLogEntry(log, nil, i, 1, &testCommand1{Val: "foo", I: int(i)})
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("Unable to append entry %d: %v", i, err)
		}
	}

	// Commit all entries (required before compaction)
	if err := log.setCommitIndex(1000); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}

	// Verify log state before compaction
	if log.currentIndex() != 1000 {
		t.Fatalf("Expected current index 1000, got %d", log.currentIndex())
	}

	// Compact the log - keep only entries after index 800
	// This simulates what happens after a snapshot
	if err := log.compact(800, 1); err != nil {
		t.Fatalf("Unable to compact: %v", err)
	}

	// After compaction:
	// - startIndex = 800
	// - entries = [801, 802, ..., 1000] (200 entries)
	// - max valid index = 800 + 200 = 1000
	if log.startIndex != 800 {
		t.Fatalf("Expected startIndex 800, got %d", log.startIndex)
	}
	if len(log.entries) != 200 {
		t.Fatalf("Expected 200 entries, got %d", len(log.entries))
	}

	// Test 1: Valid index within range should work
	entries, term := log.getEntriesAfter(900, 100)
	if entries == nil {
		t.Fatalf("Expected entries for valid index 900")
	}
	if term != 1 {
		t.Fatalf("Expected term 1, got %d", term)
	}

	// Test 2: Index at startIndex should return all entries (up to maxLogEntriesPerRequest)
	entries, term = log.getEntriesAfter(800, 100)
	if entries == nil {
		t.Fatalf("Expected entries for startIndex")
	}
	// When index == startIndex, it returns all entries (200), limited by max (100)
	if len(entries) != 100 && len(entries) != 200 {
		t.Fatalf("Expected 100-200 entries, got %d", len(entries))
	}

	// Test 3: Index before startIndex should return nil (for snapshot fallback)
	entries, term = log.getEntriesAfter(500, 100)
	if entries != nil || term != 0 {
		t.Fatalf("Expected nil for index before startIndex, got entries=%v term=%d", entries, term)
	}

	// Test 4: Index beyond end of log should NOT panic
	// This is the bug scenario from issue #7810
	// The index 1050 could be a stale prevLogIndex from before compaction
	//
	// Current behavior: PANICS with "raft: Index is beyond end of log: 200 1050"
	// Expected behavior: Return nil to trigger snapshot fallback
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("getEntriesAfter should not panic for index beyond log end: %v", r)
		}
	}()

	entries, term = log.getEntriesAfter(1050, 100)
	if entries != nil || term != 0 {
		t.Fatalf("Expected nil for index beyond log end, got entries=%v term=%d", entries, term)
	}
}

// TestGetEntriesAfterRaceWithCompaction simulates the race condition
// between peer.flush() and log compaction that causes issue #7810
func TestGetEntriesAfterRaceWithCompaction(t *testing.T) {
	path := getLogPath()
	log := newLog()
	log.ApplyFunc = func(e *LogEntry, c Command) (interface{}, error) {
		return nil, nil
	}
	if err := log.open(path); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}
	defer log.close()
	defer os.Remove(path)

	// Create a log with entries
	for i := uint64(1); i <= 500; i++ {
		e, _ := newLogEntry(log, nil, i, 1, &testCommand1{Val: "foo", I: int(i)})
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("Unable to append entry %d: %v", i, err)
		}
	}

	// Commit entries
	if err := log.setCommitIndex(500); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}

	// Simulate: Leader becomes leader and sets peer.prevLogIndex = 500
	prevLogIndex := log.currentIndex() // 500

	// Simulate: Snapshot/compaction runs, keeping only last 50 entries
	// After this: startIndex=450, entries=[451..500], max index=500
	if err := log.compact(450, 1); err != nil {
		t.Fatalf("Unable to compact: %v", err)
	}

	// More entries are added
	for i := uint64(501); i <= 550; i++ {
		e, _ := newLogEntry(log, nil, i, 1, &testCommand1{Val: "bar", I: int(i)})
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("Unable to append entry %d: %v", i, err)
		}
	}

	// Another compaction happens (e.g., during heavy writes)
	// After: startIndex=540, entries=[541..550], max index=550
	if err := log.setCommitIndex(550); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}
	if err := log.compact(540, 1); err != nil {
		t.Fatalf("Unable to compact: %v", err)
	}

	// Now the peer's prevLogIndex (500) is between startIndex (540) and max (550)
	// Actually, 500 < 540, so it would return nil (correct behavior)
	
	// But what if prevLogIndex was updated to something in between compactions?
	// Simulate: prevLogIndex was set to 545 during first compaction window
	prevLogIndex = 545

	// Then another aggressive compaction happens
	for i := uint64(551); i <= 600; i++ {
		e, _ := newLogEntry(log, nil, i, 1, &testCommand1{Val: "baz", I: int(i)})
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("Unable to append entry %d: %v", i, err)
		}
	}
	if err := log.setCommitIndex(600); err != nil {
		t.Fatalf("Unable to commit: %v", err)
	}
	// Compact to only keep last 5 entries
	// After: startIndex=595, entries=[596..600], max=600
	if err := log.compact(595, 1); err != nil {
		t.Fatalf("Unable to compact: %v", err)
	}

	// Now prevLogIndex=545 is:
	// - Greater than startIndex (595)? No, 545 < 595
	// So it would return nil (correct)

	// The real problematic case is when prevLogIndex > startIndex but > max
	// This can happen if:
	// 1. prevLogIndex is set to current log end (e.g., 600)
	// 2. Log gets truncated (not compacted) due to leader change
	// 3. New log has fewer entries

	// Simulate this by manually setting startIndex to create the condition
	log.mutex.Lock()
	log.startIndex = 590
	log.entries = log.entries[5:] // Only keep last 5 entries [596..600]
	log.mutex.Unlock()

	// Now: startIndex=590, entries=5, max valid=595
	// But we call with prevLogIndex=600
	prevLogIndex = 600

	// This should trigger the panic in the current code
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("getEntriesAfter panicked for index beyond log end: %v\n"+
				"This is bug #7810 - should return nil for snapshot fallback instead", r)
		}
	}()

	entries, term := log.getEntriesAfter(prevLogIndex, 100)
	if entries != nil {
		t.Fatalf("Expected nil for index beyond log, got %d entries", len(entries))
	}
	t.Logf("Correctly returned nil, term=%d for index beyond log end", term)
}

