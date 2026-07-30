package raft

import (
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Starts a leader whose state machine Save() is driven by saveFunc.
func runLeaderWithSaveFunc(t *testing.T, saveFunc func() ([]byte, error)) *server {
	t.Helper()

	s := newTestServer("1", &testTransporter{})
	srv := s.(*server)
	srv.stateMachine = &testStateMachine{
		saveFunc:     saveFunc,
		recoveryFunc: func([]byte) error { return nil },
	}
	if err := s.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(s.Stop)

	if _, err := s.Do(&DefaultJoinCommand{Name: s.Name()}); err != nil {
		t.Fatalf("self join: %v", err)
	}

	// TakeSnapshot does nothing until something is committed past startIndex.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if lastIndex, _ := srv.log.commitInfo(); lastIndex > srv.log.startIndex {
			return srv
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a committed entry to snapshot")
		}
		time.Sleep(time.Millisecond)
	}
}

// The event loops ask for a snapshot on every iteration and callers can ask at
// any time, so several TakeSnapshot goroutines used to build one at the same
// time. Whichever finished first cleared the shared pendingSnapshot field the
// others were still filling in, so they either dereferenced nil or stored a nil
// current snapshot.
func TestTakeSnapshotOneAtATime(t *testing.T) {
	var inFlight int32
	var overlapped atomic.Bool

	s := runLeaderWithSaveFunc(t, func() ([]byte, error) {
		if atomic.AddInt32(&inFlight, 1) > 1 {
			overlapped.Store(true)
		}
		// Hold the snapshot open long enough for every caller to pile up.
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return []byte("state"), nil
	})

	var wg sync.WaitGroup
	var taken int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := s.TakeSnapshot(); {
			case err == nil:
				atomic.AddInt32(&taken, 1)
			case errors.Is(err, ErrSnapshotInProgress):
			default:
				t.Errorf("TakeSnapshot: %v", err)
			}
		}()
	}
	wg.Wait()

	if overlapped.Load() {
		t.Error("two snapshots were built at the same time")
	}
	if taken == 0 {
		t.Error("no snapshot was taken")
	}
	if s.snapshot == nil {
		t.Fatal("no snapshot was saved")
	}
}

// Records that the state machine was handed the leader's state.
func recordRecovery(recovered *bool) func([]byte) error {
	return func([]byte) error {
		*recovered = true
		return nil
	}
}

// Starts a follower waiting for a snapshot recovery, whose state machine answers
// a handed-over state with recoveryFunc.
func runRecoveringFollower(t *testing.T, recoveryFunc func([]byte) error) *server {
	t.Helper()

	s := newTestServer("1", &testTransporter{})
	srv := s.(*server)
	srv.stateMachine = &testStateMachine{
		saveFunc:     func() ([]byte, error) { return []byte("state"), nil },
		recoveryFunc: recoveryFunc,
	}
	if err := s.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(s.Stop)

	// A term of our own, to see whether a failed recovery overwrites it.
	srv.currentTerm = 9

	if resp := s.RequestSnapshot(&SnapshotRequest{LastIndex: 5, LastTerm: 1}); !resp.Success {
		t.Fatal("snapshot request refused")
	}
	return srv
}

func recover5(s *server) *SnapshotRecoveryResponse {
	return s.SnapshotRecoveryRequest(&SnapshotRecoveryRequest{
		LeaderName: "2",
		LastIndex:  5,
		LastTerm:   1,
		Peers:      []*Peer{{Name: "2", ConnectionString: ""}},
		State:      []byte("state"),
	})
}

// Whatever a declined recovery got through, it has to leave the entries it would
// have replaced in place, must not move the commit index past them, and has to
// hand the server back so the leader can try again.
func assertRecoveryDeclined(t *testing.T, s *server) {
	t.Helper()

	if s.log.startIndex == 5 {
		t.Error("log was compacted by a failed recovery")
	}
	// A commit index past the entries we kept makes commitInfo read off the end.
	if lastIndex, _ := s.log.commitInfo(); lastIndex != 0 {
		t.Errorf("commit index advanced to %d with an uncompacted log", lastIndex)
	}
	// snapshotLoop does not answer a SnapshotRequest, so staying there would leave
	// the leader unable to start the handshake again.
	if state := s.State(); state != Follower {
		t.Errorf("server left in %q after a failed recovery", state)
	}
}

// A recovery that never got its snapshot onto disk must not have installed any
// of what the snapshot carried either.
func assertNothingInstalled(t *testing.T, s *server, recovered bool) {
	t.Helper()

	if recovered {
		t.Error("state machine was recovered before the snapshot was saved")
	}
	if s.currentTerm != 9 {
		t.Errorf("term overwritten to %d before the snapshot was saved", s.currentTerm)
	}
	if len(s.peers) != 0 {
		t.Errorf("peers replaced before the snapshot was saved: %d", len(s.peers))
	}
}

// Recovery discards the log it is replacing, so a snapshot that never reached
// disk must not be reported as recovered: the server would come back with
// neither the entries nor the snapshot that was supposed to replace them.
func TestSnapshotRecoveryDeclinedWhenSaveFails(t *testing.T) {
	var recovered bool
	s := runRecoveringFollower(t, recordRecovery(&recovered))

	// Put a file where the snapshot directory belongs, so writing one fails.
	snapshotDir := path.Join(s.Path(), "snapshot")
	if err := os.RemoveAll(snapshotDir); err != nil {
		t.Fatalf("remove snapshot dir: %v", err)
	}
	if err := os.WriteFile(snapshotDir, []byte("not a directory"), 0600); err != nil {
		t.Fatalf("block snapshot dir: %v", err)
	}

	if resp := recover5(s); resp.Success {
		t.Error("recovery reported success with no snapshot on disk")
	}
	if s.snapshot != nil {
		t.Error("an unsaved snapshot was made current")
	}
	assertRecoveryDeclined(t, s)
	assertNothingInstalled(t, s, recovered)
}

// A state machine that will not take the leader's state used to bring the
// process down, and by then the log those entries lived in was already gone, so
// every restart met the same snapshot and died the same way. It is a declined
// recovery like any other: the log stays and the leader can come back to it.
func TestSnapshotRecoveryDeclinedWhenStateMachineRefuses(t *testing.T) {
	s := runRecoveringFollower(t, func([]byte) error {
		return errors.New("state machine will not take it")
	})

	if resp := recover5(s); resp.Success {
		t.Error("recovery reported success although the state was not taken")
	}
	assertRecoveryDeclined(t, s)
}

// Compaction only drops entries the snapshot already covers, so failing it does
// not undo the recovery on disk. What it must not do is report success or move
// the commit index, and it must not leave the staged file behind.
func TestSnapshotRecoveryDeclinedWhenCompactFails(t *testing.T) {
	var recovered bool
	s := runRecoveringFollower(t, recordRecovery(&recovered))

	// compact writes the surviving entries to <log>.new before renaming it over
	// the log; a directory there fails the open.
	if err := os.Mkdir(s.LogPath()+".new", 0700); err != nil {
		t.Fatalf("block compaction: %v", err)
	}

	if resp := recover5(s); resp.Success {
		t.Error("recovery reported success although the log was not replaced")
	}
	assertRecoveryDeclined(t, s)

	if _, err := os.Stat(s.SnapshotPath(5, 1) + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("staged snapshot left behind: %v", err)
	}
}

// A recovery covering the same point as the snapshot already on disk writes to
// that snapshot's own path. Whatever happens to the rest of the recovery, that
// path has to hold a whole snapshot the server can come back from, never one
// written over in place with the tail of the last one still attached.
func TestSnapshotAtSamePointStaysLoadable(t *testing.T) {
	var recovered bool
	s := runRecoveringFollower(t, recordRecovery(&recovered))

	// Longer than the state the recovery carries, so a write over it in place
	// would leave a tail behind and fail the checksum.
	existing := &Snapshot{5, 1, nil, []byte(strings.Repeat("original", 64)), s.SnapshotPath(5, 1)}
	if err := existing.save(); err != nil {
		t.Fatalf("save existing snapshot: %v", err)
	}
	s.snapshot = existing

	if err := os.Mkdir(s.LogPath()+".new", 0700); err != nil {
		t.Fatalf("block compaction: %v", err)
	}

	if resp := recover5(s); resp.Success {
		t.Error("recovery reported success although the log was not replaced")
	}
	assertRecoveryDeclined(t, s)

	if _, err := os.Stat(existing.Path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("staged snapshot left behind: %v", err)
	}

	// The server has to be able to come back from what is on disk.
	s.Stop()
	if err := s.LoadSnapshot(); err != nil {
		t.Fatalf("snapshot on disk no longer loads: %v", err)
	}
	if s.snapshot.LastIndex != 5 || s.snapshot.LastTerm != 1 {
		t.Errorf("loaded snapshot covers %d/%d", s.snapshot.LastTerm, s.snapshot.LastIndex)
	}
}

// A snapshot that fails must not leave the server thinking one is still in
// flight, or it never snapshots or compacts its log again.
func TestTakeSnapshotAfterFailure(t *testing.T) {
	var attempts int32

	s := runLeaderWithSaveFunc(t, func() ([]byte, error) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return nil, errors.New("state machine unavailable")
		}
		return []byte("state"), nil
	})

	if err := s.TakeSnapshot(); err == nil {
		t.Fatal("expected the first snapshot to fail")
	}
	if err := s.TakeSnapshot(); err != nil {
		t.Fatalf("snapshot after a failed one: %v", err)
	}
	if s.snapshot == nil {
		t.Fatal("no snapshot was saved")
	}
}
