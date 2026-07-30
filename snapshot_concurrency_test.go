package raft

import (
	"errors"
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
