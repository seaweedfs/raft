package raft

import (
	"os"
	"path"
	"testing"
)

// Snapshot names carry the term and index they cover, and comparing them as text
// puts "1_10.ss" before "1_9.ss". A server that loaded the wrong one would come
// back at an index it had already moved past.
func TestLatestSnapshotName(t *testing.T) {
	tests := []struct {
		name      string
		filenames []string
		want      string
	}{
		{"none", nil, ""},
		{"single", []string{"1_5.ss"}, "1_5.ss"},
		{"index past a digit", []string{"1_9.ss", "1_10.ss"}, "1_10.ss"},
		{"index over term", []string{"9_1.ss", "1_10.ss"}, "1_10.ss"},
		{"same index, later term", []string{"1_10.ss", "2_10.ss"}, "2_10.ss"},
		{"ignores other files", []string{"1_5.ss.tmp", "notes.txt", "1_5.ss"}, "1_5.ss"},
		{"only other files", []string{"1_5.ss.tmp", "notes.txt"}, ""},
		// A snapshot is staged next to its own path before it is put in place, so
		// one left behind can cover more of the log than the one that counts.
		{"ignores a staged snapshot ahead of it", []string{"1_5.ss", "1_10.ss.tmp"}, "1_5.ss"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := latestSnapshotName(test.filenames); got != test.want {
				t.Errorf("latestSnapshotName(%v) = %q, want %q", test.filenames, got, test.want)
			}
		})
	}
}

// The same thing through LoadSnapshot, with both snapshots really on disk.
func TestLoadSnapshotPicksTheHighestIndex(t *testing.T) {
	s := newTestServer("1", &testTransporter{})
	srv := s.(*server)
	srv.stateMachine = &testStateMachine{
		saveFunc:     func() ([]byte, error) { return nil, nil },
		recoveryFunc: func([]byte) error { return nil },
	}
	if err := os.MkdirAll(path.Join(s.Path(), "snapshot"), 0700); err != nil {
		t.Fatalf("create snapshot dir: %v", err)
	}

	for _, snapshot := range []*Snapshot{
		{9, 1, nil, []byte("older"), s.SnapshotPath(9, 1)},
		{10, 1, nil, []byte("newer"), s.SnapshotPath(10, 1)},
	} {
		if err := snapshot.save(); err != nil {
			t.Fatalf("save snapshot: %v", err)
		}
	}

	if err := s.LoadSnapshot(); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if srv.snapshot.LastIndex != 10 {
		t.Errorf("loaded the snapshot at index %d, want 10", srv.snapshot.LastIndex)
	}
	if string(srv.snapshot.State) != "newer" {
		t.Errorf("loaded state %q, want %q", srv.snapshot.State, "newer")
	}
}
