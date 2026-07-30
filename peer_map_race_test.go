package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// The peer map is read outside the event loop — Peers hands out a copy,
// FlushCommitIndex writes the configuration, TakeSnapshot records the membership
// in a snapshot — while AddPeer and RemovePeer write it. Every one of those has
// to hold the same lock: a reader that takes it alone is still racing a writer
// that does not, and a concurrent map iteration and map write is answered by the
// runtime killing the process.
//
// The server is left stopped so that this reports on the peer map alone, rather
// than on the log and snapshot fields its loops would be touching at the same
// time.
func TestPeerMapReadWhileMembershipChanges(t *testing.T) {
	s := newTestServer("1", &testTransporter{})

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			name := fmt.Sprintf("peer-%d", i%4)
			if err := s.AddPeer(name, ""); err != nil {
				t.Errorf("AddPeer: %v", err)
				return
			}
			if err := s.RemovePeer(name); err != nil {
				t.Errorf("RemovePeer: %v", err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Peers()
			s.MemberCount()
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}
