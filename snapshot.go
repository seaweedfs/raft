package raft

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/seaweedfs/raft/protobuf"
	"google.golang.org/protobuf/proto"
)

// Snapshot represents an in-memory representation of the current state of the system.
type Snapshot struct {
	LastIndex uint64 `json:"lastIndex"`
	LastTerm  uint64 `json:"lastTerm"`

	// Cluster configuration.
	Peers []*Peer `json:"peers"`
	State []byte  `json:"state"`
	Path  string  `json:"path"`
}

// The request sent to a server to start from the snapshot.
type SnapshotRecoveryRequest struct {
	LeaderName string
	LastIndex  uint64
	LastTerm   uint64
	Peers      []*Peer
	State      []byte
}

// The response returned from a server appending entries to the log.
type SnapshotRecoveryResponse struct {
	Term        uint64
	Success     bool
	CommitIndex uint64
}

// The request sent to a server to start from the snapshot.
type SnapshotRequest struct {
	LeaderName string
	LastIndex  uint64
	LastTerm   uint64
}

// The response returned if the follower entered snapshot state
type SnapshotResponse struct {
	Success bool `json:"success"`
}

// save writes the snapshot to file and puts it in place.
func (ss *Snapshot) save() error {
	if err := ss.stage(); err != nil {
		return err
	}
	if err := ss.commit(); err != nil {
		ss.discard()
		return err
	}
	return nil
}

// Where a snapshot is written before it takes its place on disk. The suffix
// keeps it out of the snapshots LoadSnapshot picks from.
func (ss *Snapshot) stagePath() string {
	return ss.Path + ".tmp"
}

// stage writes the snapshot beside its own path, so whatever is already there
// is left alone until commit puts this one there instead. A caller with a
// fallible step before the snapshot may be installed stages it, and the file it
// would replace survives an abandoned recovery intact.
func (ss *Snapshot) stage() error {
	file, err := os.OpenFile(ss.stagePath(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	err = ss.write(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		ss.discard()
		return err
	}

	return nil
}

// commit puts a staged snapshot at its own path, replacing what was there. The
// rename is flushed before this returns, since the log compaction that follows
// it must not be the only part of a recovery to outlive a crash.
func (ss *Snapshot) commit() error {
	if err := os.Rename(ss.stagePath(), ss.Path); err != nil {
		return err
	}
	return syncDir(path.Dir(ss.Path))
}

// discard drops a staged snapshot that will not be installed.
func (ss *Snapshot) discard() {
	os.Remove(ss.stagePath())
}

// write serializes the snapshot behind its checksum and flushes it to disk.
func (ss *Snapshot) write(file *os.File) error {
	// Serialize to JSON.
	b, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	// Generate checksum and write it to disk.
	checksum := crc32.ChecksumIEEE(b)
	if _, err = fmt.Fprintf(file, "%08x\n", checksum); err != nil {
		return err
	}

	// Write the snapshot to disk.
	if _, err = file.Write(b); err != nil {
		return err
	}

	// Ensure that the snapshot has been flushed to disk before continuing.
	return file.Sync()
}

// Reports whether both snapshots cover the same point in the log, which is what
// decides their path on disk.
func (ss *Snapshot) sameAs(other *Snapshot) bool {
	return ss.LastIndex == other.LastIndex && ss.LastTerm == other.LastTerm
}

// remove deletes the snapshot file.
func (ss *Snapshot) remove() error {
	if err := os.Remove(ss.Path); err != nil {
		return err
	}
	return nil
}

// Creates a new Snapshot request.
func newSnapshotRecoveryRequest(leaderName string, snapshot *Snapshot) *SnapshotRecoveryRequest {
	return &SnapshotRecoveryRequest{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
		Peers:      snapshot.Peers,
		State:      snapshot.State,
	}
}

// Encodes the SnapshotRecoveryRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *SnapshotRecoveryRequest) Encode(w io.Writer) (int, error) {

	protoPeers := make([]*protobuf.SnapshotRecoveryRequest_Peer, len(req.Peers))

	for i, peer := range req.Peers {
		protoPeers[i] = &protobuf.SnapshotRecoveryRequest_Peer{
			Name:             peer.Name,
			ConnectionString: peer.ConnectionString,
		}
	}

	pb := &protobuf.SnapshotRecoveryRequest{
		LeaderName: req.LeaderName,
		LastIndex:  req.LastIndex,
		LastTerm:   req.LastTerm,
		Peers:      protoPeers,
		State:      req.State,
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the SnapshotRecoveryRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *SnapshotRecoveryRequest) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.SnapshotRecoveryRequest{}
	if err = proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	req.LeaderName = pb.GetLeaderName()
	req.LastIndex = pb.GetLastIndex()
	req.LastTerm = pb.GetLastTerm()
	req.State = pb.GetState()

	req.Peers = make([]*Peer, len(pb.Peers))

	for i, peer := range pb.Peers {
		req.Peers[i] = &Peer{
			Name:             peer.GetName(),
			ConnectionString: peer.GetConnectionString(),
		}
	}

	return totalBytes, nil
}

// Creates a new Snapshot response.
func newSnapshotRecoveryResponse(term uint64, success bool, commitIndex uint64) *SnapshotRecoveryResponse {
	return &SnapshotRecoveryResponse{
		Term:        term,
		Success:     success,
		CommitIndex: commitIndex,
	}
}

// Encode writes the response to a writer.
// Returns the number of bytes written and any error that occurs.
func (req *SnapshotRecoveryResponse) Encode(w io.Writer) (int, error) {
	pb := &protobuf.SnapshotRecoveryResponse{
		Term:        req.Term,
		Success:     req.Success,
		CommitIndex: req.CommitIndex,
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the SnapshotRecoveryResponse from a buffer.
func (req *SnapshotRecoveryResponse) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.SnapshotRecoveryResponse{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	req.Term = pb.GetTerm()
	req.Success = pb.GetSuccess()
	req.CommitIndex = pb.GetCommitIndex()

	return totalBytes, nil
}

// Creates a new Snapshot request.
func newSnapshotRequest(leaderName string, snapshot *Snapshot) *SnapshotRequest {
	return &SnapshotRequest{
		LeaderName: leaderName,
		LastIndex:  snapshot.LastIndex,
		LastTerm:   snapshot.LastTerm,
	}
}

// Encodes the SnapshotRequest to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (req *SnapshotRequest) Encode(w io.Writer) (int, error) {
	pb := &protobuf.SnapshotRequest{
		LeaderName: req.LeaderName,
		LastIndex:  req.LastIndex,
		LastTerm:   req.LastTerm,
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the SnapshotRequest from a buffer. Returns the number of bytes read and
// any error that occurs.
func (req *SnapshotRequest) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.SnapshotRequest{}

	if err := proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	req.LeaderName = pb.GetLeaderName()
	req.LastIndex = pb.GetLastIndex()
	req.LastTerm = pb.GetLastTerm()

	return totalBytes, nil
}

// Creates a new Snapshot response.
func newSnapshotResponse(success bool) *SnapshotResponse {
	return &SnapshotResponse{
		Success: success,
	}
}

// Encodes the SnapshotResponse to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (resp *SnapshotResponse) Encode(w io.Writer) (int, error) {
	pb := &protobuf.SnapshotResponse{
		Success: resp.Success,
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the SnapshotResponse from a buffer. Returns the number of bytes read and
// any error that occurs.
func (resp *SnapshotResponse) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.SnapshotResponse{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	resp.Success = pb.GetSuccess()

	return totalBytes, nil
}
