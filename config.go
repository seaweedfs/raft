package raft

type Config struct {
	CommitIndex uint64  `json:"commitIndex"`
	Peers       []*Peer `json:"peers"`
	CurrentTerm uint64  `json:"currentTerm,omitempty"`
	VotedFor    string  `json:"votedFor,omitempty"`
}
