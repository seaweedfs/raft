//go:build !windows

package raft

import "os"

// syncDir fsyncs the directory so that a preceding rename is durable.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
