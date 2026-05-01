//go:build windows

package raft

// syncDir is a no-op on Windows. FlushFileBuffers requires a handle opened
// with GENERIC_WRITE, but os.Open opens directories read-only, so calling
// Sync on the resulting handle returns ERROR_ACCESS_DENIED. Windows also
// has no portable equivalent of fsync(dir_fd): NTFS journals metadata
// operations (including renames), so directory entries reach disk via the
// filesystem's own ordering guarantees rather than an explicit dir flush.
func syncDir(dir string) error {
	return nil
}
