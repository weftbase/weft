//go:build linux

package sqlite

import "os"

// pidNamespace names the PID namespace this process runs in, as the
// kernel shows it ("pid:[4026531836]", the namespace's inode), or ""
// when /proc does not say. Two processes that share a hostname but not
// a PID namespace — two containers over one bind-mounted database
// file — cannot see each other's pids: a pid check across them would
// read a live holder as dead. The lock's host identity carries it, so
// such a holder is another host's, never judged dead (acquire).
func pidNamespace() string {
	ns, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return ""
	}
	return ns
}
