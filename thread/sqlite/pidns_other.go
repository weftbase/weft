//go:build !linux

package sqlite

// pidNamespace has no answer off Linux: the host identity is the
// hostname alone, as before.
func pidNamespace() string { return "" }
