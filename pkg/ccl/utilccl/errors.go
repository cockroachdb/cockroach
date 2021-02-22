package utilccl

import "strings"

// IsDistSQLRetryableError returns true if the supplied error, or any of its parent
// causes is an rpc error.
// This is an unfortunate implementation that should be looking for a more
// specific error.
func IsDistSQLRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// TODO(knz): this is a bad implementation. Make it go away
	// by avoiding string comparisons.

	errStr := err.Error()
	if strings.Contains(errStr, `rpc error`) {
		// When a crdb node dies, any DistSQL flows with processors scheduled on
		// it get an error with "rpc error" in the message from the call to
		// `(*DistSQLPlanner).Run`.
		return true
	}
	return false
}
