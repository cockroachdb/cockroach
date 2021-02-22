// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	// When a crdb node dies, any DistSQL flows with processors scheduled on
	// it get an error with "rpc error" in the message from the call to
	// `(*DistSQLPlanner).Run`.
	return strings.Contains(errStr, `rpc error`)
}
