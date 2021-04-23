// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
)

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

// IsPermanentBulkJobError returns true if the error results in a permanent
// failure of a bulk job (IMPORT, BACKUP, RESTORE). This function is a allowlist
// instead of a blocklist: only known safe errors are confirmed to not be
// permanent errors. Anything unknown is assumed to be permanent.
func IsPermanentBulkJobError(err error) bool {
	if err == nil {
		return false
	}

	return !IsDistSQLRetryableError(err) &&
		!grpcutil.IsClosedConnection(err) &&
		!flowinfra.IsNoInboundStreamConnectionError(err) &&
		!kvcoord.IsSendError(err)
}
