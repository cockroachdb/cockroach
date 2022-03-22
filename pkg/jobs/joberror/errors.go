// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package joberror

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/errors"
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

// isBreakerOpenError returns true if err is a circuit.ErrBreakerOpen.
func isBreakerOpenError(err error) bool {
	return errors.Is(err, circuit.ErrBreakerOpen)
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
		!kvcoord.IsSendError(err) &&
		!isBreakerOpenError(err)
}
