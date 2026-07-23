// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// CreateReassignOwnedByCounter returns a counter to increment for the REASSIGN OWNED BY command.
func CreateReassignOwnedByCounter() telemetry.Counter {
	return telemetry.GetCounter("sql.reassign_owned_by")
}
