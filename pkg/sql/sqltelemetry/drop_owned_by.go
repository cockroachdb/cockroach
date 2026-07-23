// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// CreateDropOwnedByCounter returns a counter to increment for the DROP OWNED BY command.
func CreateDropOwnedByCounter() telemetry.Counter {
	return telemetry.GetCounter("sql.drop_owned_by")
}
