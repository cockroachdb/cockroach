// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// CreateDropOwnedByCounter returns a counter to increment for the DROP OWNED BY command.
func CreateDropOwnedByCounter() telemetry.Counter {
	return telemetry.GetCounter("sql.drop_owned_by")
}
