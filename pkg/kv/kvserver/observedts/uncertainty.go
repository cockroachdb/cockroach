// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package observedts

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// IsUncertain deterines whether a value with the provided timestamp is
// uncertain to a reader with a read_timestamp below the value's and the
// specified (max_timestamp, observed_max_timestamp) pair.
//
// The (max_timestamp, observed_max_timestamp) pair represent the reader's
// uncertainty interval. The max_timestamp is the reader's initial upper bound
// on the interval and the observed_max_timestamp is an optional tighter bound
// established through the use of observed timestamps. Both values are provided
// because the observed_max_timestamp does not apply to synthetic timestamps.
//
// NOTE: if observedMaxTs is not empty, it must be <= maxTs.
func IsUncertain(maxTs, observedMaxTs, valueTs hlc.Timestamp) bool {
	if !observedMaxTs.IsEmpty() && !valueTs.IsFlagSet(hlc.TimestampFlag_SYNTHETIC) {
		return valueTs.LessEq(observedMaxTs)
	}
	return valueTs.LessEq(maxTs)
}
