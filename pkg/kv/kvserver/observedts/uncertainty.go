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

// IsUncertain determines whether a value with the provided timestamp is
// uncertain to a reader with a read_timestamp below the value's and the
// specified (local_uncertainty_limit, global_uncertainty_limit) pair.
//
// The (local_uncertainty_limit, global_uncertainty_limit) pair represent the
// reader's uncertainty interval. The global_uncertainty_limit is the reader's
// initial upper bound on the interval and the local_uncertainty_limit is an
// optional tighter bound established through the use of observed timestamps.
// Both values are provided because the local_uncertainty_limit does not apply
// to synthetic timestamps.
//
// NOTE: if localUncertaintyLimit is set, it must be <= globalUncertaintyLimit.
func IsUncertain(localUncertaintyLimit, globalUncertaintyLimit, valueTs hlc.Timestamp) bool {
	if !localUncertaintyLimit.IsEmpty() && !valueTs.Synthetic {
		return valueTs.LessEq(localUncertaintyLimit)
	}
	return valueTs.LessEq(globalUncertaintyLimit)
}
