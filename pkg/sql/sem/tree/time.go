// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"time"

	"github.com/cockroachdb/errors"
)

// TimeFamilyPrecisionToRoundDuration takes in a type's precision, and returns the
// duration to use to pass into time.Truncate to truncate to that duration.
// Panics if the precision is not supported.
func TimeFamilyPrecisionToRoundDuration(precision int32) time.Duration {
	switch precision {
	case 0:
		return time.Second
	case 1:
		return time.Millisecond * 100
	case 2:
		return time.Millisecond * 10
	case 3:
		return time.Millisecond
	case 4:
		return time.Microsecond * 100
	case 5:
		return time.Microsecond * 10
	case 6:
		return time.Microsecond
	}
	panic(errors.Newf("unsupported precision: %d", precision))
}
