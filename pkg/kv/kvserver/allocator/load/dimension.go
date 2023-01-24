// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package load

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// Dimension is a singe dimension of load that a component may track.
type Dimension int

const (
	// Queries refers to the number of queries.
	Queries Dimension = iota
	// CPU refers to the cpu time (ns) used in processing.
	CPU

	nDimensionsTyped
	nDimensions = int(nDimensionsTyped)
)

// String returns a human readable string representation of the dimension.
func (d Dimension) String() string {
	switch d {
	case Queries:
		return "queries-per-second"
	case CPU:
		return "cpu-per-second"
	default:
		panic(fmt.Sprintf("cannot name: unknown dimension with ordinal %d", d))
	}
}

// Format returns a formatted string for a value.
func (d Dimension) Format(value float64) string {
	switch d {
	case Queries:
		return fmt.Sprintf("%.1f", value)
	case CPU:
		return string(humanizeutil.Duration(time.Duration(int64(value))))
	default:
		panic(fmt.Sprintf("cannot format value: unknown dimension with ordinal %d", d))
	}
}
