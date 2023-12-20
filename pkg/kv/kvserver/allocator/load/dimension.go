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
	"github.com/cockroachdb/redact"
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

// SafeValue implements the redact.SafeValue interface.
func (d Dimension) SafeValue() {}

// format returns a formatted string for a value.
func (d Dimension) format(value float64) redact.SafeString {
	switch d {
	case Queries:
		return redact.SafeString(fmt.Sprintf("%.1f", value))
	case CPU:
		return humanizeutil.Duration(time.Duration(int64(value)))
	default:
		panic(fmt.Sprintf("cannot format value: unknown dimension with ordinal %d", d))
	}
}
