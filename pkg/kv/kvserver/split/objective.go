// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package split

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// SplitObjective is a type that specifies a load based splitting objective.
type SplitObjective int

const (
	// SplitQPS will track and split QPS (queries-per-second) over a range.
	SplitQPS SplitObjective = iota
	// SplitCPU will track and split CPU (cpu-per-second) over a range.
	SplitCPU
)

// String returns a human readable string representation of the dimension.
func (d SplitObjective) String() string {
	switch d {
	case SplitQPS:
		return "qps"
	case SplitCPU:
		return "cpu"
	default:
		panic(fmt.Sprintf("cannot name: unknown objective with ordinal %d", d))
	}
}

// Format returns a formatted string for a value.
func (d SplitObjective) Format(value float64) string {
	switch d {
	case SplitQPS:
		return fmt.Sprintf("%.1f", value)
	case SplitCPU:
		return string(humanizeutil.Duration(time.Duration(int64(value))))
	default:
		panic(fmt.Sprintf("cannot format value: unknown objective with ordinal %d", d))
	}
}
