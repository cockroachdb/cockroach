// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package split

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/redact"
)

// SplitObjective is a type that specifies a load based splitting objective.
type SplitObjective int

const (
	// SplitQPS will track and split QPS (queries-per-second) over a range.
	SplitQPS SplitObjective = iota
	// SplitCPU will track and split CPU (cpu-per-second) over a range.
	SplitCPU
)

// String returns a human-readable string representation of the dimension.
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

// SafeValue implements the redact.SafeValue interface.
func (SplitObjective) SafeValue() {}

// Format returns a formatted string for a value.
func (d SplitObjective) Format(value float64) redact.SafeString {
	switch d {
	case SplitQPS:
		return redact.SafeString(fmt.Sprintf("%.1f", value))
	case SplitCPU:
		return humanizeutil.Duration(time.Duration(int64(value)))
	default:
		panic(fmt.Sprintf("cannot format value: unknown objective with ordinal %d", d))
	}
}
