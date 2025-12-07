// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"fmt"
	"math"
	"strings"
)

// Cost is the best-effort approximation of the actual cost of executing a
// particular operator tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost struct {
	C float64
	Penalties

	// aux is auxiliary information within a cost that does not affect how the
	// cost is compared to other costs with Less.
	aux struct {
		// fullScanCount is the number of full table or index scans in a
		// sub-plan, up to 65535.
		fullScanCount uint16
		// unboundedReadCount is the number of read expressions (e.g., scans,
		// lookup joins, etc.) in a sub-plan that have no upper-bound
		// cardinality, up to 65535.
		unboundedReadCount uint16
	}
}

// MaxCost is the maximum possible estimated cost. It's used to suppress memo
// group members during testing, by setting their cost so high that any other
// member will have a lower cost.
var MaxCost = Cost{
	C:         math.Inf(+1),
	Penalties: HugeCostPenalty | FullScanPenalty | UnboundedCardinalityPenalty,
}

// Less returns true if this cost is lower than the given cost.
func (c Cost) Less(other Cost) bool {
	if c.Penalties != other.Penalties {
		return c.Penalties < other.Penalties
	}
	// Two plans with the same cost can have slightly different floating point
	// results (e.g. same subcosts being added up in a different order). So we
	// treat plans with very similar cost as equal.
	//
	// We use "units of least precision" for similarity: this is the number of
	// representable floating point numbers in-between the two values. This is
	// better than a fixed epsilon because the allowed error is proportional to
	// the magnitude of the numbers. Because the mantissa is in the low bits, we
	// can just use the bit representations as integers.
	const ulpTolerance = 1000
	return math.Float64bits(c.C)+ulpTolerance <= math.Float64bits(other.C)
}

// Add adds the other cost to this cost.
func (c *Cost) Add(other Cost) {
	c.C += other.C
	c.Penalties |= other.Penalties
	c.aux.fullScanCount = addUint16(c.aux.fullScanCount, other.aux.fullScanCount)
	c.aux.unboundedReadCount = addUint16(c.aux.unboundedReadCount, other.aux.unboundedReadCount)
}

// FullScanCount returns the number of full scans in the cost.
func (c Cost) FullScanCount() uint16 {
	return c.aux.fullScanCount
}

// IncrFullScanCount increments that auxiliary full scan count within c.
func (c *Cost) IncrFullScanCount() {
	c.aux.fullScanCount = addUint16(c.aux.fullScanCount, 1)
}

// UnboundedReadCount returns the number of full scans in the cost.
func (c Cost) UnboundedReadCount() uint16 {
	return c.aux.unboundedReadCount
}

// IncrUnboundedReadCount increments that auxiliary full scan count within c.
func (c *Cost) IncrUnboundedReadCount() {
	c.aux.unboundedReadCount = addUint16(c.aux.unboundedReadCount, 1)
}

// Penalties is an ordered bitmask where each bit indicates a cost penalty. The
// penalties are ordered by precedence, with the highest precedence penalty
// using the highest-order bit. This allows Penalties to be easily compared with
// built-in comparison operators (>, <, =, etc.). For example, Penalties with
// HugeCostPenalty will always be greater than Penalties without.
type Penalties uint8

const (
	// HugeCostPenalty is true if a plan should be avoided at all costs. This is
	// used when the optimizer is forced to use a particular plan, and will
	// error if it cannot be used. It takes precedence over other penalties,
	// since it indicates that a plan is being forced with a hint, and will
	// error if we cannot comply with the hint.
	HugeCostPenalty Penalties = 1 << (7 - iota)

	// FullScanPenalty is true if the cost of a full table or index scan is
	// penalized, indicating that a full scan should only be used if no other
	// plan is possible.
	FullScanPenalty

	// UnboundedCardinalityPenalty is true if the operator or any of its
	// descendants have no guaranteed upperbound on the number of rows that they
	// can produce. See props.AnyCardinality.
	UnboundedCardinalityPenalty

	// NoPenalties represents no penalties.
	NoPenalties Penalties = 0
)

// Summary returns a short string describing the cost. The format is:
//
//	<Cost>:<Penalties>:<aux>
//
// Where:
//
//	<Cost> is the floating point cost value.
//	<Penalties> contains "H", "F", or "U" for HugeCostPenalty, FullScanPenalty,
//	  and UnboundedCardinalityPenalty, respectively.
//	<aux> contains the number of full scans and unbounded reads.
//
// For example, the summary "1.23:HF:5f6u" indicates a cost of 1.23 with the
// HugeCostPenalty and FullScanPenalty penalties, 5 full scans, and 6 unbounded
// reads.
func (c Cost) Summary() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "%.9g:", c.C)
	if c.Penalties&HugeCostPenalty != 0 {
		sb.WriteByte('H')
	}
	if c.Penalties&FullScanPenalty != 0 {
		sb.WriteByte('F')
	}
	if c.Penalties&UnboundedCardinalityPenalty != 0 {
		sb.WriteByte('U')
	}
	_, _ = fmt.Fprintf(&sb, ":%df", c.aux.fullScanCount)
	_, _ = fmt.Fprintf(&sb, "%du", c.aux.unboundedReadCount)
	return sb.String()
}

func addUint16(a, b uint16) uint16 {
	if a > math.MaxUint16-b {
		return math.MaxUint16
	}
	return a + b
}
