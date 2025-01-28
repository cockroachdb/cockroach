// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import "math"

// Cost is the best-effort approximation of the actual cost of executing a
// particular operator tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost struct {
	C     float64
	Flags CostFlags
}

// MaxCost is the maximum possible estimated cost. It's used to suppress memo
// group members during testing, by setting their cost so high that any other
// member will have a lower cost.
var MaxCost = Cost{
	C: math.Inf(+1),
	Flags: CostFlags{
		FullScanPenalty:      true,
		HugeCostPenalty:      true,
		UnboundedCardinality: true,
	},
}

// Less returns true if this cost is lower than the given cost.
func (c Cost) Less(other Cost) bool {
	if c.Flags != other.Flags {
		return c.Flags.Less(other.Flags)
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
	c.Flags.Add(other.Flags)
}

// CostFlags contains flags that penalize the cost of an operator.
type CostFlags struct {
	// FullScanPenalty is true if the cost of a full table or index scan is
	// penalized, indicating that a full scan should only be used if no other plan
	// is possible.
	FullScanPenalty bool
	// HugeCostPenalty is true if a plan should be avoided at all costs. This is
	// used when the optimizer is forced to use a particular plan, and will error
	// if it cannot be used.
	HugeCostPenalty bool
	// UnboundedCardinality is true if the operator or any of its descendants
	// have no guaranteed upperbound on the number of rows that they can
	// produce. See props.AnyCardinality.
	UnboundedCardinality bool
}

// Less returns true if these flags indicate a lower penalty than the other
// CostFlags.
func (c CostFlags) Less(other CostFlags) bool {
	// HugeCostPenalty takes precedence over other penalties, since it indicates
	// that a plan is being forced with a hint, and will error if we cannot comply
	// with the hint.
	if c.HugeCostPenalty != other.HugeCostPenalty {
		return !c.HugeCostPenalty
	}
	if c.FullScanPenalty != other.FullScanPenalty {
		return !c.FullScanPenalty
	}
	if c.UnboundedCardinality != other.UnboundedCardinality {
		return !c.UnboundedCardinality
	}
	return false
}

// Add adds the other flags to these flags.
func (c *CostFlags) Add(other CostFlags) {
	c.FullScanPenalty = c.FullScanPenalty || other.FullScanPenalty
	c.HugeCostPenalty = c.HugeCostPenalty || other.HugeCostPenalty
	c.UnboundedCardinality = c.UnboundedCardinality || other.UnboundedCardinality
}

// Empty returns true if these flags are empty.
func (c CostFlags) Empty() bool {
	return !c.FullScanPenalty && !c.HugeCostPenalty && !c.UnboundedCardinality
}
