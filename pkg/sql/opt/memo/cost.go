// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import "math"

type CostFlags struct {
	FullScanPenalty bool
}

func (c *CostFlags) Add(other CostFlags) CostFlags {
	c.FullScanPenalty = c.FullScanPenalty || other.FullScanPenalty
	return *c
}

func (c CostFlags) Less(other CostFlags) bool {
	if c.FullScanPenalty != other.FullScanPenalty {
		return !c.FullScanPenalty
	}
	return false
}

func (c CostFlags) Empty() bool {
	return c == CostFlags{}
}

// Cost is the best-effort approximation of the actual cost of executing a
// particular operator tree.
// TODO: Need more details about what one "unit" of cost means.
type Cost struct {
	Cost  float64
	Flags CostFlags
}

// MaxCost is the maximum possible estimated cost. It's used to suppress memo
// group members during testing, by setting their cost so high that any other
// member will have a lower cost.
var MaxCost = Cost{
	Cost:  math.Inf(+1),
	Flags: CostFlags{FullScanPenalty: true},
}

// Less returns true if this cost is lower than the given cost.
func (c Cost) Less(other Cost) bool {
	//if c.Flags != other.Flags {
	//	return c.Flags.Less(other.Flags)
	//}
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
	return math.Float64bits(c.Cost)+ulpTolerance <= math.Float64bits(other.Cost)
}

// Add adds the other cost to this cost and returns the result.
func (c *Cost) Add(other Cost) Cost {
	c.Cost += other.Cost
	c.Flags.Add(other.Flags)
	return *c
}
