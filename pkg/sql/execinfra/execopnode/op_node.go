// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execopnode

// OpNode is an interface to operator-like structures with children.
type OpNode interface {
	// ChildCount returns the number of children (inputs) of the operator.
	ChildCount(verbose bool) int

	// Child returns the nth child (input) of the operator.
	Child(nth int, verbose bool) OpNode
}

// OpChains describes a forest of OpNodes that represent a single physical plan.
// Each entry in the slice is a root of a separate OpNode tree.
type OpChains []OpNode
