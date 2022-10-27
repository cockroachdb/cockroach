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

// Dimension is a singe dimension of load that a component may track.
type Dimension int

const (
	// Queries refers to the number of queries.
	Queries Dimension = iota
)

const (
	nDimensions = 1
)

// DimensionNames contains a mapping of a load dimension, to a human
// readable string.
var DimensionNames = map[Dimension]string{
	Queries: "queries-per-second",
}
