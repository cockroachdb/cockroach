// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// Info is an interface exposing only the information about a CockroachDB
// cluster that is needed by the query optimizer.
type Info interface {
	// NeighborhoodCount returns the number of neighborhoods in this CockroachDB
	// cluster. Neighborhood information is used by the optimizer to estimate the
	// cost of operations that may span multiple neighborhoods.
	NeighborhoodCount() int

	// Neighborhood returns information about the ith neighborhood in this
	// CockroachDB cluster, where i < NeighborhoodCount.
	Neighborhood(i int) Neighborhood

	// NeighborhoodFromNode returns the neighborhood that contains the given
	// node.
	NeighborhoodFromNode(id roachpb.NodeID) NeighborhoodID
}
