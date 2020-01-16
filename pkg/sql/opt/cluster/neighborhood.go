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

import "github.com/cockroachdb/cockroach/pkg/sql/opt/cat"

type NeighborhoodID int32

// Neighborhood is an interface to a region or set of nodes in a CockroachDB
// cluster, exposing only the information needed by the query optimizer.
type Neighborhood interface {
	// ID returns the stable identifier for this neighborhood.
	ID() NeighborhoodID

	// SatisfiesConstraints checks whether a neighborhood satisfies all of the
	// given constraints.
	//
	// - If a constraint is of the REQUIRED type, satisfying it means at least
	//   one node in the neighborhood should match the constraint's spec.
	// - If a constraint is of the PROHIBITED type, satisfying it means at least
	//   one node in the neighborhood should not match the constraint's spec.
	SatisfiesConstraints(constraints cat.ConstraintSet) bool
}
