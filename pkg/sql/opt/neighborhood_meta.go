// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import "github.com/cockroachdb/cockroach/pkg/sql/opt/cluster"

// NeighborhoodID uniquely identifies the usage of a Cockroach neighborhood
// within the scope of a query. NeighborhoodID 0 is reserved to mean "unknown
// neighborhood".
type NeighborhoodID int32

// index returns the index of the neighborhood in Metadata.neighborhoods. It's
// biased by 1, so that NeighborhoodID 0 can be be reserved to mean "unknown
// neighborhood".
func (n NeighborhoodID) index() int {
	return int(n - 1)
}

// NeighborhoodMeta stores information about one of the neighborhoods stored in
// the metadata.
type NeighborhoodMeta struct {
	// MetaID is the identifier for this neighborhood that is unique within the
	// query metadata.
	MetaID NeighborhoodID

	// Neighborhood is a reference to the neighborhood in the cluster.
	Neighborhood cluster.Neighborhood
}

// makeNeighborhoodID constructs a new NeighborhoodID.
func makeNeighborhoodID(index int) NeighborhoodID {
	// Bias the neighborhood index by 1.
	return NeighborhoodID(index + 1)
}
