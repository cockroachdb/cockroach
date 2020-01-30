// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// IndexPartition describes a single index partition. Each partition consists
// of a subset of index rows that is disjoint from the subsets in all other
// partitions. Each subset is defined by a range of keys that form a prefix
// on the index columns.
type IndexPartition struct {
	// From is the inclusive lower bound of this partition. The values correspond
	// to a prefix of the index columns.
	From tree.Datums

	// To is the exclusive upper bound of this partition. The values correspond
	// to a prefix of the index columns.
	To tree.Datums

	// Zone is an interface to the zone configuration which constrains placement
	// of this index partition.
	Zone Zone
}

// FormatPartition nicely formats a catalog IndexPartition using a treeprinter
// for debugging and testing.
func FormatPartition(r IndexPartition, tp treeprinter.Node) {
	child := tp.Childf("from:%s to:%s", r.From, r.To)
	FormatZone(r.Zone, child)
}
