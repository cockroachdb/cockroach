// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// MakeSortedColStatKey constructs a unique key representing cols that can be
// used as the key in a map, and also sorts cols as a side-effect.
func MakeSortedColStatKey(cols []descpb.ColumnID) string {
	var colSet intsets.Fast
	for _, c := range cols {
		colSet.Add(int(c))
	}
	// We've already done the work to order the column set, so might as well make
	// cols match that ordering now instead of sorting it later.
	var i int
	colSet.ForEach(func(c int) {
		cols[i] = descpb.ColumnID(c)
		i++
	})
	return colSet.String()
}

// These two are used only by tests and are defined to prevent an import cycle.
var (
	// RandType is randgen.RandType.
	RandType func(rng *rand.Rand) *types.T
	// RandDatum is randgen.RandDatum.
	RandDatum func(rng *rand.Rand, typ *types.T, nullOk bool) tree.Datum
)
