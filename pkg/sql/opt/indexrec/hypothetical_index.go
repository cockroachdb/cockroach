// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexrec

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// hypotheticalIndex is a dummy implementation of cat.Index, used with
// hypotheticalTable for index recommendations.
type hypotheticalIndex struct {
	tab          *hypotheticalTable
	name         tree.Name
	cols         []cat.IndexColumn
	indexOrdinal int
	zone         *zonepb.ZoneConfig
}

var _ cat.Index = &hypotheticalIndex{}

// ID is part of the cat.Index interface.
func (hi *hypotheticalIndex) ID() cat.StableID {
	return cat.StableID(hi.indexOrdinal)
}

// Name is part of the cat.Index interface.
func (hi *hypotheticalIndex) Name() tree.Name {
	return hi.name
}

// IsUnique is part of the cat.Index interface.
func (hi *hypotheticalIndex) IsUnique() bool {
	return false
}

// IsInverted is part of the cat.Index interface.
func (hi *hypotheticalIndex) IsInverted() bool {
	return false
}

// ColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ColumnCount() int {
	return len(hi.cols)
}

// KeyColumnCount is part of the cat.Index interface.
// TODO(neha): Here we assume that hypothetical indexes are not unique, although
//   depending on their columns, they could be. We should account for this, which
//   would affect KeyColumnCount and LaxKeyColumnCount.
func (hi *hypotheticalIndex) KeyColumnCount() int {
	// We assume that the number of key columns for a hypothetical index is one
	// greater than the column count since hypothetical indexes are never unique.
	// The final column is a fake index column that we pretend exists to guarantee
	// uniqueness. See the implementation of hypotheticalIndex.Column().
	return len(hi.cols) + 1
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) LaxKeyColumnCount() int {
	// Hypothetical indexes are never unique, so their lax key is the same as
	// their key.
	return hi.KeyColumnCount()
}

// NonInvertedPrefixColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) NonInvertedPrefixColumnCount() int {
	panic(errors.AssertionFailedf("hypothetical indexes are not inverted"))
}

// Column is part of the cat.Index interface.
func (hi *hypotheticalIndex) Column(i int) cat.IndexColumn {
	if i == len(hi.cols) {
		// The special bogus PK column goes at the end of the index columns. It
		// has ID 0.
		return cat.IndexColumn{Column: hi.tab.Column(0)}
	}
	return hi.cols[i]
}

// InvertedColumn is part of the cat.Index interface.
func (hi *hypotheticalIndex) InvertedColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("hypothetical indexes are not inverted"))
}

// Predicate is part of the cat.Index interface.
func (hi *hypotheticalIndex) Predicate() (string, bool) {
	return "", false
}

// Zone is part of the cat.Index interface.
func (hi *hypotheticalIndex) Zone() cat.Zone {
	return hi.zone
}

// Span is part of the cat.Index interface.
func (hi *hypotheticalIndex) Span() roachpb.Span {
	panic(errors.AssertionFailedf("no span"))
}

// Table is part of the cat.Index interface.
func (hi *hypotheticalIndex) Table() cat.Table {
	return hi.tab
}

// Ordinal is part of the cat.Index interface.
func (hi *hypotheticalIndex) Ordinal() int {
	return hi.indexOrdinal
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ImplicitPartitioningColumnCount() int {
	return 0
}

// InterleaveAncestorCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) InterleaveAncestorCount() int {
	return 0
}

// InterleaveAncestor is part of the cat.Index interface.
func (hi *hypotheticalIndex) InterleaveAncestor(i int) (table, index cat.StableID, numKeyCols int) {
	panic(errors.AssertionFailedf("no interleavings"))
}

// InterleavedByCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) InterleavedByCount() int {
	return 0
}

// InterleavedBy is part of the cat.Index interface.
func (hi *hypotheticalIndex) InterleavedBy(i int) (table, index cat.StableID) {
	panic(errors.AssertionFailedf("no interleavings"))
}

// GeoConfig is part of the cat.Index interface.
func (hi *hypotheticalIndex) GeoConfig() *geoindex.Config {
	return nil
}

// Version is part of the cat.Index interface.
func (hi *hypotheticalIndex) Version() descpb.IndexDescriptorVersion {
	return 0
}

// PartitionCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) PartitionCount() int {
	return 0
}

// Partition is part of the cat.Index interface.
func (hi *hypotheticalIndex) Partition(i int) cat.Partition {
	return nil
}
