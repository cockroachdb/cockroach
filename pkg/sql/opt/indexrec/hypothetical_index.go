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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// hypotheticalIndex is a dummy implementation of cat.Index, used with
// hypotheticalTable for index recommendations.
type hypotheticalIndex struct {
	tab  *hypotheticalTable
	name tree.Name

	// cols stores the index columns, in order.
	cols []cat.IndexColumn

	// indexOrdinal stores the index's ordinal position on the hypothetical table.
	indexOrdinal int

	// zone stores the table's zone.
	zone *zonepb.ZoneConfig

	// suffixKeyColsOrdList contains all implicit column ordinals. Implicit
	// columns are columns that are in the table's primary key but are not already
	// in the index columns.
	suffixKeyColsOrdList []int
}

var _ cat.Index = &hypotheticalIndex{}

func (hi *hypotheticalIndex) init(
	tab *hypotheticalTable,
	name tree.Name,
	cols []cat.IndexColumn,
	indexOrd int,
	zone *zonepb.ZoneConfig,
) {
	hi.tab = tab
	hi.name = name
	hi.cols = cols
	hi.indexOrdinal = indexOrd
	hi.zone = zone

	// Build an index column ordinal set.
	var colsOrdSet util.FastIntSet
	for _, col := range hi.cols {
		colsOrdSet.Add(col.Ordinal())
	}

	// Build the suffix key column list.
	suffixKeyColsSet := hi.tab.primaryKeyColsOrdSet.Difference(colsOrdSet)
	hi.suffixKeyColsOrdList = suffixKeyColsSet.Ordered()
}

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
	// A hypotheticalIndex is not unique because there is no motivation to enforce
	// a unique constraint.
	return false
}

// IsInverted is part of the cat.Index interface.
func (hi *hypotheticalIndex) IsInverted() bool {
	// Hypothetical indexes are not inverted.
	// TODO(nehageorge): Add support for inverted index recommendations.
	return false
}

// ColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ColumnCount() int {
	// For now, this is the same as the KeyColumnCount, because there are no
	// stored columns.
	return len(hi.cols) + len(hi.suffixKeyColsOrdList)
}

// KeyColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) KeyColumnCount() int {
	// Since hypothetical indexes are not unique, we build a key by including all
	// the index key columns and then appending any primary key columns that are
	// not already included.
	return len(hi.cols) + len(hi.suffixKeyColsOrdList)
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) LaxKeyColumnCount() int {
	// Hypothetical indexes are never unique, so their lax key is the same as
	// their regular key.
	return hi.KeyColumnCount()
}

// NonInvertedPrefixColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) NonInvertedPrefixColumnCount() int {
	panic(errors.AssertionFailedf("hypothetical indexes are not inverted"))
}

// Column is part of the cat.Index interface.
func (hi *hypotheticalIndex) Column(i int) cat.IndexColumn {
	if i >= len(hi.cols) {
		// The column is an added suffix primary key column. Construct the
		// corresponding cat.Column.
		suffixColOrd := hi.suffixKeyColsOrdList[i-len(hi.cols)]
		return cat.IndexColumn{Column: hi.tab.Column(suffixColOrd)}
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

// GeoConfig is part of the cat.Index interface.
func (hi *hypotheticalIndex) GeoConfig() *geoindex.Config {
	return nil
}

// Version is part of the cat.Index interface.
func (hi *hypotheticalIndex) Version() descpb.IndexDescriptorVersion {
	// Return the latest version for non-primary indexes, since hypothetical
	// indexes are not primary indexes.
	return tabledesc.LatestNonPrimaryIndexDescriptorVersion
}

// PartitionCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) PartitionCount() int {
	return 0
}

// Partition is part of the cat.Index interface.
func (hi *hypotheticalIndex) Partition(i int) cat.Partition {
	return nil
}
