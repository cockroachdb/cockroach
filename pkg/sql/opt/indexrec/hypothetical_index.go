// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package indexrec

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// hypotheticalIndex is a dummy implementation of cat.Index, used with
// HypotheticalTable for index recommendations.
type hypotheticalIndex struct {
	tab  *HypotheticalTable
	name tree.Name

	// cols stores the index columns, in order.
	cols []cat.IndexColumn

	// indexOrdinal stores the index's ordinal position on the hypothetical table.
	indexOrdinal int

	// zone stores the table's zone.
	zone cat.Zone

	// suffixKeyColsOrdList contains all implicit column ordinals. Implicit
	// columns are columns that are in the table's primary key but are not already
	// in the index columns.
	suffixKeyCols []cat.IndexColumn

	// storedCols contains all the table's column ordinals that are not key
	// columns (neither index columns nor suffix key columns).
	storedCols []cat.IndexColumn

	// typ indicates the type of the index - forward, inverted, or vector.
	typ idxtype.T
}

var _ cat.Index = &hypotheticalIndex{}

func (hi *hypotheticalIndex) init(
	tab *HypotheticalTable,
	name tree.Name,
	cols []cat.IndexColumn,
	indexOrd int,
	typ idxtype.T,
	zone cat.Zone,
) {
	hi.tab = tab
	hi.name = name
	hi.cols = cols
	hi.indexOrdinal = indexOrd
	hi.typ = typ
	hi.zone = zone

	// Build an index column ordinal set.
	var colsOrdSet intsets.Fast
	for _, col := range hi.cols {
		colsOrdSet.Add(col.Ordinal())
	}

	// Build the suffix key column list.
	pkColOrds := hi.tab.primaryKeyColsOrdSet
	hi.suffixKeyCols = make([]cat.IndexColumn, 0, pkColOrds.Len())
	for i, ok := pkColOrds.Next(0); ok; i, ok = pkColOrds.Next(i + 1) {
		if !colsOrdSet.Contains(i) {
			hi.suffixKeyCols = append(hi.suffixKeyCols, cat.IndexColumn{Column: hi.tab.Column(i)})
		}
	}

	// Build the stored cols for forward indexes only.
	if typ.SupportsStoring() {
		keyColsOrds := colsOrdSet.Union(pkColOrds)
		hi.storedCols = make([]cat.IndexColumn, 0, tab.ColumnCount())
		for i, n := 0, tab.ColumnCount(); i < n; i++ {
			if !keyColsOrds.Contains(i) {
				hi.storedCols = append(hi.storedCols, cat.IndexColumn{Column: hi.tab.Column(i)})
			}
		}
	}
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

// Type is part of the cat.Index interface.
func (hi *hypotheticalIndex) Type() idxtype.T {
	return hi.typ
}

// GetInvisibility is part of the cat.Index interface.
func (hi *hypotheticalIndex) GetInvisibility() float64 {
	// A hypotheticalIndex should not be invisible because there is no motivation
	// to recommend a not visible index.
	return 0.0
}

// ColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ColumnCount() int {
	return len(hi.cols) + len(hi.suffixKeyCols) + len(hi.storedCols)
}

// ExplicitColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ExplicitColumnCount() int {
	return len(hi.cols)
}

// KeyColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) KeyColumnCount() int {
	// Since hypothetical indexes are not unique, we build a key by including all
	// the index key columns and then appending any primary key columns that are
	// not already included.
	return len(hi.cols) + len(hi.suffixKeyCols)
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) LaxKeyColumnCount() int {
	// Hypothetical indexes are never unique, so their lax key is the same as
	// their regular key.
	return hi.KeyColumnCount()
}

// PrefixColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) PrefixColumnCount() int {
	if !hi.Type().AllowsPrefixColumns() {
		panic(errors.AssertionFailedf("only inverted and vector indexes have prefix columns"))
	}
	return len(hi.cols) - 1
}

// Column is part of the cat.Index interface.
func (hi *hypotheticalIndex) Column(i int) cat.IndexColumn {
	if i < len(hi.cols) {
		// The column is an index column.
		return hi.cols[i]
	}
	numKeyCols := len(hi.cols) + len(hi.suffixKeyCols)
	if i < numKeyCols {
		// The column is an implicit key column.
		return hi.suffixKeyCols[i-len(hi.cols)]
	}
	// The column is a stored column.
	return hi.storedCols[i-numKeyCols]
}

// InvertedColumn is part of the cat.Index interface.
func (hi *hypotheticalIndex) InvertedColumn() cat.IndexColumn {
	if hi.Type() != idxtype.INVERTED {
		panic(errors.AssertionFailedf("non-inverted indexes do not have inverted columns"))
	}
	return hi.cols[len(hi.cols)-1]
}

// VectorColumn is part of the cat.Index interface.
func (hi *hypotheticalIndex) VectorColumn() cat.IndexColumn {
	panic(errors.AssertionFailedf("hypothetical indexes do not have vector columns"))
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
func (hi *hypotheticalIndex) Ordinal() cat.IndexOrdinal {
	return hi.indexOrdinal
}

// ImplicitColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ImplicitColumnCount() int {
	return 0
}

// ImplicitPartitioningColumnCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) ImplicitPartitioningColumnCount() int {
	return 0
}

// GeoConfig is part of the cat.Index interface.
func (hi *hypotheticalIndex) GeoConfig() geopb.Config {
	if hi.Type() == idxtype.INVERTED {
		srcCol := hi.tab.Column(hi.InvertedColumn().InvertedSourceColumnOrdinal())
		switch srcCol.DatumType().Family() {
		case types.GeometryFamily:
			return *geoindex.DefaultGeometryIndexConfig()
		case types.GeographyFamily:
			return *geoindex.DefaultGeographyIndexConfig()
		}
	}
	return geopb.Config{}
}

// Version is part of the cat.Index interface.
func (hi *hypotheticalIndex) Version() descpb.IndexDescriptorVersion {
	return descpb.LatestIndexDescriptorVersion
}

// PartitionCount is part of the cat.Index interface.
func (hi *hypotheticalIndex) PartitionCount() int {
	return 0
}

// Partition is part of the cat.Index interface.
func (hi *hypotheticalIndex) Partition(i int) cat.Partition {
	return nil
}

// hasSameExplicitCols checks whether the given existing index has identical
// explicit columns as the hypothetical index. To be identical, they need to
// have the exact same list, length, and order. If the index is inverted, it
// also checks to make sure that the inverted column has the same source column.
// If so, it returns true.
func (hi *hypotheticalIndex) hasSameExplicitCols(existingIndex cat.Index) bool {
	indexCols := hi.cols
	if existingIndex.ExplicitColumnCount() != len(indexCols) {
		return false
	}
	return hi.hasPrefixOfExplicitCols(existingIndex)
}

// hasPrefixOfExplicitCols returns true if the explicit columns in the
// hypothetical index are a prefix of the explicit columns in the given existing
// index.
func (hi *hypotheticalIndex) hasPrefixOfExplicitCols(existingIndex cat.Index) bool {
	indexCols := hi.cols
	if existingIndex.ExplicitColumnCount() < len(indexCols) {
		return false
	}
	for j, m := 0, len(indexCols); j < m; j++ {
		// Compare every existingIndex columns with indexCols.
		existingIndexCol := existingIndex.Column(j)
		indexCol := indexCols[j]

		if indexCol.Kind() == cat.Inverted && existingIndexCol.Kind() == cat.Inverted {
			// If the columns are inverted, compare their source columns.
			if existingIndexCol.InvertedSourceColumnOrdinal() != indexCol.InvertedSourceColumnOrdinal() {
				return false
			}
		} else if existingIndexCol != indexCol {
			// Otherwise, compare every column directly.
			return false
		}
	}
	return true
}
