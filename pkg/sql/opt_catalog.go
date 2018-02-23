// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// optCatalog implements the optbase.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// resolver needs to be set via a call to init before calling other methods.
	resolver SchemaResolver

	// wrappers is a cache of table wrappers that's used to satisfy repeated
	// calls to the FindTable method for the same table.
	wrappers map[*sqlbase.TableDescriptor]*optTable
}

var _ optbase.Catalog = &optCatalog{}

// init allows the optCatalog wrapper to be inlined.
func (oc *optCatalog) init(resolver SchemaResolver) {
	oc.resolver = resolver
}

// FindTable is part of the optbase.Catalog interface.
func (oc *optCatalog) FindTable(ctx context.Context, name *tree.TableName) (optbase.Table, error) {
	desc, err := ResolveExistingObject(ctx, oc.resolver, name, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}

	// Check to see if there's already a wrapper for this table descriptor.
	if oc.wrappers == nil {
		oc.wrappers = make(map[*sqlbase.TableDescriptor]*optTable)
	}
	wrapper, ok := oc.wrappers[desc]
	if !ok {
		wrapper = newOptTable(desc)
		oc.wrappers[desc] = wrapper
	}
	return wrapper, nil
}

// optTable is a wrapper around sqlbase.TableDescriptor that caches index
// wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.TableDescriptor

	// primary is the inlined wrapper for the table's primary index.
	primary optIndex

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int

	// wrappers is a cache of index wrappers that's used to satisfy repeated
	// calls to the SecondaryIndex method for the same index.
	wrappers map[*sqlbase.IndexDescriptor]*optIndex
}

var _ optbase.Table = &optTable{}

func newOptTable(desc *sqlbase.TableDescriptor) *optTable {
	ot := &optTable{}
	ot.init(desc)
	return ot
}

// init allows the optTable wrapper to be inlined.
func (ot *optTable) init(desc *sqlbase.TableDescriptor) {
	ot.desc = desc
	ot.primary.init(ot, &desc.PrimaryIndex)
}

// Name is part of the optbase.Table interface.
func (ot *optTable) TabName() optbase.TableName {
	return optbase.TableName(ot.desc.Name)
}

// ColumnCount is part of the optbase.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// Column is part of the optbase.Table interface.
func (ot *optTable) Column(i int) optbase.Column {
	return &ot.desc.Columns[i]
}

// Primary is part of the optbase.Table interface.
func (ot *optTable) Primary() optbase.Index {
	return &ot.primary
}

// SecondaryCount is part of the optbase.Table interface.
func (ot *optTable) SecondaryCount() int {
	return len(ot.desc.Indexes)
}

// Secondary is part of the optbase.Table interface.
func (ot *optTable) Secondary(i int) optbase.Index {
	desc := &ot.desc.Indexes[i]

	// Check to see if there's already a wrapper for this index descriptor.
	if ot.wrappers == nil {
		ot.wrappers = make(map[*sqlbase.IndexDescriptor]*optIndex)
	}
	wrapper, ok := ot.wrappers[desc]
	if !ok {
		wrapper = newOptIndex(ot, desc)
		ot.wrappers[desc] = wrapper
	}
	return wrapper
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) int {
	if ot.colMap == nil {
		ot.colMap = make(map[sqlbase.ColumnID]int)
		for i := range ot.desc.Columns {
			ot.colMap[ot.desc.Columns[i].ID] = i
		}
	}
	return ot.colMap[colID]
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tbl           *optTable
	desc          *sqlbase.IndexDescriptor
	numCols       int
	numUniqueCols int
}

var _ optbase.Index = &optIndex{}

func newOptIndex(tbl *optTable, desc *sqlbase.IndexDescriptor) *optIndex {
	oi := &optIndex{}
	oi.init(tbl, desc)
	return oi
}

// init allows the optIndex wrapper to be inlined.
func (oi *optIndex) init(tbl *optTable, desc *sqlbase.IndexDescriptor) {
	oi.tbl = tbl
	oi.desc = desc
	oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)

	// If index is not unique, extra key columns are added.
	oi.numUniqueCols = len(desc.ColumnIDs)
	if !desc.Unique {
		oi.numUniqueCols += len(desc.ExtraColumnIDs)
	}
}

// IdxName is part of the optbase.Index interface.
func (oi *optIndex) IdxName() string {
	return oi.desc.Name
}

// ColumnCount is part of the optbase.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// UniqueColumnCount is part of the optbase.Index interface.
func (oi *optIndex) UniqueColumnCount() int {
	return oi.numUniqueCols
}

// Column is part of the optbase.Index interface.
func (oi *optIndex) Column(i int) optbase.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord := oi.tbl.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return optbase.IndexColumn{
			Column:     oi.tbl.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		// Extra columns are in the key only if this is not a unique index.
		ord := oi.tbl.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return optbase.IndexColumn{Column: oi.tbl.Column(ord), Ordinal: ord}
	}

	// Storing columns are never in the key.
	i -= length
	ord := oi.tbl.lookupColumnOrdinal(oi.desc.StoreColumnIDs[i])
	return optbase.IndexColumn{Column: oi.tbl.Column(ord), Ordinal: ord}
}
