// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This module holds structures used to build insert fast path
// foreign key and uniqueness checks.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TableColumnOrdinal is the 0-based ordinal index of a cat.Table column.
// It is used when operations involve a table directly (e.g. scans, index/lookup
// joins, mutations).
type TableColumnOrdinal int32

// MkErrFn is a function that generates an error which includes values from a
// relevant row.
type MkErrFn func(tree.Datums) error

// InsertFastPathFKUniqCheck contains information about a foreign key or
// uniqueness check to be performed by the insert fast-path (see
// ConstructInsertFastPath). It identifies the index into which we can perform
// the lookup. This is a structure used during exploration which mirrors the
// structure of the same name in the exec package.
type InsertFastPathFKUniqCheck struct {
	ReferencedTable cat.Table
	ReferencedIndex cat.Index

	// InsertCols contains the FK columns from the origin table or the index key
	// columns for a uniqueness check, in the order of the ReferencedIndex
	// columns. For each, the value in the array indicates the index of the column
	// in the input table.
	InsertCols []TableColumnOrdinal

	MatchMethod tree.CompositeKeyMatchMethod

	// DatumsFromConstraint contains constant values from the WHERE clause
	// constraint which are part of the index key to look up. The number of
	// entries corresponds with the number of KV lookups. For example, when built
	// from analyzing a locality-optimized operation accessing 1 local region and
	// 2 remote regions, the resulting DatumsFromConstraint would have 3 entries.
	DatumsFromConstraint []tree.Datums

	// MkErr is called when a violation is detected (i.e. the index has no entries
	// for a given inserted row). The values passed correspond to InsertCols
	// above.
	MkErr MkErrFn
}
