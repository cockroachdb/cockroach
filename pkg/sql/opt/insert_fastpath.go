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

// InsertFastPathFKUniqCheck contains information about a foreign key or
// uniqueness check to be performed by the insert fast-path (see
// ConstructInsertFastPath). It identifies the index into which we can perform
// the lookup. This is a structure used during exploration which contains
// information we can use to build the structure of the same name in the exec
// package.
type InsertFastPathFKUniqCheck struct {
	ReferencedTableID      TableID
	ReferencedIndexOrdinal cat.IndexOrdinal

	// InsertCols contains the FK columns from the origin table or the index key
	// columns for a uniqueness check, in the order of the ReferencedIndexOrdinal
	// columns. For each, the value in the array indicates the index of the column
	// in the input table.
	InsertCols ColList

	// DatumsFromConstraint contains constant values from the WHERE clause
	// constraint which are part of the index key to look up. The number of
	// entries corresponds with the number of KV lookups. For example, when built
	// from analyzing a locality-optimized operation accessing 1 local region and
	// 2 remote regions, the resulting DatumsFromConstraint would have 3 entries.
	DatumsFromConstraint []tree.Datums
}
