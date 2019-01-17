// Copyright 2019 The Cockroach Authors.
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

package row

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// baseFKHelper is an auxiliary struct that facilitates FK existence
// checks for one FK constraint.
//
// TODO(knz): the fact that this captures the txn is problematic. The
// txn should be passed as argument.
type baseFKHelper struct {
	// txn is the current KV transaction.
	txn *client.Txn

	// dir indicates the direction of the check.
	//
	// Note that the helper is used both for forward checks (when
	// INSERTing data in a *referencing* table) and backward checks
	// (when DELETEing data in a *referenced* table). UPDATE uses
	// helpers in both directions.
	//
	// Because it serves both directions, the explanations below
	// avoid using the words "referencing" and "referenced". Instead
	// it uses "searched" for the table/index where the existence
	// is tested; and "target" for the table/index being mutated.
	//
	dir FKCheck

	// rf is the row fetcher used to look up rows in the searched table.
	rf Fetcher

	// searchIdx is the index used for lookups over the searched table.
	searchIdx *sqlbase.IndexDescriptor

	// prefixLen is the number of columns being looked up. In the common
	// case it matches the number of columns in searchIdx, however it is
	// possible to run a lookup check for a prefix of the columns in
	// searchIdx, eg. `(a,b) REFERENCES t(x,y)` with an index on
	// `(x,y,z)`.
	prefixLen int

	// Pre-computed KV key prefix for searchIdx.
	searchPrefix []byte

	// ids maps column IDs in index searchIdx to positions of the `row`
	// array provided to each FK existence check. This tells the checker
	// where to find the values in the row for each column of the
	// searched index.
	ids map[sqlbase.ColumnID]int

	// ref is a copy of the ForeignKeyReference object in the table
	// descriptor.  During the check this is used to decide how to check
	// the value (MATCH style).
	//
	// TODO(knz): the entire reference object is not needed during the
	// mutation, only the match style. Simplify this.
	ref sqlbase.ForeignKeyReference

	// searchTable is the descriptor of the searched table. Stored only
	// for error messages; lookups use the pre-computed searchPrefix.
	searchTable *sqlbase.ImmutableTableDescriptor
	// writeIdx is the descriptor for the target index being mutated.
	// Stored only for error messages.
	writeIdx sqlbase.IndexDescriptor
}

// makeBaseFKHelper instanciates a FK helper.
//
// - dir is the direction of the check.
//
// - ref is a copy of the FK constraint object that points
//   to the table where to perform the existence check.
//
//   For forward checks, this is a copy of the FK
//   constraint placed on the referencing table.
//   For backward checks, this is a copy of the FK
//   constraint placed as backref on the referenced table.
//
//   This is used to derive the searched table/index,
//   and determine the MATCH style.
//
// - writeIdx is the target index being mutated. This is used
//   to determine prefixLen in combination with searchIdx.
//
// - colMap maps column IDs in the searched index, to positions
//   in the input `row` of datums during the check.
//
// - allocs is a suitable datum allocated used to initialize
//   the row fetcher.
//
// - otherTables is an object that provides schema extraction services.
//   TODO(knz): this should become homogeneous across the 3 packages
//   sql, sqlbase, row. The proliferation is annoying.
func makeBaseFKHelper(
	txn *client.Txn,
	otherTables TableLookupsByID,
	writeIdx sqlbase.IndexDescriptor,
	ref sqlbase.ForeignKeyReference,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
	dir FKCheck,
) (baseFKHelper, error) {
	b := baseFKHelper{
		txn:         txn,
		writeIdx:    writeIdx,
		searchTable: otherTables[ref.Table].Table,
		dir:         dir,
		ref:         ref,
	}
	if b.searchTable == nil {
		return b, errors.Errorf("referenced table %d not in provided table map %+v", ref.Table, otherTables)
	}
	b.searchPrefix = sqlbase.MakeIndexKeyPrefix(b.searchTable.TableDesc(), ref.Index)
	searchIdx, err := b.searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return b, err
	}
	b.prefixLen = len(searchIdx.ColumnIDs)
	if len(writeIdx.ColumnIDs) < b.prefixLen {
		b.prefixLen = len(writeIdx.ColumnIDs)
	}
	b.searchIdx = searchIdx
	tableArgs := FetcherTableArgs{
		Desc:             b.searchTable,
		Index:            b.searchIdx,
		ColIdxMap:        b.searchTable.ColumnIdxMap(),
		IsSecondaryIndex: b.searchIdx.ID != b.searchTable.PrimaryIndex.ID,
		Cols:             b.searchTable.Columns,
	}
	err = b.rf.Init(false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, alloc, tableArgs)
	if err != nil {
		return b, err
	}

	// See https://github.com/cockroachdb/cockroach/issues/20305 or
	// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
	// different composite foreign key matching methods.
	b.ids = make(map[sqlbase.ColumnID]int, len(writeIdx.ColumnIDs))
	switch ref.Match {
	case sqlbase.ForeignKeyReference_SIMPLE:
		for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
			if found, ok := colMap[writeColID]; ok {
				b.ids[searchIdx.ColumnIDs[i]] = found
			} else {
				return b, errSkipUnusedFK
			}
		}
		return b, nil
	case sqlbase.ForeignKeyReference_FULL:
		var missingColumns []string
		for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
			if found, ok := colMap[writeColID]; ok {
				b.ids[searchIdx.ColumnIDs[i]] = found
			} else {
				missingColumns = append(missingColumns, writeIdx.ColumnNames[i])
			}
		}
		switch len(missingColumns) {
		case 0:
			return b, nil
		case 1:
			return b, errors.Errorf("missing value for column %q in multi-part foreign key", missingColumns[0])
		case b.prefixLen:
			// All the columns are nulls, don't check the foreign key.
			return b, errSkipUnusedFK
		default:
			sort.Strings(missingColumns)
			return b, errors.Errorf("missing values for columns %q in multi-part foreign key", missingColumns)
		}
	default:
		return baseFKHelper{}, pgerror.NewAssertionErrorf(
			"unknown composite key match type: %v", ref.Match,
		)
	}
}
