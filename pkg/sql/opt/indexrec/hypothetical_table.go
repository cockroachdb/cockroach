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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BuildHypotheticalTables builds a hypotheticalTable for each table in
// indexCandidates. This hypotheticalTable stores a hypothetical index for each
// of the table's index candidates.
func BuildHypotheticalTables(
	indexCandidates map[cat.Table][][]cat.IndexColumn, tableZones map[cat.Table]*zonepb.ZoneConfig,
) (oldTables, hypTables map[cat.StableID]cat.Table) {
	hypTables = make(map[cat.StableID]cat.Table)
	oldTables = make(map[cat.StableID]cat.Table)

	for t, indexes := range indexCandidates {
		hypIndexes := []cat.Index{t.Index(cat.PrimaryIndex)}
		hypTable := hypotheticalTable{t, hypIndexes}
		count := 1

		for _, index := range indexes {
			idx := hypotheticalIndex{
				tab:          &hypTable,
				name:         tree.Name(fmt.Sprintf("_hyp_%d", count)),
				indexOrdinal: count,
				cols:         index,
				zone:         tableZones[t],
			}
			hypIndexes = append(hypIndexes, &idx)
			count++
		}

		hypTable.hypotheticalIndexes = hypIndexes
		oldTables[t.ID()] = t
		hypTables[t.ID()] = &hypTable
	}

	return oldTables, hypTables
}

// hypotheticalTable is a wrapper around cat.Table, used for creating index
// recommendations. The hypotheticalIndexes slice stores fake indexes that could
// potentially speed up queries to this table.
type hypotheticalTable struct {
	cat.Table
	hypotheticalIndexes []cat.Index
}

var _ cat.Table = &hypotheticalTable{}

// IndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) IndexCount() int {
	return len(ht.hypotheticalIndexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) WritableIndexCount() int {
	// The only writable index is the Primary Index
	return 1
}

// DeletableIndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) DeletableIndexCount() int {
	// The only deletable index is the Primary Index
	return 1
}

// Index is part of the cat.Table interface.
func (ht *hypotheticalTable) Index(i cat.IndexOrdinal) cat.Index {
	return ht.hypotheticalIndexes[i]
}
