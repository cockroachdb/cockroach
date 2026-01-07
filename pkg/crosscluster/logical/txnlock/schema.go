// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"encoding/binary"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func tablePrefix(tableID descpb.ID) uint64 {
	// TODO(jeffswenson): hash this
	return uint64(tableID)
}

func uniqueIndexPrefix(tableID descpb.ID, ucID descpb.IndexID) uint64 {
	// TODO(jeffswenson): hash this
	return uint64(tableID)<<32 | uint64(ucID)
}

type columnSet struct {
	columns []int32
	// prefix is an integer that is combined with the hash. It is used to ensure
	// different tables or unique constraints produce different hashes.
	prefix uint64
}

func (c *columnSet) hash(row tree.Datums) uint64 {
	// TODO(jeffswenson): can we get rid of the panic here?
	h := fnv.New64a()
	// Include the prefix in the hash to ensure different tables/constraints
	// produce different hashes even with the same column values.
	var prefixBytes [8]byte
	binary.BigEndian.PutUint64(prefixBytes[:], c.prefix)
	h.Write(prefixBytes[:])
	for _, idx := range c.columns {
		datum := row[idx]
		// Encode the datum for consistent hashing
		ed := rowenc.EncDatum{Datum: datum}
		encoded, err := ed.Fingerprint(
			context.Background(),
			datum.ResolvedType(),
			&tree.DatumAlloc{},
			nil, /* appendTo */
			nil, /* acc */
		)
		if err != nil {
			panic(err)
		}
		h.Write(encoded)
	}
	return h.Sum64()
}

func (c *columnSet) null(row tree.Datums) bool {
	for _, idx := range c.columns {
		if row[idx] == tree.DNull {
			return true
		}
	}
	return false
}

func (c *columnSet) equal(ctx *eval.Context, rowA, rowB tree.Datums) bool {
	// if either is null return false
	if c.null(rowA) || c.null(rowB) {
		return false
	}
	// compare each column in the set
	for _, idx := range c.columns {
		cmp, err := rowA[idx].Compare(context.Background(), ctx, rowB[idx])
		if err != nil {
			panic(err)
		}
		if cmp != 0 {
			return false
		}
	}
	return true
}

type tableConstraints struct {
	PrimaryKey        columnSet
	UniqueConstraints []columnSet
	// TODO(jeffswenson): add support for foreign key ordering
}

func newTableConstraints(table catalog.TableDescriptor) *tableConstraints {
	// Get the column schema which determines the order of datums in rows
	columnSchema := sqlwriter.GetColumnSchema(table)

	// Build a map from column ID to index in the datums array
	colIDToIndex := make(map[descpb.ColumnID]int32, len(columnSchema))
	for i, col := range columnSchema {
		colIDToIndex[col.Column.GetID()] = int32(i)
	}

	tc := &tableConstraints{}

	// Extract primary key columns
	primaryIndex := table.GetPrimaryIndex()
	tc.PrimaryKey = columnSet{
		columns: make([]int32, primaryIndex.NumKeyColumns()),
		prefix:  tablePrefix(table.GetID()),
	}
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		colID := primaryIndex.GetKeyColumnID(i)
		tc.PrimaryKey.columns[i] = colIDToIndex[colID]
	}

	// Extract unique constraints with indexes (excluding primary key)
	for _, uc := range table.EnforcedUniqueConstraintsWithIndex() {
		// Skip the primary key index
		if uc.GetID() == primaryIndex.GetID() {
			continue
		}
		cols := make([]int32, uc.NumKeyColumns())
		for i := 0; i < uc.NumKeyColumns(); i++ {
			colID := uc.GetKeyColumnID(i)
			cols[i] = colIDToIndex[colID]
		}
		tc.UniqueConstraints = append(tc.UniqueConstraints, columnSet{
			columns: cols,
			prefix:  uniqueIndexPrefix(table.GetID(), uc.GetID()),
		})
	}

	// Extract unique constraints without indexes
	for i, uc := range table.EnforcedUniqueConstraintsWithoutIndex() {
		colIDs := uc.CollectKeyColumnIDs().Ordered()
		cols := make([]int32, len(colIDs))
		for i, colID := range colIDs {
			cols[i] = colIDToIndex[colID]
		}
		tc.UniqueConstraints = append(tc.UniqueConstraints, columnSet{
			columns: cols,
			prefix: uniqueIndexPrefix(
				table.GetID(),
				descpb.IndexID(len(table.EnforcedUniqueConstraintsWithIndex())+i+1),
			),
		})
	}

	return tc
}

func (t *tableConstraints) deriveLocks(row ldrdecoder.DecodedRow, locks []Lock) []Lock {
	evalCtx := eval.Context{}
	locks = append(locks, Lock{
		Hash: t.PrimaryKey.hash(row.Row),
	})
	for _, uc := range t.UniqueConstraints {
		switch {
		case uc.null(row.Row) && uc.null(row.PrevRow):
			continue
		case uc.equal(&evalCtx, row.Row, row.PrevRow):
			continue
		default:
			if !uc.null(row.Row) {
				locks = append(locks, Lock{
					Hash: uc.hash(row.Row),
					Read: false,
				})
			}
			if !uc.null(row.PrevRow) {
				locks = append(locks, Lock{
					Hash: uc.hash(row.PrevRow),
					Read: false,
				})
			}
		}
	}
	return locks
}

func (t *tableConstraints) DependsOn(a, b ldrdecoder.DecodedRow) bool {
	// TODO(jeffswenson): we should get a real eval context during
	// initialization.
	evalCtx := eval.Context{}
	if len(t.UniqueConstraints) == 0 {
		return false
	}
	for _, uc := range t.UniqueConstraints {
		if uc.equal(&evalCtx, a.PrevRow, b.Row) {
			return true
		}
	}
	return false
}
