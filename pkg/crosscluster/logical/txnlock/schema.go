// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// tableConstraints is used to represent all of the constraints that are
// relevant for lock synthesis.
type tableConstraints struct {
	evalCtx           *eval.Context
	PrimaryKey        columnSet
	UniqueConstraints []columnSet
	// TODO(jeffswenson): add support for foreign key ordering
}

func newTableConstraints(
	evalCtx *eval.Context, table catalog.TableDescriptor,
) (*tableConstraints, error) {
	columnSchema := sqlwriter.GetColumnSchema(table)
	colIDToIndex := make(map[descpb.ColumnID]int32, len(columnSchema))
	for i, col := range columnSchema {
		colIDToIndex[col.Column.GetID()] = int32(i)
	}

	tc := &tableConstraints{evalCtx: evalCtx}

	primaryIndex := table.GetPrimaryIndex()
	pkMixin, err := tableMixin(table.GetID())
	if err != nil {
		return nil, err
	}
	tc.PrimaryKey = columnSet{
		columns: make([]int32, primaryIndex.NumKeyColumns()),
		mixin:   pkMixin,
	}
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		colID := primaryIndex.GetKeyColumnID(i)
		tc.PrimaryKey.columns[i] = colIDToIndex[colID]
	}

	for _, uc := range table.EnforcedUniqueConstraintsWithIndex() {
		if uc.GetID() == primaryIndex.GetID() {
			continue
		}
		cols := make([]int32, uc.NumKeyColumns())
		for i := 0; i < uc.NumKeyColumns(); i++ {
			colID := uc.GetKeyColumnID(i)
			cols[i] = colIDToIndex[colID]
		}
		ucMixin, err := uniqueIndexMixin(table.GetID(), uc.GetID())
		if err != nil {
			return nil, err
		}
		tc.UniqueConstraints = append(tc.UniqueConstraints, columnSet{
			columns: cols,
			mixin:   ucMixin,
		})
	}

	for _, uc := range table.EnforcedUniqueConstraintsWithoutIndex() {
		colIDs := uc.CollectKeyColumnIDs().Ordered()
		cols := make([]int32, len(colIDs))
		for i, colID := range colIDs {
			cols[i] = colIDToIndex[colID]
		}
		ucMixin, err := uniqueIndexMixin(
			table.GetID(),
			descpb.IndexID(uc.GetConstraintID()),
		)
		if err != nil {
			return nil, err
		}
		tc.UniqueConstraints = append(tc.UniqueConstraints, columnSet{
			columns: cols,
			mixin:   ucMixin,
		})
	}

	return tc, nil
}

func (t *tableConstraints) deriveLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
	pkHash, err := t.PrimaryKey.hash(ctx, row.Row)
	if err != nil {
		return nil, err
	}
	locks = append(locks, Lock{
		Hash: pkHash,
	})
	for _, uc := range t.UniqueConstraints {
		if uc.null(row.Row) && uc.null(row.PrevRow) {
			continue
		}
		eq, err := uc.equal(ctx, t.evalCtx, row.Row, row.PrevRow)
		if err != nil {
			return nil, err
		}
		if eq {
			continue
		}
		if !uc.null(row.Row) {
			h, err := uc.hash(ctx, row.Row)
			if err != nil {
				return nil, err
			}
			locks = append(locks, Lock{
				Hash: h,
				Read: false,
			})
		}
		if !uc.null(row.PrevRow) {
			h, err := uc.hash(ctx, row.PrevRow)
			if err != nil {
				return nil, err
			}
			locks = append(locks, Lock{
				Hash: h,
				Read: false,
			})
		}
	}
	return locks, nil
}

// DependsOn returns true if b must be applied before a can be applied. Only
// unique constraint conflicts are checked here; primary key conflicts are not
// possible because a transaction contains at most one record per row.
func (t *tableConstraints) DependsOn(
	ctx context.Context, a, b ldrdecoder.DecodedRow,
) (bool, error) {
	if len(t.UniqueConstraints) == 0 {
		return false, nil
	}
	// NOTE: We don't check primary keys because a transaction can't include two
	// writes to the same primary key.
	for _, uc := range t.UniqueConstraints {
		// We don't check current values because two rows can't have the same
		// unique value.
		eq, err := uc.equal(ctx, t.evalCtx, a.Row, b.PrevRow)
		if err != nil {
			return false, err
		}
		if eq {
			return true, nil
		}
	}
	return false, nil
}
