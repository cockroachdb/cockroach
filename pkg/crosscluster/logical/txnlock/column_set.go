// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"encoding/binary"
	"hash/fnv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func constraintMixin(tableID descpb.ID, ucID descpb.ConstraintID) (uint64, error) {
	h := fnv.New64a()
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(tableID))
	binary.BigEndian.PutUint32(buf[4:], uint32(ucID))
	if _, err := h.Write(buf[:]); err != nil {
		return 0, errors.Wrap(err, "hashing constraint mixin")
	}
	return h.Sum64(), nil
}

// A columnSet is a collection of columns that are relevant for an individual
// constraint.
type columnSet struct {
	tableID descpb.ID
	columns []int32
	// mixin is an integer that is combined with the hash. It is used to ensure
	// different tables or unique constraints produce different hashes.
	mixin uint64
}

// hash computes the hash that is used as the lock. hash(rowA) != hash(rowB)
// implies !equal(rowA, rowB).
func (c *columnSet) hash(ctx context.Context, row tree.Datums) (LockHash, error) {
	h := fnv.New64a()
	var prefixBytes [8]byte
	binary.BigEndian.PutUint64(prefixBytes[:], c.mixin)
	if _, err := h.Write(prefixBytes[:]); err != nil {
		return 0, errors.Wrap(err, "hashing mixin for lock derivation")
	}
	for _, idx := range c.columns {
		datum := row[idx]
		ed := rowenc.EncDatum{Datum: datum}
		encoded, err := ed.Fingerprint(
			ctx,
			datum.ResolvedType(),
			&tree.DatumAlloc{},
			nil, /* appendTo */
			nil, /* acc */
		)
		if err != nil {
			return 0, errors.Wrap(err, "hashing datum for lock derivation")
		}
		if _, err := h.Write(encoded); err != nil {
			return 0, errors.Wrap(err, "hashing encoded datum for lock derivation")
		}
	}
	return LockHash(h.Sum64()), nil
}

// null returns true if any of the columns are null.
func (c *columnSet) null(row tree.Datums) bool {
	if len(row) == 0 {
		return true
	}
	for _, idx := range c.columns {
		if row[idx] == tree.DNull {
			return true
		}
	}
	return false
}

// equal returns true if the columns are equal. equal(rowA, rowB) implies
// hash(rowA) == hash(rowB). Convenience wrapper around columnSetEqual for
// comparing columns from the same column set.
func (c *columnSet) equal(
	ctx context.Context, evalCtx *eval.Context, rowA, rowB tree.Datums,
) (bool, error) {
	return columnSetEqual(ctx, evalCtx, c, c, rowA, rowB)
}

// equal returns true if the columns are equal. equal(rowA, rowB) implies
// hash(rowA) == hash(rowB). Supports comparing columns from different column
// sets, in the case of foreign key constraints.
func columnSetEqual(
	ctx context.Context, evalCtx *eval.Context, colA, colB *columnSet, rowA, rowB tree.Datums,
) (bool, error) {
	if len(colA.columns) != len(colB.columns) {
		return false, errors.AssertionFailedf(
			"mismatched column count in equal: table %d has %d columns, table %d has %d columns",
			colA.tableID, len(colA.columns), colB.tableID, len(colB.columns),
		)
	}
	if colA.null(rowA) || colB.null(rowB) {
		return false, nil
	}
	for i := range colA.columns {
		cmp, err := rowA[colA.columns[i]].Compare(ctx, evalCtx, rowB[colB.columns[i]])
		if err != nil {
			return false, errors.NewAssertionErrorWithWrappedErrf(err,
				"comparing datums for lock derivation: table %d column %d vs table %d column %d",
				colA.tableID, colA.columns[i], colB.tableID, colB.columns[i])
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}
