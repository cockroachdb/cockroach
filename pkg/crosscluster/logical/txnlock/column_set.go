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

// lockKind distinguishes different kinds of locks so that primary key locks
// and unique index locks hash into different spaces.
type lockKind byte

const (
	primaryKeyLock  lockKind = 1
	uniqueIndexLock lockKind = 2
)

func tableMixin(tableID descpb.ID) (uint64, error) {
	h := fnv.New64a()
	if _, err := h.Write([]byte{byte(primaryKeyLock)}); err != nil {
		return 0, errors.Wrap(err, "hashing table mixin")
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(tableID))
	if _, err := h.Write(buf[:]); err != nil {
		return 0, errors.Wrap(err, "hashing table mixin")
	}
	return h.Sum64(), nil
}

func uniqueIndexMixin(tableID descpb.ID, ucID descpb.IndexID) (uint64, error) {
	h := fnv.New64a()
	if _, err := h.Write([]byte{byte(uniqueIndexLock)}); err != nil {
		return 0, errors.Wrap(err, "hashing unique index mixin")
	}
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(tableID))
	binary.BigEndian.PutUint32(buf[4:], uint32(ucID))
	if _, err := h.Write(buf[:]); err != nil {
		return 0, errors.Wrap(err, "hashing unique index mixin")
	}
	return h.Sum64(), nil
}

// A columnSet is a collection of columns that are relevant for an individual
// constraint.
type columnSet struct {
	columns []int32
	// mixin is an integer that is combined with the hash. It is used to ensure
	// different tables or unique constraints produce different hashes.
	mixin uint64
}

// hash computes the hash that is used as the lock. hash(rowA) != hash(rowB)
// implies !equal(rowA, rowB).
func (c *columnSet) hash(ctx context.Context, row tree.Datums) (uint64, error) {
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
	return h.Sum64(), nil
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
// hash(rowA) == hash(rowB).
func (c *columnSet) equal(
	ctx context.Context, evalCtx *eval.Context, rowA, rowB tree.Datums,
) (bool, error) {
	if c.null(rowA) || c.null(rowB) {
		return false, nil
	}
	for _, idx := range c.columns {
		cmp, err := rowA[idx].Compare(ctx, evalCtx, rowB[idx])
		if err != nil {
			return false, errors.Wrap(err, "comparing datums for lock derivation")
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}
