// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descidgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// Generator implements DescIDGenerator.
type Generator struct {
	db    *kv.DB
	codec keys.SQLCodec
}

// NewGenerator constructs a new Generator.
func NewGenerator(codec keys.SQLCodec, db *kv.DB) *Generator {
	return &Generator{
		codec: codec,
		db:    db,
	}
}

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter.
func (g *Generator) GenerateUniqueDescID(ctx context.Context) (catid.DescID, error) {
	// Increment unique descriptor counter.
	newVal, err := kv.IncrementValRetryable(ctx, g.db, g.codec.DescIDSequenceKey(), 1)
	if err != nil {
		return descpb.InvalidID, err
	}
	return descpb.ID(newVal - 1), nil
}

// TransactionalGenerator implements eval.DescIDGenerator using a transaction.
type TransactionalGenerator struct {
	codec keys.SQLCodec
	txn   *kv.Txn
}

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter.
func (t *TransactionalGenerator) GenerateUniqueDescID(ctx context.Context) (catid.DescID, error) {
	got, err := t.txn.Inc(ctx, t.codec.DescIDSequenceKey(), 1)
	if err != nil {
		return 0, err
	}
	return catid.DescID(got.ValueInt() - 1), nil
}

// NewTransactionalGenerator constructs a transactional DescIDGenerator.
func NewTransactionalGenerator(codec keys.SQLCodec, txn *kv.Txn) *TransactionalGenerator {
	return &TransactionalGenerator{
		codec: codec,
		txn:   txn,
	}
}

// PeekNextUniqueDescID returns the next as-of-yet unassigned unique descriptor
// ID in the sequence. Note that this value is _not_ guaranteed to be the same
// as that returned by a subsequent call to GenerateUniqueDescID. It will,
// however, be a lower bound on it.
//
// Note that this function must not be used during the execution of a
// transaction which uses a TransactionalGenerator. Otherwise, deadlocks
// may occur.
func PeekNextUniqueDescID(ctx context.Context, db *kv.DB, codec keys.SQLCodec) (descpb.ID, error) {
	v, err := db.Get(ctx, codec.DescIDSequenceKey())
	if err != nil {
		return descpb.InvalidID, err
	}
	return descpb.ID(v.ValueInt()), nil
}

// GenerateUniqueRoleID returns the next available Role ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueRoleID(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec,
) (catid.RoleID, error) {
	return IncrementUniqueRoleID(ctx, db, codec, 1)
}

// IncrementUniqueRoleID returns the next available Role ID and increments
// the counter by inc. The incrementing is non-transactional, and the counter
// could be incremented multiple times because of retries.
func IncrementUniqueRoleID(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, inc int64,
) (catid.RoleID, error) {
	newVal, err := kv.IncrementValRetryable(ctx, db, codec.SequenceKey(keys.RoleIDSequenceID), inc)
	if err != nil {
		return 0, err
	}
	return catid.RoleID(newVal - inc), nil
}
