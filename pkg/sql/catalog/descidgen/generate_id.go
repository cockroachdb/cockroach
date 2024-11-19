// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descidgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

// generator implements eval.DescIDGenerator.
type generator struct {
	settings *cluster.Settings
	codec    keys.SQLCodec
	key      func(context.Context) (roachpb.Key, error)
	getOrInc func(ctx context.Context, key roachpb.Key, inc int64) (int64, error)
}

// GenerateUniqueDescID is part of the eval.DescIDGenerator interface.
func (g *generator) GenerateUniqueDescID(ctx context.Context) (id catid.DescID, _ error) {
	nextID, err := g.run(ctx, 1)
	if err != nil {
		return catid.InvalidDescID, err
	}
	return nextID - 1, nil
}

// IncrementDescID is part of the eval.DescIDGenerator interface.
func (g *generator) IncrementDescID(ctx context.Context, inc int64) (id catid.DescID, _ error) {
	nextID, err := g.run(ctx, inc)
	if err != nil {
		return catid.InvalidDescID, err
	}
	return nextID - catid.DescID(inc), nil
}

// PeekNextUniqueDescID is part of the eval.DescIDGenerator interface.
func (g *generator) PeekNextUniqueDescID(ctx context.Context) (descpb.ID, error) {
	return g.run(ctx, 0 /* inc */)
}

// run is a convenience method for accessing the descriptor ID counter.
func (g *generator) run(ctx context.Context, inc int64) (catid.DescID, error) {
	key, err := g.key(ctx)
	if err != nil {
		return 0, err
	}
	nextID, err := g.getOrInc(ctx, key, inc)
	return catid.DescID(nextID), err
}

// NewGenerator constructs a non-transactional eval.DescIDGenerator.
//
// In this implementation the value returned by PeekNextUniqueDescID is _not_
// guaranteed to be the same as that returned by a subsequent call to
// GenerateUniqueDescID. It will, however, be a lower bound on it.
//
// In this implementation the increment applied by IncrementDescID is _not_
// guaranteed to be the exact. It will, however, be a lower bound on the actual
// increment.
//
// Note that this implementation must not be used during the execution of a
// transaction which uses a transactional generator as constructed by
// NewTransactionalGenerator. Otherwise, deadlocks may occur.
func NewGenerator(settings *cluster.Settings, codec keys.SQLCodec, db *kv.DB) eval.DescIDGenerator {
	return &generator{
		settings: settings,
		codec:    codec,
		key: func(ctx context.Context) (roachpb.Key, error) {
			return key(ctx, codec, settings)
		},
		getOrInc: func(ctx context.Context, key roachpb.Key, inc int64) (int64, error) {
			if inc == 0 {
				ret, err := db.Get(ctx, key)
				return ret.ValueInt(), err
			}
			return kv.IncrementValRetryable(ctx, db, key, inc)
		},
	}
}

func key(
	ctx context.Context, codec keys.SQLCodec, settings *cluster.Settings,
) (roachpb.Key, error) {
	key := codec.SequenceKey(keys.DescIDSequenceID)
	return key, nil
}

// NewTransactionalGenerator constructs a transactional eval.DescIDGenerator.
func NewTransactionalGenerator(
	settings *cluster.Settings, codec keys.SQLCodec, txn *kv.Txn,
) eval.DescIDGenerator {
	return &generator{
		settings: settings,
		codec:    codec,
		key: func(ctx context.Context) (roachpb.Key, error) {
			return key(ctx, codec, settings)
		},
		getOrInc: func(ctx context.Context, key roachpb.Key, inc int64) (_ int64, err error) {
			var ret kv.KeyValue
			if inc == 0 {
				ret, err = txn.Get(ctx, key)
			} else {
				ret, err = txn.Inc(ctx, key, inc)
			}
			return ret.ValueInt(), err
		},
	}
}

// GenerateUniqueRoleID returns the next available Role ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueRoleID(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec,
) (catid.RoleID, error) {
	return IncrementUniqueRoleID(ctx, db, codec, 1)
}

// GenerateUniqueRoleIDInTxn is like GenerateUniqueRoleID but performs the
// operation in the provided transaction.
func GenerateUniqueRoleIDInTxn(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) (catid.RoleID, error) {
	res, err := txn.Inc(ctx, codec.SequenceKey(keys.RoleIDSequenceID), 1)
	if err != nil {
		return 0, err
	}
	newVal, err := res.Value.GetInt()
	if err != nil {
		return 0, errors.NewAssertionErrorWithWrappedErrf(err, "failed to get int from role_id sequence")
	}
	return catid.RoleID(newVal - 1), nil
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
