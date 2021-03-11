// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// NewOneLevelUncachedDescGetter returns a new DescGetter backed by the passed
// Txn. It will use the transaction to resolve mutable descriptors using
// GetDescriptorByID but will pass a nil DescGetter into those lookup calls to
// ensure that the entire graph of dependencies is not traversed.
func NewOneLevelUncachedDescGetter(txn *kv.Txn, codec keys.SQLCodec) catalog.BatchDescGetter {
	return &oneLevelUncachedDescGetter{
		txn:   txn,
		codec: codec,
	}
}

type oneLevelUncachedDescGetter struct {
	codec keys.SQLCodec
	txn   *kv.Txn
}

var _ catalog.DescGetter = (*oneLevelUncachedDescGetter)(nil)

func (t *oneLevelUncachedDescGetter) fromKeyValue(
	ctx context.Context, kv kv.KeyValue,
) (catalog.Descriptor, error) {
	return descriptorFromKeyValue(
		ctx,
		t.codec,
		kv,
		immutable,
		catalog.Any,
		bestEffort,
		// We pass a nil DescGetter for several reasons:
		// 1. avoid infinite recursion (hence the "oneLevel" aspect),
		// 2. avoid any unnecessary and irrelevant post-deserialization changes,
		// 3. it's not used by validation at this level anyway.
		nil, /* dg */
		catalog.ValidationLevelSelfOnly,
	)
}

func (t *oneLevelUncachedDescGetter) GetDesc(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	descKey := catalogkeys.MakeDescMetadataKey(t.codec, id)
	kv, err := t.txn.Get(ctx, descKey)
	if err != nil {
		return nil, err
	}
	return t.fromKeyValue(ctx, kv)
}

func (t *oneLevelUncachedDescGetter) GetDescs(
	ctx context.Context, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	ba := t.txn.NewBatch()
	for _, id := range reqs {
		descKey := catalogkeys.MakeDescMetadataKey(t.codec, id)
		ba.Get(descKey)
	}
	err := t.txn.Run(ctx, ba)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.Descriptor, len(reqs))
	for i, res := range ba.Results {
		ret[i], err = t.fromKeyValue(ctx, res.Rows[0])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
