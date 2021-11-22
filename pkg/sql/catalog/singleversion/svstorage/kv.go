// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

func scan(
	ctx context.Context, txn *kv.Txn, c tableCodec, prefixes []Row, resultFunc func(Row),
) (err error) {
	b, err := runBatch(ctx, txn, c.encodePrefix, addScanToBatch, prefixes)
	if err != nil {
		return err
	}
	for _, res := range b.Results {
		// TODO(ajwerner): Deal with resume spans. They ought to be uncommon. Or,
		// at least, they would be if something periodically cleaned up.
		if res.ResumeSpan != nil {
			return errors.AssertionFailedf("got a resume span from scanning")
		}
		for _, kv := range res.Rows {
			var r Row
			if err := c.decode(kv.Key, &r); err != nil {
				return err
			}
			resultFunc(r)
		}
	}
	return nil
}

type encodeFunc func(Row) (roachpb.Key, error)

func runBatch(
	ctx context.Context, txn *kv.Txn, ef encodeFunc, addToBatch addToBatchFunc, rows []Row,
) (*kv.Batch, error) {
	b := txn.NewBatch()
	if err := addRowsToBatch(b, ef, addToBatch, rows); err != nil {
		return nil, err
	}
	return b, txn.Run(ctx, b)
}

func addRowsToBatch(b *kv.Batch, ef encodeFunc, addToBatch addToBatchFunc, rows []Row) error {
	for i := range rows {
		k, err := ef(rows[i])
		if err != nil {
			return err
		}
		addToBatch(b, k)
	}
	return nil
}

func deleteRows(ctx context.Context, txn *kv.Txn, c tableCodec, rows []Row) error {
	_, err := runBatch(ctx, txn, c.encode, addDelToBatch, rows)
	return err
}

func putRows(ctx context.Context, txn *kv.Txn, c tableCodec, rows []Row) error {
	_, err := runBatch(ctx, txn, c.encode, addPutToBatch, rows)
	return err
}

type addToBatchFunc func(*kv.Batch, roachpb.Key)

func addDelToBatch(b *kv.Batch, k roachpb.Key) { b.Del(k) }

func emptyTuple() *roachpb.Value {
	var v roachpb.Value
	v.SetTuple(nil)
	return &v
}

func addPutToBatch(b *kv.Batch, k roachpb.Key) { b.Put(k, emptyTuple()) }

func addScanToBatch(b *kv.Batch, k roachpb.Key) { b.Scan(k, k.PrefixEnd()) }
