// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// WithDatabase wraps s such that any calls made with a nil *Txn will be wrapped
// in a call to db.Txn. This is often convenient in testing.
func WithDatabase(
	s protectedts.Storage, db *kv.DB, ief sqlutil.InternalExecutorFactory,
) protectedts.Storage {
	return &storageWithDatabase{s: s, db: db, ief: ief}
}

type storageWithDatabase struct {
	db  *kv.DB
	ief sqlutil.InternalExecutorFactory
	s   protectedts.Storage
}

func (s *storageWithDatabase) Protect(ctx context.Context, txn *kv.Txn, r *ptpb.Record) error {
	if txn == nil {
		return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return s.s.Protect(ctx, txn, r)
		})
	}
	return s.s.Protect(ctx, txn, r)
}

func (s *storageWithDatabase) GetRecord(
	ctx context.Context, txn *kv.Txn, id uuid.UUID, executor sqlutil.InternalExecutor,
) (r *ptpb.Record, err error) {
	if txn == nil {
		err = s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, ie sqlutil.InternalExecutor) error {
			r, err = s.s.GetRecord(ctx, newTxn, id, ie)
			return err
		})
		return r, err
	}
	return s.s.GetRecord(ctx, txn, id, executor)
}

func (s *storageWithDatabase) MarkVerified(
	ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, id uuid.UUID,
) error {
	if txn == nil {
		return s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, executor sqlutil.InternalExecutor) error {
			return s.s.Release(ctx, newTxn, executor, id)
		})
	}
	return s.s.Release(ctx, txn, ie, id)
}

func (s *storageWithDatabase) Release(
	ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, id uuid.UUID,
) error {
	if txn == nil {
		return s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, executor sqlutil.InternalExecutor) error {
			return s.s.Release(ctx, newTxn, executor, id)
		})
	}
	return s.s.Release(ctx, txn, ie, id)
}

func (s *storageWithDatabase) GetMetadata(
	ctx context.Context, txn *kv.Txn, executor sqlutil.InternalExecutor,
) (md ptpb.Metadata, err error) {
	if txn == nil {
		err = s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, ie sqlutil.InternalExecutor) error {
			md, err = s.s.GetMetadata(ctx, newTxn, ie)
			return err
		})
		return md, err
	}
	return s.s.GetMetadata(ctx, txn, executor)
}

func (s *storageWithDatabase) GetState(
	ctx context.Context, txn *kv.Txn, executor sqlutil.InternalExecutor,
) (state ptpb.State, err error) {
	if txn == nil {
		err = s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, ie sqlutil.InternalExecutor) (err error) {
			state, err = s.s.GetState(ctx, newTxn, ie)
			return err
		})
		return state, err
	}
	return s.s.GetState(ctx, txn, executor)
}

func (s *storageWithDatabase) UpdateTimestamp(
	ctx context.Context,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	id uuid.UUID,
	timestamp hlc.Timestamp,
) (err error) {
	if txn == nil {
		err = s.ief.TxnWithExecutor(ctx, s.db, nil /* sessionData */, func(ctx context.Context, newTxn *kv.Txn, executor sqlutil.InternalExecutor) error {
			return s.s.UpdateTimestamp(ctx, newTxn, executor, id, timestamp)
		})
		return err
	}
	return s.s.UpdateTimestamp(ctx, txn, ie, id, timestamp)
}
