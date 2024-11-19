// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// WithDatabase wraps s such that any calls made with a nil *Txn will be wrapped
// in a call to db.Txn. This is often convenient in testing.
func WithDatabase(s protectedts.Manager, db isql.DB) protectedts.Storage {
	return &storageWithDatabase{s: s, db: db}
}

type storageWithDatabase struct {
	db isql.DB
	s  protectedts.Manager
}

func (s *storageWithDatabase) Protect(ctx context.Context, r *ptpb.Record) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return s.s.WithTxn(txn).Protect(ctx, r)
	})
}

func (s *storageWithDatabase) GetRecord(
	ctx context.Context, id uuid.UUID,
) (r *ptpb.Record, _ error) {
	return r, s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		r, err = s.s.WithTxn(txn).GetRecord(ctx, id)
		return err
	})
}

func (s *storageWithDatabase) MarkVerified(ctx context.Context, id uuid.UUID) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return s.s.WithTxn(txn).MarkVerified(ctx, id)
	})
}

func (s *storageWithDatabase) Release(ctx context.Context, id uuid.UUID) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return s.s.WithTxn(txn).Release(ctx, id)
	})
}

func (s *storageWithDatabase) GetMetadata(ctx context.Context) (md ptpb.Metadata, _ error) {
	return md, s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		md, err = s.s.WithTxn(txn).GetMetadata(ctx)
		return err
	})
}

func (s *storageWithDatabase) GetState(ctx context.Context) (state ptpb.State, err error) {
	return state, s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		state, err = s.s.WithTxn(txn).GetState(ctx)
		return err
	})
}

func (s *storageWithDatabase) UpdateTimestamp(
	ctx context.Context, id uuid.UUID, timestamp hlc.Timestamp,
) (err error) {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		return s.s.WithTxn(txn).UpdateTimestamp(ctx, id, timestamp)
	})
}
