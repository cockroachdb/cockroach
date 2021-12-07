// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ptstorageproxy implements protectedts.Storage.
package ptstorageproxy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstoragedeprecated"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// storage interacts with the durable state of the protectedts subsystem.
type storage struct {
	deprecatedStorage protectedts.Storage
	storage           protectedts.Storage
}

// New creates a new Storage.
func New(settings *cluster.Settings, ex sqlutil.InternalExecutor) protectedts.Storage {
	return &storage{
		deprecatedStorage: ptstoragedeprecated.New(settings, ex),
		storage:           ptstorage.New(settings),
	}
}

var _ protectedts.Storage = (*storage)(nil)

func (s *storage) Protect(ctx context.Context, txn *kv.Txn, record *ptpb.Record) error {
	return s.deprecatedStorage.Protect(ctx, txn, record)
}

func (s *storage) GetRecord(
	ctx context.Context, txn *kv.Txn, uuid uuid.UUID,
) (*ptpb.Record, error) {
	return s.deprecatedStorage.GetRecord(ctx, txn, uuid)
}

func (s *storage) MarkVerified(ctx context.Context, txn *kv.Txn, uuid uuid.UUID) error {
	return s.deprecatedStorage.MarkVerified(ctx, txn, uuid)
}

func (s *storage) Release(ctx context.Context, txn *kv.Txn, uuid uuid.UUID) error {
	return s.deprecatedStorage.Release(ctx, txn, uuid)
}

func (s *storage) GetMetadata(ctx context.Context, txn *kv.Txn) (ptpb.Metadata, error) {
	return s.deprecatedStorage.GetMetadata(ctx, txn)
}

func (s *storage) GetState(ctx context.Context, txn *kv.Txn) (ptpb.State, error) {
	return s.deprecatedStorage.GetState(ctx, txn)
}

func (s *storage) UpdateTimestamp(
	ctx context.Context, txn *kv.Txn, id uuid.UUID, timestamp hlc.Timestamp,
) error {
	return s.deprecatedStorage.UpdateTimestamp(ctx, txn, id, timestamp)
}
