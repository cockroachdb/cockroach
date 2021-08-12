// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrationutils provides utilities for use in migrations to
// affect the schema layer.
package migrationutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/errors"
)

// DB is defined just to allow us to use a fake client.DB when testing this
// package.
type DB interface {
	Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]kv.KeyValue, error)
	Get(ctx context.Context, key interface{}) (kv.KeyValue, error)
	Put(ctx context.Context, key, value interface{}) error
	Txn(ctx context.Context, retryable func(ctx context.Context, txn *kv.Txn) error) error
}

// CreateSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func CreateSystemTable(
	ctx context.Context, db DB, codec keys.SQLCodec, desc catalog.TableDescriptor,
) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		tKey := catalogkeys.MakePublicObjectNameKey(codec, desc.GetParentID(), desc.GetName())
		b.CPut(tKey, desc.GetID(), nil)
		b.CPut(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()), desc.DescriptorProto(), nil)
		if err := txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	// CPuts only provide idempotent inserts if we ignore the errors that arise
	// when the condition isn't met.
	if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
		return nil
	}
	return err
}
