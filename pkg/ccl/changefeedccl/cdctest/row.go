// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/stretchr/testify/require"
)

// MakeRangeFeedValueReader starts rangefeed on the specified table and returns a function
// that returns the next *roachpb.RangeFeedValue from the table.
// This funciton is intended to be used in tests that wish to read low level roachpb.KeyValue(s).
// Instead of trying to generate KVs ourselves (subject to encoding restrictions, etc), it is
// simpler to just "INSERT ..." into the table, and then use this function to read next value.
func MakeRangeFeedValueReader(
	t *testing.T, execCfgI interface{}, desc catalog.TableDescriptor,
) (func(t *testing.T) *roachpb.RangeFeedValue, func()) {
	t.Helper()
	execCfg := execCfgI.(sql.ExecutorConfig)
	rows := make(chan *roachpb.RangeFeedValue)
	ctx, cleanup := context.WithCancel(context.Background())

	_, err := execCfg.RangeFeedFactory.RangeFeed(ctx, "feed-"+desc.GetName(),
		[]roachpb.Span{desc.PrimaryIndexSpan(keys.SystemSQLCodec)},
		execCfg.Clock.Now(),
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case <-ctx.Done():
			case rows <- value:
			}
		},
		rangefeed.WithDiff(true),
	)
	require.NoError(t, err)

	var timeout = 5 * time.Second
	if util.RaceEnabled {
		timeout = 3 * timeout
	}

	// Helper to read next rangefeed value.
	return func(t *testing.T) *roachpb.RangeFeedValue {
		t.Helper()
		select {
		case r := <-rows:
			return r
		case <-time.After(timeout):
			t.Fatal("timeout reading row")
			return nil
		}
	}, cleanup
}

// GetHydratedTableDescriptor returns a table descriptor for the specified
// table.  The descriptor is "hydrated" if it has user defined data types.
func GetHydratedTableDescriptor(
	t *testing.T, execCfgI interface{}, kvDB *kv.DB, tableName string,
) catalog.TableDescriptor {
	t.Helper()
	desc := desctestutils.TestingGetPublicTableDescriptor(
		kvDB, keys.SystemSQLCodec, "defaultdb", tableName)
	if !desc.ContainsUserDefinedTypes() {
		return desc
	}

	ctx := context.Background()
	execCfg := execCfgI.(sql.ExecutorConfig)
	collection := execCfg.CollectionFactory.NewCollection(ctx, nil)
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, execCfg.Clock.Now())
		if err != nil {
			return err
		}
		desc, err = collection.GetImmutableTableByID(ctx, txn, desc.GetID(), tree.ObjectLookupFlags{})
		return err
	}))
	// Immediately release the lease, since we only need it for the exact
	// timestamp requested.
	collection.ReleaseAll(ctx)
	return desc
}
