// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/database"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDatabaseAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if err := kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		if _, err := catalogkv.GetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, keys.SystemDatabaseID); err != nil {
			return err
		}
		if _, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, keys.SystemDatabaseID); err != nil {
			return err
		}

		databaseCache := database.NewCache(keys.SystemSQLCodec, config.NewSystemConfig(zonepb.DefaultZoneConfigRef()))
		_, err := databaseCache.GetDatabaseDescByID(ctx, txn, keys.SystemDatabaseID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
