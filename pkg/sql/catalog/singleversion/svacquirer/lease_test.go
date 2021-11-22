// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svacquirer_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svacquirer"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tableName, tableID := svtestutils.SetUpTestingTable(t, tdb)
	var a *svacquirer.Acquirer
	{
		codec := keys.SystemSQLCodec
		instance := svtestutils.TestInstance{}
		exp := kvDB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
		rff := s.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory
		instance.SetSession(svtestutils.MakeTestSession("foo", exp))
		storage := svstorage.NewStorage(codec, tableID)
		a = svacquirer.NewAcquirer(log.AmbientContext{}, s.Stopper(), kvDB, rff, storage, &instance)
	}

	before := kvDB.Clock().Now()
	l, err := a.Acquire(ctx, 1)
	require.NoError(t, err)
	require.True(t, before.Less(l.Start()))

	tdb.CheckQueryResults(t, "SELECT * FROM "+tableName, [][]string{
		{"lease", "1", "foo"},
	})
	l2, err := a.Acquire(ctx, 1)
	tdb.CheckQueryResults(t, "SELECT * FROM "+tableName, [][]string{
		{"lease", "1", "foo"},
	})
	l2.Release(ctx, before)
	tdb.CheckQueryResults(t, "SELECT * FROM "+tableName, [][]string{
		{"lease", "1", "foo"},
	})
	l.Release(ctx, before)
	tdb.CheckQueryResultsRetry(t, "SELECT * FROM "+tableName, [][]string{})
}
