// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSchemaFeedSingleVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	var tableID descpb.ID
	tdb.QueryRow(t, "SELECT 'foo'::REGCLASS::INT").Scan(&tableID)

	require.Equal(t, descpb.ID(52), tableID)
	m := schemafeed.MakeMetrics(time.Minute)
	clock := tc.Server(0).DB().Clock()
	before := clock.Now()
	log.Infof(ctx, "before %v", before)
	sf := schemafeed.New(ctx,
		&tc.Server(0).DistSQLServer().(*distsql.ServerImpl).ServerConfig,
		changefeedbase.OptSchemaChangeEventClassColumnChange,
		jobspb.ChangefeedTargets{
			tableID: {StatementTimeName: "foo"},
		},
		before,
		&m,
	)

	g, gCtx := errgroup.WithContext(ctx)
	gCtx, cancel := context.WithCancel(gCtx)
	defer g.Wait()
	defer cancel()
	g.Go(func() error { return sf.Run(gCtx) })
	tdb.Exec(t, "INSERT INTO foo VALUES (1)")
	{
		events, err := sf.Peek(ctx, clock.Now())
		require.NoError(t, err)
		require.Len(t, events, 0)
	}

	tdb.Exec(t, "ALTER TABLE foo ADD COLUMN k INT")
	{
		events, err := sf.Pop(ctx, clock.Now())
		require.NoError(t, err)
		require.Len(t, events, 1)
	}

	{
		events, err := sf.Peek(ctx, clock.Now())
		require.NoError(t, err)
		require.Len(t, events, 0)
	}

}
