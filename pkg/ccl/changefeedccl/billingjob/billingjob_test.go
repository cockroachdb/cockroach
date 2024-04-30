// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package billingjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl" // register ccl hooks
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/billingjob"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchChangefeedBillingBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Parallel()

	ctx := context.Background()
	params := base.TestServerArgs{}
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	execCtx, close := sql.MakeJobExecContext(ctx, "test", username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer close()

	// add some changefeed jobs
	// registry := s.JobRegistry().(*jobs.Registry)
	stmt := "set cluster setting kv.rangefeed.enabled = true"
	_, err := s.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor().Exec(ctx, "test", nil, stmt)
	require.NoError(t, err)

	ie := s.InternalExecutor().(*sql.InternalExecutor)

	stmt = "CREATE DATABASE testdb"
	_, err = ie.ExecEx(ctx, "test-create-db", nil, sessiondata.InternalExecutorOverride{User: username.NodeUserName()}, stmt)
	require.NoError(t, err)

	stmt = "CREATE TABLE testdb.test as SELECT generate_series(1, 1000) AS id"
	_, err = ie.ExecEx(ctx, "test-create-table", nil, sessiondata.InternalExecutorOverride{User: username.NodeUserName()}, stmt)
	require.NoError(t, err)

	stmt = `CREATE CHANGEFEED FOR TABLE testdb.test INTO 'null://' WITH initial_scan='no';`
	_, err = ie.ExecEx(ctx, "test-create-cf", nil, sessiondata.InternalExecutorOverride{User: username.NodeUserName()}, stmt)
	require.NoError(t, err)

	res, err := billingjob.FetchChangefeedBillingBytes(ctx, execCtx)
	require.NoError(t, err)
	assert.NotZero(t, res)
}

// TODO: add test for experimental / non enterpise feeds, if they're implemented as jobs
