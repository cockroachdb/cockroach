// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutorClearsMonitorMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	mon := s.SQLServerInternal().(*SQLServer).internalDBMemMonitor
	ief := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	sessionData := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "TestInternalExecutorClearsMonitorMemory")
	ie := ief.NewInternalExecutor(sessionData)
	rows, err := ie.QueryIteratorEx(ctx, "test", nil, sessiondata.NodeUserSessionDataOverride, `SELECT 1`)
	require.NoError(t, err)
	require.Greater(t, mon.AllocBytes(), int64(0))
	err = rows.Close()
	require.NoError(t, err)
	srv.Stopper().Stop(ctx)
	require.Equal(t, mon.AllocBytes(), int64(0))
}
