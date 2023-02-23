// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	mon := s.(*TestServer).sqlServer.internalDBMemMonitor
	ief := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	sessionData := sql.NewFakeSessionData(&s.ClusterSettings().SV, "TestInternalExecutorClearsMonitorMemory")
	ie := ief.NewInternalExecutor(sessionData)
	rows, err := ie.QueryIteratorEx(ctx, "test", nil, sessiondata.NodeUserSessionDataOverride, `SELECT 1`)
	require.NoError(t, err)
	require.Greater(t, mon.AllocBytes(), int64(0))
	err = rows.Close()
	require.NoError(t, err)
	s.Stopper().Stop(ctx)
	require.Equal(t, mon.AllocBytes(), int64(0))
}
