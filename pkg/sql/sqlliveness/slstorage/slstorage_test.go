// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Start the server such that it effectively never GC's records so that
	// we can instantiate another Storage instance which we can control.
	serverSettings := cluster.MakeTestingClusterSettings()
	slstorage.DefaultGCInterval.Override(&serverSettings.SV, 24*time.Hour)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: serverSettings,
	})
	defer s.Stopper().Stop(ctx)

	// Wait for our server which roughly never runs deletion runs to run its
	// initial run.
	serverLivenessProvider := s.SQLLivenessProvider().(sqlliveness.Provider)
	require.Eventually(t, func() bool {
		return serverLivenessProvider.Metrics().(*slstorage.Metrics).SessionDeletionsRuns.Count() > 0
	}, 5*time.Second, time.Millisecond)

	// Now construct a new storage which we can control. We won't start it just
	// yet.
	settings := cluster.MakeTestingClusterSettings()
	slstorage.DefaultGCInterval.Override(&settings.SV, 1*time.Microsecond)
	ie := s.InternalExecutor().(tree.InternalExecutor)
	slStorage := slstorage.NewStorage(s.Stopper(), s.Clock(), s.DB(), ie, settings)

	const sid = "1"
	err := slStorage.Insert(ctx, sid, s.Clock().Now())
	require.NoError(t, err)
	metrics := slStorage.Metrics()
	require.Equal(t, int64(1), metrics.WriteSuccesses.Count())

	// Start the new Storage and wait for it delete our session which expired
	// immediately.
	slStorage.Start(ctx)
	require.Eventually(t, func() bool {
		a, err := slStorage.IsAlive(ctx, sid)
		return !a && err == nil
	}, 2*time.Second, 10*time.Millisecond)
	log.Infof(ctx, "wtf mate")
	require.Eventually(t, func() bool {
		return metrics.SessionsDeleted.Count() == int64(1)
	}, time.Second, time.Millisecond, metrics.SessionsDeleted.Count())
	require.True(t, metrics.SessionDeletionsRuns.Count() >= 1,
		metrics.SessionDeletionsRuns.Count())

	// Ensure that the update to the session failed.
	found, err := slStorage.Update(ctx, sid, s.Clock().Now())
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, int64(1), metrics.WriteFailures.Count())
}
