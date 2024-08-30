// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package license_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGracePeriodInitTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is the timestamp that we'll override the grace period init timestamp with.
	// This will be set when bringing up the server.
	ts1 := timeutil.Unix(1724329716, 0)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				LicenseTestingKnobs: license.TestingKnobs{
					EnableGracePeriodInitTSWrite: true,
					OverrideStartTime:            &ts1,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Create a new enforcer, to test that it won't overwrite the grace period init
	// timestamp that was already setup.
	enforcer := &license.Enforcer{}
	ts2 := ts1.Add(1)
	enforcer.TestingKnobs = &license.TestingKnobs{
		EnableGracePeriodInitTSWrite: true,
		OverrideStartTime:            &ts2,
	}
	// Ensure request for the grace period init ts1 before start just returns the start
	// time used when the enforcer was created.
	require.Equal(t, ts2, enforcer.GetGracePeriodInitTS())
	// Start the enforcer to read the timestamp from the KV.
	err := enforcer.Start(ctx, srv.SystemLayer().InternalDB().(descs.DB))
	require.NoError(t, err)
	require.Equal(t, ts1, enforcer.GetGracePeriodInitTS())

	// Access the enforcer that is cached in the executor config to make sure they
	// work for the system tenant and secondary tenant.
	require.Equal(t, ts1, srv.SystemLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetGracePeriodInitTS())
	require.Equal(t, ts1, srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).LicenseEnforcer.GetGracePeriodInitTS())
}
