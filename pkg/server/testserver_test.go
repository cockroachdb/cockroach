// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkTestServerStartup(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	testCases := []struct {
		name      string
		tenantOpt base.DefaultTestTenantOptions
	}{
		{"SharedTenant", base.SharedTestTenantAlwaysEnabled},
		{"ExternalTenant", base.ExternalTestTenantAlwaysEnabled},
		{"SystemTenantOnly", base.TestControlsTenantsExplicitly},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {

				args := base.TestServerArgs{
					DefaultTestTenant: tc.tenantOpt,
					Settings:          cluster.MakeTestingClusterSettings(),
					Knobs: base.TestingKnobs{
						SQLEvalContext: &eval.TestingKnobs{
							// We disable the randomization of some batch sizes to get consistent
							// results.
							ForceProductionValues: true,
						},
					},
				}
				// Disable leader fortification as it currently causes large variability in runtime.
				kvserver.RaftLeaderFortificationFractionEnabled.Override(context.Background(), &args.Settings.SV, 0.0)

				s := serverutils.StartServerOnly(b, args)
				s.Stopper().Stop(context.Background())
			}
		})
	}
}

func TestServerStartup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name      string
		tenantOpt base.DefaultTestTenantOptions
	}{
		{"SharedTenant", base.SharedTestTenantAlwaysEnabled},
		{"ExternalTenant", base.ExternalTestTenantAlwaysEnabled},
		{"SystemTenantOnly", base.TestControlsTenantsExplicitly},
	}

	for _, tc := range testCases {

		args := base.TestServerArgs{
			DefaultTestTenant: tc.tenantOpt,
			Settings:          cluster.MakeTestingClusterSettings(),
		}
		// Disable leader fortification as it currently causes large variability in runtime.
		kvserver.RaftLeaderFortificationFractionEnabled.Override(context.Background(), &args.Settings.SV, 0.0)

		t.Run(tc.name, func(t *testing.T) {
			s := serverutils.StartServerOnly(t, args)
			s.Stopper().Stop(context.Background())
		})
	}
}

func TestServerProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sql, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(sql)

	// Ensure all ranges have 1 replica
	sqlDB.CheckQueryResults(t, "select count(*) from [show cluster ranges] where replicas = '{1}'", sqlDB.QueryStr(t, "select count(*) from [show cluster ranges]"))
}
