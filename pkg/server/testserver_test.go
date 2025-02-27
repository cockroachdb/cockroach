// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		args := base.TestServerArgs{
			DefaultTestTenant: tc.tenantOpt,
		}
		b.Run(tc.name, func(b *testing.B) {
			testutils.RunTrueAndFalse(b, "slim", func(b *testing.B, useSlimServer bool) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var s serverutils.TestServerInterface
					if useSlimServer {
						s = serverutils.StartSlimServerOnly(b, args)
					} else {
						s = serverutils.StartServerOnly(b, args)
					}
					s.Stopper().Stop(context.Background())
				}
			})
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
		}
		t.Run(tc.name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "slim", func(t *testing.T, useSlimServer bool) {
				var s serverutils.TestServerInterface
				if useSlimServer {
					s = serverutils.StartSlimServerOnly(t, args)
				} else {
					s = serverutils.StartServerOnly(t, args)
				}
				s.Stopper().Stop(context.Background())
			})
		})
	}
}

func TestServerProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("default", func(t *testing.T) {
		s, sql, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())

		sqlDB := sqlutils.MakeSQLRunner(sql)

		// Ensure all ranges have 1 replica
		sqlDB.CheckQueryResults(t, "select count(*) from [show cluster ranges] where replicas = '{1}'", sqlDB.QueryStr(t, "select count(*) from [show cluster ranges]"))
	})

	t.Run("slim", func(t *testing.T) {
		s, sql, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())

		// Ensure there isn't a metrics poller job, as the permanent upgrade that
		// creates it is skipped in slim mode.
		sqlDB := sqlutils.MakeSQLRunner(sql)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM system.jobs WHERE id = 101", [][]string{{"0"}})

		// Ensure no span config job has spun up
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM system.jobs WHERE job_type = 'AUTO SPAN CONFIG RECONCILIATION'", [][]string{{"0"}})
	})
}
