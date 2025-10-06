// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestServerTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
}

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
				s := serverutils.StartServerOnly(b, base.TestServerArgs{
					DefaultTestTenant: tc.tenantOpt,
					Knobs: base.TestingKnobs{
						SQLEvalContext: &eval.TestingKnobs{
							// We disable the randomization of some batch sizes to get consistent
							// results.
							ForceProductionValues: true,
						},
					},
				})
				s.Stopper().Stop(context.Background())
			}
		})
	}
}
