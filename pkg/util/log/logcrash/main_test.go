// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logcrash_test

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	ctx := context.Background()

	// MakeTestingClusterSettings initializes log.ReportingSettings to this
	// instance of setting values.
	// TODO(knz): This comment appears to be untrue.
	st := cluster.MakeTestingClusterSettings()
	logcrash.DiagnosticsReportingEnabled.Override(ctx, &st.SV, false)
	logcrash.CrashReports.Override(ctx, &st.SV, false)

	os.Exit(m.Run())
}
