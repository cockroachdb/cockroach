// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestMain(m *testing.M) {
	start := timeutil.Now()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory,
		serverutils.WithDRPCOption(base.TestDRPCEnabledRandomly))
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	exit := m.Run()
	testcluster.PrintTimings(timeutil.Since(start))
	os.Exit(exit)
}

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go
