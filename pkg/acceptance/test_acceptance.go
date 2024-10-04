// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build acceptance
// +build acceptance

// Acceptance tests are comparatively slow to run, so we use the above build
// tag to separate invocations of `go test` which are intended to run the
// acceptance tests from those which are not. The corollary file to this one
// is test_main.go

package acceptance

import (
	"context"
	"os"
	"os/signal"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func MainTest(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(RunTests(m))
}

// RunTests runs the tests in a package while gracefully handling interrupts.
func RunTests(m *testing.M) int {
	randutil.SeedForTests()

	ctx := context.Background()
	defer cluster.GenerateCerts(ctx)()

	go func() {
		// Shut down tests when interrupted (for example CTRL+C).
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		stopper.Stop(ctx)
	}()
	return m.Run()
}
