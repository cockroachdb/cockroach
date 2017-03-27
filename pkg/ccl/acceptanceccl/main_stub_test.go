// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

// +build !acceptance

// Our Docker-based acceptance tests are comparatively slow to run, so we use
// the above build tag to separate invocations of `go test` which are intended
// to run the acceptance tests from those which are not. The corollary file to
// this one is main_test.go. Acceptance tests against remote clusters (which
// aren't run unless `-remote` is passed explicitly) are run through this file
// via the standard facilities (i.e. `make test` and without `acceptance` build
// tag).

package acceptanceccl

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	if !acceptance.IsRemote() {
		log.Infof(context.Background(), "not running with `acceptance` build tag or against remote cluster; skipping")
		return
	}
	security.SetReadFileFn(securitytest.Asset)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	acceptance.RunTests(m)
}
