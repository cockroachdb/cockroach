// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package instancestorage_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	// We care about running all of these tests both in the "regular" config
	// and in the "COCKROACH_MR_SYSTEM_DATABASE" config. This is certainly
	// hacky, but go supports it, begrudgingly (see golang.org/issues/23129).
	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
	defer envutil.TestSetEnv(
		fakeTestingTB{}, "COCKROACH_MR_SYSTEM_DATABASE", "t",
	)()
	os.Exit(m.Run())
}

//go:generate ../../../util/leaktest/add-leaktest.sh *_test.go

// fakeTestingTB is used to access envutil.TestSetEnv in TestMain.
type fakeTestingTB struct{ testing.TB }

func (fakeTestingTB) Helper() {}
