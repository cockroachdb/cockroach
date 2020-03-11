// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow_test

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

var (
	// testAllocator is an Allocator with an unlimited budget for use in tests.
	testAllocator *colexec.Allocator

	// testMemMonitor and testMemAcc are a test monitor with an unlimited budget
	// and a memory account bound to it for use in tests.
	testMemMonitor *mon.BytesMonitor
	testMemAcc     *mon.BoundAccount

	// testDiskMonitor and testDiskmAcc are a test monitor with an unlimited budget
	// and a disk account bound to it for use in tests.
	testDiskMonitor *mon.BytesMonitor
	testDiskAcc     *mon.BoundAccount
)

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(func() int {
		ctx := context.Background()
		testMemMonitor = execinfra.NewTestMemMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testMemMonitor.Stop(ctx)
		memAcc := testMemMonitor.MakeBoundAccount()
		testMemAcc = &memAcc
		testAllocator = colexec.NewAllocator(ctx, testMemAcc)
		defer testMemAcc.Close(ctx)

		testDiskMonitor = execinfra.NewTestDiskMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testDiskMonitor.Stop(ctx)
		diskAcc := testDiskMonitor.MakeBoundAccount()
		testDiskAcc = &diskAcc
		defer testDiskAcc.Close(ctx)

		return m.Run()
	}())
}
