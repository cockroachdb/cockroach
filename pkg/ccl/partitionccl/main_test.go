// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	// TODO(andrei): The common thing to do is to call
	// ccl.TestingEnableEnterprise() instead of the utilccl version. That requires
	// this file to move to the partitionccl_test package. Doing that causes one
	// of the tests to panic. I haven't investigated why.
	//defer utilccl.TestingEnableEnterpriseTricky()()
	defer ccl.TestingEnableEnterprise()()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go
