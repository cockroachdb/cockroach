// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package diagnosticsccl_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	kvtenantccl.InitConnectorFactory()
	defer ccl.TestingEnableEnterprise()()
	os.Exit(m.Run())
}
