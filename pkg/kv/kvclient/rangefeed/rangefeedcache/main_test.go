// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeedcache_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

func init() {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
}

func TestMain(m *testing.M) {
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	kvtenant.InitTestConnectorFactory()
	os.Exit(m.Run())
}

//go:generate ../../../../util/leaktest/add-leaktest.sh *_test.go
