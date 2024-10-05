// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
)

// ResetTest sets up the test environment. In particular, it embeds the
// EmbeddedCertsDir folder and makes the tls package load from there.
func ResetTest() {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
}

func TestMain(m *testing.M) {
	ResetTest()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go
