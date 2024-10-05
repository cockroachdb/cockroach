// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reducesql_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

func TestMain(m *testing.M) {
	serverutils.InitTestServerFactory(server.TestServerFactory)
	code := m.Run()
	os.Exit(code)
}
