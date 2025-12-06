// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestFingerprinter(t *testing.T) {
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	rows, err := s.SQLConn(t).Query("SELECT * FROM system.statement_fingerprints")
	require.NoError(t, err)
	require.True(t, rows.Next())
	// get the rows, print out the query field of each
	queries := []string{}
	for rows.Next() {
		var (
			id           int64
			fingerprint  string
			query        string
			summary      string
			implicitTxn  bool
			databaseName string
			createdAt    string
		)
		err := rows.Scan(&id, &fingerprint, &query, &summary, &implicitTxn, &databaseName, &createdAt)
		require.NoError(t, err)
		queries = append(queries, fingerprint)
	}
	fmt.Println(queries)
}
