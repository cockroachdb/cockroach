// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/stretchr/testify/assert"
)

func TestRunExplainCombinations(t *testing.T) {
	tests := []struct {
		bundlePath          string
		placeholderToColMap map[int]string
		expectedInputs      [][]string
		expectedOutputs     []string
	}{
		{
			bundlePath: "bundle",
			placeholderToColMap: map[int]string{
				1: "public.a.a",
				2: "public.a.b",
			},
			expectedInputs: [][]string{{"999", "1006"}},
			expectedOutputs: []string{`distribution: local
vectorized: true

• filter
│ estimated row count: 1
│ filter: b = 1006
│
└── • scan
      estimated row count: 1 (0.10% of the table; stats collected 3 days ago)
      table: a@primary
      spans: [/999 - /999]
`},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())
	cliCtx := &clicfg.Context{}
	c := &clisqlcfg.Context{
		CliCtx:  cliCtx,
		ConnCtx: &clisqlclient.Context{CliCtx: cliCtx},
		ExecCtx: &clisqlexec.Context{CliCtx: cliCtx},
	}
	c.LoadDefaults(os.Stdout, os.Stderr)
	pgURL, cleanupFn := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()
	conn := c.ConnCtx.MakeSQLConn(os.Stdout, os.Stdout, pgURL.String())
	for _, test := range tests {
		bundle, err := loadStatementBundle(filepath.Join("testdata/explain-bundle", test.bundlePath))
		assert.NoError(t, err)
		// Disable autostats collection, which will override the injected stats.
		if err := conn.Exec(`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`, nil); err != nil {
			t.Fatal(err)
		}
		var initStmts = [][]byte{bundle.env, bundle.schema}
		initStmts = append(initStmts, bundle.stats...)
		for _, a := range initStmts {
			if err := conn.Exec(string(a), nil); err != nil {
				t.Fatal(err)
			}
		}

		inputs, outputs, err := getExplainCombinations(conn, test.placeholderToColMap, bundle)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedInputs, inputs)
		assert.Equal(t, test.expectedOutputs, outputs)
	}
}
