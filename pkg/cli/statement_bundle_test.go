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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestRunExplainCombinations(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			expectedInputs: [][]string{{"999", "8"}},
			expectedOutputs: []string{`select
 ├── scan a
 │    └── constraint: /1: [/999 - /999]
 └── filters
      └── b = 8
`},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
	cliCtx := &clicfg.Context{}
	c := &clisqlcfg.Context{
		CliCtx:  cliCtx,
		ConnCtx: &clisqlclient.Context{CliCtx: cliCtx},
		ExecCtx: &clisqlexec.Context{CliCtx: cliCtx},
	}
	c.LoadDefaults(os.Stdout, os.Stderr)
	pgURL, cleanupFn := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	ctx := context.Background()

	conn := c.ConnCtx.MakeSQLConn(os.Stdout, os.Stdout, pgURL.String())
	for _, test := range tests {
		bundle, err := loadStatementBundle(testutils.TestDataPath(t, "explain-bundle", test.bundlePath))
		assert.NoError(t, err)
		// Disable autostats collection, which will override the injected stats.
		if err := conn.Exec(ctx, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`); err != nil {
			t.Fatal(err)
		}
		var initStmts = [][]byte{bundle.env, bundle.schema}
		initStmts = append(initStmts, bundle.stats...)
		for _, a := range initStmts {
			if err := conn.Exec(ctx, string(a)); err != nil {
				t.Fatal(err)
			}
		}

		inputs, outputs, err := getExplainCombinations(conn, "EXPLAIN(OPT)", test.placeholderToColMap, bundle)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedInputs, inputs)
		assert.Equal(t, test.expectedOutputs, outputs)
	}
}
