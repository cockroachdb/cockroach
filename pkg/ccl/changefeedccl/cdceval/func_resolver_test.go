// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestResolveFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
CREATE FUNCTION yesterday(mvcc DECIMAL) 
RETURNS DECIMAL IMMUTABLE LEAKPROOF LANGUAGE SQL AS $$
  SELECT mvcc - 24 * 3600 * 1e9
$$`)

	testCases := []struct {
		testName       string
		fnName         tree.UnresolvedName
		expectedSchema string
		err            string
	}{
		{
			testName:       "default to use pg_catalog schema",
			fnName:         tree.MakeUnresolvedName("lower"),
			expectedSchema: "pg_catalog",
		},
		{
			testName:       "explicit to use pg_catalog schema",
			fnName:         tree.MakeUnresolvedName("pg_catalog", "lower"),
			expectedSchema: "pg_catalog",
		},
		{
			testName: "explicit to use pg_catalog schema but cdc name",
			fnName:   tree.MakeUnresolvedName("pg_catalog", "cdc_prev"),
			err:      "unknown function: pg_catalog.cdc_prev()",
		},
		{
			testName:       "cdc name without schema",
			fnName:         tree.MakeUnresolvedName("changefeed_creation_timestamp"),
			expectedSchema: "public",
		},
		{
			testName:       "uppercase cdc name without schema",
			fnName:         tree.MakeUnresolvedName("changefeed_creATIon_TimeStamp"),
			expectedSchema: "public",
		},
		{
			testName:       "udf",
			fnName:         tree.MakeUnresolvedName("yesterday"),
			expectedSchema: "public",
		},
	}

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			var funcDef *tree.ResolvedFunctionDefinition
			err := withPlanner(context.Background(), &execCfg, hlc.Timestamp{},
				username.RootUserName(), s.Clock().Now(), defaultDBSessionData,
				func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) (err error) {
					defer cleanup()
					semaCtx := execCtx.SemaCtx()
					r := newCDCFunctionResolver(semaCtx.FunctionResolver)
					funcDef, err = r.ResolveFunction(
						context.Background(),
						tree.MakeUnresolvedFunctionName(&tc.fnName),
						semaCtx.SearchPath,
					)
					return err
				})

			if tc.err != "" {
				require.Regexp(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			for _, o := range funcDef.Overloads {
				require.Equal(t, tc.expectedSchema, o.Schema)
			}
		})
	}
}
