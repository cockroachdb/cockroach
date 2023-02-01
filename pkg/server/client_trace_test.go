// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTraceWithRuntimeTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var args base.TestClusterArgs
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	const fingerprint = `INSERT INTO kv VALUES (_, _), (__more1_10__)`

	db := tc.ServerConn(2)
	for _, stmt := range []string{
		`CREATE TABLE kv (k INT PRIMARY KEY, v INT)`,
		`ALTER TABLE kv SPLIT AT VALUES (1), (2), (3)`,
		`SELECT crdb_internal.request_statement_bundle('` + fingerprint + `', 0::FLOAT, 0::INTERVAL, 0::INTERVAL)`,
		`INSERT INTO kv VALUES (0, 0), (1, 1), (2, 2), (3, 3)`,
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	t.Log(sqlutils.MatrixToStr(sqlutils.MakeSQLRunner(db).QueryStr(t,
		`SELECT * FROM system.statement_diagnostics WHERE statement_fingerprint = $1`, fingerprint,
	)))

}
