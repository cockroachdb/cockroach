// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkEmptySelectSingleNode(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	benchmarkEmptySelect(b, 1)
}

func BenchmarkEmptySelectMultiNode(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	benchmarkEmptySelect(b, 3)
}

func benchmarkEmptySelect(b *testing.B, n int) {
	ctx := context.Background()

	tc := testcluster.StartTestCluster(b, n, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	_, err := db.Exec(`CREATE TABLE kv (k INT PRIMARY KEY, v INT)`)
	require.NoError(b, err)
	stmts := make([]*sql.Stmt, n)
	for i := range stmts {
		var err error
		stmts[i], err = db.Prepare(`SELECT * FROM kv`)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmts[i%n].Exec() // poor man's round robin
		require.NoError(b, err)
	}
}
