// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbuiltins_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkFingerprintQueryForIndex(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(b, `CREATE TABLE t (k INT PRIMARY KEY, v INT, INDEX v_idx (v))`)
	runner.Exec(b, `INSERT INTO t SELECT i, i % 1000 FROM generate_series(1, 50000) AS g(i)`)
	runner.Exec(b, `ANALYZE t`)

	// Build the fingerprint query for the secondary index.
	var fingerprintQuery string
	require.NoError(b, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		dbDesc, err := col.ByName(txn.KV()).Get().Database(ctx, "defaultdb")
		if err != nil {
			return err
		}
		scDesc, err := col.ByName(txn.KV()).Get().Schema(ctx, dbDesc, "public")
		if err != nil {
			return err
		}
		tableDesc, err := col.ByName(txn.KV()).Get().Table(ctx, dbDesc, scDesc, "t")
		if err != nil {
			return err
		}
		index := tableDesc.PublicNonPrimaryIndexes()[0]
		fingerprintQuery, err = sql.BuildFingerprintQueryForIndex(tableDesc, index, nil /* ignoredColumns */)
		return err
	}))

	b.ResetTimer()
	for b.Loop() {
		runner.Exec(b, fingerprintQuery)
	}
}
