// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workloadccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
)

func benchmarkImportFixture(b *testing.B, gen workload.Generator) {
	ctx := context.Background()

	var bytes int64
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		s, db, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: `d`})
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(b, `CREATE DATABASE d`)

		b.StartTimer()
		const filesPerNode = 1
		const noInjectStats, csvServer = false, ``
		importBytes, err := workloadccl.ImportFixture(
			ctx, db, gen, `d`, filesPerNode, noInjectStats, csvServer,
		)
		require.NoError(b, err)
		bytes += importBytes
		b.StopTimer()

		s.Stopper().Stop(ctx)
	}
	b.SetBytes(bytes / int64(b.N))
}

func BenchmarkImportFixture(b *testing.B) {
	skip.UnderShort(b, "skipping long benchmark")

	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkImportFixture(b, tpcc.FromWarehouses(1))
	})
}
