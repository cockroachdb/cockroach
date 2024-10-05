// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
)

func benchmarkImportFixture(b *testing.B, gen workload.Generator) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	var bytes int64
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		s, db, _ := serverutils.StartServer(
			b,
			base.TestServerArgs{
				UseDatabase:       `d`,
				SQLMemoryPoolSize: 1 << 30, /* 1GiB */ // default 256MiB might be insufficient
			},
		)
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
