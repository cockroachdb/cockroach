// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkGenerateObjects(b *testing.B) {
	defer log.Scope(b).Close(b)

	benches := []struct {
		name   string
		counts string
		maxN   int
	}{
		{name: "1000 tables", counts: `[1,1,1000]`, maxN: 100},
		{name: "10x100 tables", counts: `[1,10,100]`, maxN: 100},
		{name: "10x10x10 tables", counts: `[10,10,10]`, maxN: 100},
		{name: "10000 tables", counts: `[1,1,10000]`, maxN: 10},
	}

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{
		// Secondary tenants have unreasonably low span config limits. We
		// can't use them yet for this test.
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(83461),
	})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	// Disable auto stats and range splits, which introduce noise.
	db.Exec(b, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	db.Exec(b, `SET CLUSTER SETTING spanconfig.range_coalescing.system.enabled = true`)

	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			// The benchmark framework increases b.N automatically to make
			// it "fill" the allocated time. However, test servers really
			// don't like when there are hundreds of thousands of tables
			// created. So we cap the number of iterations. This does not
			// decrease the quality of the benchmark though, since go will
			// still repeat the bench call to "fill" the allocated time.
			if b.N > bench.maxN {
				//lint:ignore SA3001 should not assign to b.N
				b.N = bench.maxN
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Exec(b,
					`SELECT crdb_internal.generate_test_objects(('{"names":"foo.bar.baz","counts":'||$1||'}')::JSONB)`,
					bench.counts)
			}
			b.StopTimer()
		})
	}
}
