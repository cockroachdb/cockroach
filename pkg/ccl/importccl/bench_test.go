// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/format"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

//BenchmarkConvertToKVs/tpcc/warehouses=1-8         1        3558824936 ns/op          22.46 MB/s
func BenchmarkConvertToKVs(b *testing.B) {
	skip.UnderShort(b, "skipping long benchmark")

	tpccGen := tpcc.FromWarehouses(1)
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkConvertToKVs(b, tpccGen)
	})
}

func benchmarkConvertToKVs(b *testing.B, g workload.Generator) {
	ctx := context.Background()
	tableID := descpb.ID(bootstrap.TestingUserDescID(0))
	ts := timeutil.Now()

	var bytes int64
	b.ResetTimer()
	for _, t := range g.Tables() {
		tableDesc, err := format.ToTableDescriptor(t, tableID, ts)
		if err != nil {
			b.Fatal(err)
		}

		kvCh := make(chan row.KVBatch)
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			defer close(kvCh)
			wc := importccl.NewWorkloadKVConverter(
				0, tableDesc, t.InitialRows, 0, t.InitialRows.NumBatches, kvCh)
			evalCtx := &tree.EvalContext{
				SessionDataStack: sessiondata.NewStack(&sessiondata.SessionData{}),
				Codec:            keys.SystemSQLCodec,
			}
			semaCtx := tree.MakeSemaContext()
			return wc.Worker(ctx, evalCtx, &semaCtx)
		})
		for kvBatch := range kvCh {
			for i := range kvBatch.KVs {
				kv := &kvBatch.KVs[i]
				bytes += int64(len(kv.Key) + len(kv.Value.RawBytes))
			}
		}
		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.SetBytes(bytes)
}
