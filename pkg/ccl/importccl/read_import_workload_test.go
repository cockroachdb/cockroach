// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	_ "github.com/cockroachdb/cockroach/pkg/workload/bulkingest"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

type mockAdder struct {
	count, dataSize int64
}

func (m *mockAdder) Add(_ context.Context, k roachpb.Key, v []byte) error {
	kv := roachpb.KeyValue{Key: k, Value: roachpb.Value{RawBytes: v}}
	m.count++
	m.dataSize += int64(kv.Size())
	return nil
}
func (mockAdder) Close()                            { panic("unimplemented") }
func (mockAdder) Reset() error                      { return nil }
func (mockAdder) Flush(_ context.Context) error     { return nil }
func (mockAdder) GetSummary() roachpb.BulkOpSummary { panic("unimplemented") }

var _ storagebase.BulkAdder = &mockAdder{}

func BenchmarkWorkloadTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()

	runBenchmarkWorkloadGen(b, "order_line",
		`CREATE TABLE order_line (
			ol_o_id         integer   not null,
			ol_d_id         integer   not null,
			ol_w_id         integer   not null,
			ol_number       integer   not null,
			ol_i_id         integer   not null,
			ol_supply_w_id  integer,
			ol_delivery_d   timestamp,
			ol_quantity     integer,
			ol_amount       decimal(6,2),
			ol_dist_info    char(24),
			primary key (ol_w_id, ol_d_id, ol_o_id DESC, ol_number),
			index order_line_fk (ol_supply_w_id, ol_i_id)
		)`,
		"experimental-workload:///csv/tpcc/order_line?interleaved=false&row-end=%d&row-start=0&seed=1&version=2.0.1&warehouses=10",
	)
}
func BenchmarkWorkloadBulkingest(b *testing.B) {
	defer leaktest.AfterTest(b)()

	runBenchmarkWorkloadGen(b, "bulkingest",
		`CREATE TABLE bulkingest (
			a INT,
			b INT,
			c INT,
			payload STRING,
			PRIMARY KEY (a, b, c))`,
		"experimental-workload:///csv/bulkingest/bulkingest?row-end=%d&row-start=0&seed=1&version=1.0.0",
	)
}

func runBenchmarkWorkloadGen(b *testing.B, name, schema, uriPattern string) {
	ctx := context.TODO()
	st := cluster.MakeTestingClusterSettings()

	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	singleTable := makeTable(ctx, b, st, schema)

	format := roachpb.IOFileFormat{}
	progFn := func(pct float32) error { return nil }
	b.Run(name, func(b *testing.B) {
		kvCh := make(chan kvBatch, 10)
		conv, err := newWorkloadReader(kvCh, singleTable, evalCtx)
		if err != nil {
			b.Fatal(err)
		}
		adder := mockAdder{}

		uri := fmt.Sprintf(uriPattern, b.N)
		uris := map[int32]string{1: uri}

		group := ctxgroup.WithContext(ctx)
		group.GoCtx(func(ctx context.Context) error {
			defer conv.inputFinished(ctx)
			return conv.readFiles(ctx, uris, format, progFn, st)
		})
		group.GoCtx(func(ctx context.Context) error { return ingestKvs(ctx, &adder, kvCh) })
		if err := group.Wait(); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(adder.dataSize)
		b.Logf("produced %d kvs / %db", adder.count, adder.dataSize)
	})
}

func makeTable(
	ctx context.Context, t testing.TB, st *cluster.Settings, stmt string,
) *sqlbase.TableDescriptor {
	t.Helper()
	parsed, err := parser.ParseOne(stmt)
	if err != nil {
		t.Fatal(err)
	}
	create, ok := parsed.AST.(*tree.CreateTable)
	if !ok {
		t.Fatal("expected CREATE TABLE statement in table file")
	}
	tbl, err := MakeSimpleTableDescriptor(ctx, st, create, defaultCSVParentID, defaultCSVTableID, NoFKs, 0)
	if err != nil {
		t.Fatal(err)
	}
	return tbl.TableDesc()
}
