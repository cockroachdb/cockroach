// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
)

// BenchmarkConvertToKVs/tpcc/warehouses=1-8         1        3558824936 ns/op          22.46 MB/s
func BenchmarkConvertToKVs(b *testing.B) {
	skip.UnderShort(b, "skipping long benchmark")

	tpccGen := tpcc.FromWarehouses(1)
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkConvertToKVs(b, tpccGen)
	})
}

func toTableDescriptor(
	t workload.Table, tableID descpb.ID, ts time.Time,
) (catalog.TableDescriptor, error) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	stmt, err := parser.ParseOne(fmt.Sprintf(`CREATE TABLE "%s" %s`, t.Name, t.Schema))
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	// We need to assign a valid parent database ID to the table descriptor, but
	// the value itself doesn't matter, so we arbitrarily pick the system database
	// ID because we know it's valid.
	parentID := descpb.ID(keys.SystemDatabaseID)
	testSettings := cluster.MakeTestingClusterSettings()
	tableDesc, err := importer.MakeTestingSimpleTableDescriptor(
		ctx, &semaCtx, testSettings, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, ts.UnixNano())
	if err != nil {
		return nil, err
	}
	return tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil
}

func benchmarkConvertToKVs(b *testing.B, g workload.Generator) {
	ctx := context.Background()
	tableID := descpb.ID(bootstrap.TestingUserDescID(0))
	ts := timeutil.Now()

	var bytes int64
	b.ResetTimer()
	for _, t := range g.Tables() {
		tableDesc, err := toTableDescriptor(t, tableID, ts)
		if err != nil {
			b.Fatal(err)
		}

		kvCh := make(chan row.KVBatch)
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			defer close(kvCh)
			wc := importer.NewWorkloadKVConverter(
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
