// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package format

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/bulk"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

// ToTableDescriptor returns the corresponding TableDescriptor for a workload
// Table.
func ToTableDescriptor(
	t workload.Table, tableID sqlbase.ID, ts time.Time,
) (*sqlbase.TableDescriptor, error) {
	ctx := context.Background()
	stmt, err := parser.ParseOne(fmt.Sprintf(`CREATE TABLE "%s" %s`, t.Name, t.Schema))
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	const parentID sqlbase.ID = keys.MaxReservedDescID
	tableDesc, err := importccl.MakeSimpleTableDescriptor(
		ctx, nil /* settings */, createTable, parentID, tableID, importccl.NoFKs, ts.UnixNano())
	if err != nil {
		return nil, err
	}
	return &tableDesc.TableDescriptor, nil
}

// ToSSTable constructs a single sstable with the kvs necessary to represent a
// workload.Table as a CockroachDB SQL table. This sstable is suitable for
// handing to AddSSTable or RocksDB's IngestExternalFile.
//
// TODO(dan): Finally remove sampledataccl in favor of this.
func ToSSTable(t workload.Table, tableID sqlbase.ID, ts time.Time) ([]byte, error) {
	ctx := context.Background()
	tableDesc, err := ToTableDescriptor(t, tableID, ts)
	if err != nil {
		return nil, err
	}

	kvCh := make(chan row.KVBatch)
	wc := importccl.NewWorkloadKVConverter(
		0, tableDesc, t.InitialRows, 0, t.InitialRows.NumBatches, kvCh)

	var ssts addSSTableSender
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(kvCh)
		evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{}}
		return wc.Worker(ctx, evalCtx)
	})
	g.GoCtx(func(ctx context.Context) error {
		sstTS := hlc.Timestamp{WallTime: ts.UnixNano()}
		const sstSize = math.MaxUint64
		ba, err := bulk.MakeBulkAdder(
			ctx, &ssts, nil /* rangeCache */, sstTS, storagebase.BulkAdderOptions{SSTSize: sstSize, MinBufferSize: sstSize}, nil, /* bulkMon */
		)
		if err != nil {
			return err
		}
		defer ba.Close(ctx)
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				if err := ba.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
					return err
				}
			}
		}
		return ba.Flush(ctx)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if len(ssts) != 1 {
		return nil, errors.Errorf(`expected exactly 1 sst but got %d`, len(ssts))
	}
	return ssts[0], nil
}

type addSSTableSender [][]byte

func (s *addSSTableSender) AddSSTable(
	_ context.Context, _, _ interface{}, data []byte, _ bool, _ *enginepb.MVCCStats,
) error {
	*s = append(*s, data)
	return nil
}

func (s *addSSTableSender) SplitAndScatter(
	_ context.Context, _ roachpb.Key, _ hlc.Timestamp,
) error {
	return nil
}
