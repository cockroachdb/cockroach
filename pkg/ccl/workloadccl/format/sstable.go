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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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

	kvCh := make(chan []roachpb.KeyValue)
	var kvs []roachpb.KeyValue
	wc := importccl.NewWorkloadKVConverter(
		tableDesc, t.InitialRows, 0, t.InitialRows.NumBatches, kvCh)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(kvCh)
		evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{}}
		finishedBatchFn := func() {}
		return wc.Worker(ctx, evalCtx, finishedBatchFn)
	})
	g.GoCtx(func(ctx context.Context) error {
		for kvBatch := range kvCh {
			kvs = append(kvs, kvBatch...)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key.Compare(kvs[j].Key) < 0 })
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, err
	}
	defer sst.Close()
	for _, kv := range kvs {
		if err := sst.Add(engine.MVCCKeyValue{
			Key:   engine.MVCCKey{Key: kv.Key, Timestamp: hlc.Timestamp{WallTime: ts.UnixNano()}},
			Value: kv.Value.RawBytes,
		}); err != nil {
			return nil, err
		}
	}
	return sst.Finish()
}
