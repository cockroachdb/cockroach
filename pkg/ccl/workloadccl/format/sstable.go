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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
)

// ToTableDescriptor returns the corresponding TableDescriptor for a workload
// Table.
func ToTableDescriptor(
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
	const parentID descpb.ID = keys.MaxReservedDescID
	testSettings := cluster.MakeTestingClusterSettings()
	tableDesc, err := importccl.MakeTestingSimpleTableDescriptor(
		ctx, &semaCtx, testSettings, createTable, parentID, keys.PublicSchemaID, tableID, importccl.NoFKs, ts.UnixNano())
	if err != nil {
		return nil, err
	}
	return tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil
}

type sortableKVs []roachpb.KeyValue

func (s sortableKVs) Len() int           { return len(s) }
func (s sortableKVs) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sortableKVs) Less(i, j int) bool { return s[i].Key.Compare(s[j].Key) == -1 }

// ToSSTable constructs a single sstable with the kvs necessary to represent a
// workload.Table as a CockroachDB SQL table. This sstable is suitable for
// handing to AddSSTable or RocksDB's IngestExternalFile.
//
// TODO(dan): Finally remove sampledataccl in favor of this.
func ToSSTable(t workload.Table, tableID descpb.ID, ts time.Time) ([]byte, error) {
	ctx := context.Background()
	tableDesc, err := ToTableDescriptor(t, tableID, ts)
	if err != nil {
		return nil, err
	}

	kvCh := make(chan row.KVBatch)
	wc := importccl.NewWorkloadKVConverter(
		0, tableDesc, t.InitialRows, 0, t.InitialRows.NumBatches, kvCh)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(kvCh)
		evalCtx := &tree.EvalContext{
			SessionData: &sessiondata.SessionData{},
			Codec:       keys.SystemSQLCodec,
		}
		return wc.Worker(ctx, evalCtx)
	})
	var sst []byte
	var kvs sortableKVs
	g.GoCtx(func(ctx context.Context) error {
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				// Workload produces KVs out of order, so we buffer and sort
				// them all here. We shouldn't be concerned with the performance
				// of this
				kvs = append(kvs, kv)
			}
		}
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sstTS := hlc.Timestamp{WallTime: ts.UnixNano()}
	sstFile := &storage.MemFile{}
	sw := storage.MakeIngestionSSTWriter(sstFile)
	defer sw.Close()

	sort.Sort(kvs)
	for _, kv := range kvs {
		mvccKey := storage.MVCCKey{Timestamp: sstTS, Key: kv.Key}
		if err := sw.PutMVCC(mvccKey, kv.Value.RawBytes); err != nil {
			return nil, err
		}
	}

	err = sw.Finish()
	sst = sstFile.Data()

	return sst, nil
}
