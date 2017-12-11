// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"bytes"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func backupSourceGetDescFn(
	ctx context.Context, p sql.PlanHookState, backupSource *tree.BackupSourceExpr,
) (*sqlbase.TableDescriptor, *tree.TableName, error) {
	backupDescs, err := loadBackupDescs(ctx, backupSource.From, p.ExecCfg().Settings)
	if err != nil {
		return nil, nil, err
	}
	sqlDescs, _, err := selectTargets(p, backupDescs, backupSource.Targets)
	if err != nil {
		return nil, nil, errors.Wrap(err, "TODO(dan) BEFORE MERGE remove selectTargets")
	}

	var tableDescs []*sqlbase.TableDescriptor
	for _, d := range sqlDescs {
		if t := d.GetTable(); t != nil {
			tableDescs = append(tableDescs, t)
		}
	}

	if len(tableDescs) != 1 {
		return nil, nil, errors.Errorf("EXPERIMENTAL_BACKUP_SOURCE requires exactly one table")
	}
	tableDesc := tableDescs[0]
	tableDesc.BackupSource = &sqlbase.TableDescriptor_BackupSource{
		Uris: backupSource.From,
	}
	tn := &tree.TableName{
		TableName: tree.Name(tableDesc.Name),
		// TODO(dan): BEFORE MERGE Fill in Database.
	}
	return tableDesc, tn, nil
}

func makeBackupSourceKVFetcherFn(
	ctx context.Context, params sqlbase.KVFetcherParams, tableDesc *sqlbase.TableDescriptor,
) (sqlbase.KVFetcher, error) {
	if tableDesc.BackupSource == nil {
		return nil, errors.New("backupSourceKVFetcher requires a BackupSource")
	}
	if params.Reverse {
		return nil, errors.New("backupSourceKVFetcher does not support reverse")
	}

	kvFetcher := &backupSourceKVFetcher{
		params:    params,
		tableDesc: tableDesc,
		spanIdx:   -1,
	}

	for _, uri := range tableDesc.BackupSource.Uris {
		exportStore, err := exportStorageFromURI(ctx, uri, params.Settings)
		if err != nil {
			return nil, err
		}
		kvFetcher.exportStores = append(kvFetcher.exportStores, exportStore)

		backupDesc, err := readBackupDescriptor(ctx, exportStore, BackupDescriptorName)
		if err != nil {
			return nil, err
		}
		backupDesc.Dir = exportStore.Conf()
		kvFetcher.backupDescs = append(kvFetcher.backupDescs, backupDesc)
	}

	// TODO(dan): Avoid loading all the backup descriptors a second time.
	// TODO(dan): We need the same check as RESTORE AS OF SYSTEM TIME for
	// reading under the gc lowwater mark of a full backup.
	if _, err := ValidatePreviousBackups(
		ctx, tableDesc.BackupSource.Uris, params.Settings, params.Spans,
	); err != nil {
		return nil, err
	}

	return kvFetcher, nil
}

type backupSourceKVFetcher struct {
	params    sqlbase.KVFetcherParams
	tableDesc *sqlbase.TableDescriptor
	spanIdx   int

	exportStores []storageccl.ExportStorage
	backupDescs  []BackupDescriptor
	iters        []engine.SimpleIterator
	iter         engine.SimpleIterator
}

var _ sqlbase.KVFetcher = &backupSourceKVFetcher{}

func (f *backupSourceKVFetcher) NextKV(ctx context.Context) (bool, roachpb.KeyValue, error) {
	for {
		if f.iter == nil {
			if ok, err := f.openIter(ctx); err != nil {
				return false, roachpb.KeyValue{}, err
			} else if !ok {
				return false, roachpb.KeyValue{}, nil
			}
		}

		if ok, err := f.iter.Valid(); err != nil {
			return false, roachpb.KeyValue{}, err
		} else if !ok {
			f.iter = nil
			continue
		}

		unsafeKey := f.iter.UnsafeKey()
		if bytes.Compare(unsafeKey.Key, f.params.Spans[f.spanIdx].EndKey) >= 0 {
			f.iter = nil
			continue
		}
		if f.params.Timestamp.Less(unsafeKey.Timestamp) {
			f.iter.Next()
			continue
		}
		unsafeValue := f.iter.UnsafeValue()
		if len(unsafeValue) == 0 {
			// Value is deleted.
			f.iter.NextKey()
			continue
		}

		value := roachpb.Value{
			RawBytes:  append([]byte(nil), unsafeValue...),
			Timestamp: unsafeKey.Timestamp,
		}
		kv := roachpb.KeyValue{Key: append([]byte(nil), unsafeKey.Key...), Value: value}

		f.iter.NextKey()
		return true, kv, nil
	}
}

func (f *backupSourceKVFetcher) openIter(ctx context.Context) (bool, error) {
	f.closeIters()

	for f.spanIdx++; f.spanIdx < len(f.params.Spans); f.spanIdx++ {
		span := f.params.Spans[f.spanIdx]

		for i := range f.backupDescs {
			exportStore, backupDesc := f.exportStores[i], f.backupDescs[i]
			for _, file := range backupDesc.Files {
				if !file.Span.Overlaps(span) {
					continue
				}
				sstReader, err := exportStore.ReadFile(ctx, file.Path)
				if err != nil {
					return false, err
				}
				defer sstReader.Close()
				sstBytes, err := ioutil.ReadAll(sstReader)
				if err != nil {
					return false, err
				}
				sstIter, err := engineccl.NewMemSSTIterator(sstBytes, false /* verify */)
				if err != nil {
					return false, err
				}
				f.iters = append(f.iters, sstIter)
			}
		}
		f.iter = engineccl.MakeMultiIterator(f.iters)
		f.iter.Seek(engine.MVCCKey{Key: span.Key})
		return true, nil
	}
	return false, nil
}

func (f *backupSourceKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	// TODO(dan): BEFORE MERGE figure out what happens if we always return nil.
	return nil
}

func (f *backupSourceKVFetcher) closeIters() {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	for _, iter := range f.iters {
		iter.Close()
	}
	f.iters = f.iters[:0]
}

// TODO(dan): BEFORE MERGE this needs to be called by MultiRowFetcher to avoid
// leaking resources.
func (f *backupSourceKVFetcher) Close() {
	f.closeIters()
	for _, exportStore := range f.exportStores {
		exportStore.Close()
	}
}

func init() {
	sql.BackupSourceGetDesc = backupSourceGetDescFn
	sqlbase.MakeBackupSourceKVFetcher = makeBackupSourceKVFetcherFn
}
