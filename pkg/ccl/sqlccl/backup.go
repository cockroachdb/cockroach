// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	"io/ioutil"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// BackupDescriptorName is the file name used for serialized
	// BackupDescriptor protos.
	BackupDescriptorName = "BACKUP"
)

// exportStorageFromURI returns an ExportStorage for the given URI.
// The returned ExportStorage
func exportStorageFromURI(ctx context.Context, uri string) (storageccl.ExportStorage, error) {
	conf, err := storageccl.ExportStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	// TODO(dt): Use a tempdir in cockroach-data if at all possible.
	// In storage code , we get a cockroach-specific tempdir from the Store, but
	// here in sql, we don't have a Store -- indeed, the gateway node might not be
	// configured with any stores at all. "" falls back to sys default (eg. /tmp).
	// Consider hacking/plumbing a default tempdir or something.
	tmpDir := ""
	return storageccl.MakeExportStorage(ctx, conf, tmpDir)
}

// ReadBackupDescriptor reads and unmarshals a BackupDescriptor from given base.
func ReadBackupDescriptor(
	ctx context.Context, dir storageccl.ExportStorage,
) (BackupDescriptor, error) {
	r, err := dir.ReadFile(ctx, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer r.Close()
	descBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return BackupDescriptor{}, err
	}
	var backupDesc BackupDescriptor
	if err := backupDesc.Unmarshal(descBytes); err != nil {
		return BackupDescriptor{}, err
	}
	// TODO(dan): Sanity check this BackupDescriptor: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return backupDesc, nil
}

// ValidatePreviousBackups checks that the timestamps of previous backups are
// consistent. The most recently backed-up time is returned.
func ValidatePreviousBackups(ctx context.Context, uris []string) (hlc.Timestamp, error) {
	if len(uris) == 0 || len(uris) == 1 && uris[0] == "" {
		// Full backup.
		return hlc.Timestamp{}, nil
	}
	backups := make([]BackupDescriptor, len(uris))
	for i, uri := range uris {
		dir, err := exportStorageFromURI(ctx, uri)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		backups[i], err = ReadBackupDescriptor(ctx, dir)
		if err != nil {
			return hlc.Timestamp{}, err
		}
	}

	// This reuses Restore's logic for lining up all the start and end
	// timestamps to validate the previous backups that this one is incremental
	// from.
	_, endTime, err := makeImportRequests(nil, backups)
	return endTime, err
}

func allSQLDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.Descriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}

	sqlDescs := make([]sqlbase.Descriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&sqlDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
	}
	return sqlDescs, nil
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(tables []*sqlbase.TableDescriptor) []roachpb.Span {
	sstIntervalTree := interval.Tree{Overlapper: interval.Range.OverlapExclusive}
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			startKey := roachpb.Key(sqlbase.MakeIndexKeyPrefix(table, index.ID))
			ie := intervalSpan(roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()})
			// Errors are only returned if end <= start, which is never the case
			// here because we use `PrefixEnd` to compute end.
			_ = sstIntervalTree.Insert(ie, false)
		}
	}

	var spans []roachpb.Span
	_ = sstIntervalTree.Do(func(r interval.Interface) bool {
		spans = append(spans, roachpb.Span{
			Key:    roachpb.Key(r.Range().Start),
			EndKey: roachpb.Key(r.Range().End),
		})
		return false
	})
	return spans
}

type backupFileDescriptors []BackupDescriptor_File

func (r backupFileDescriptors) Len() int      { return len(r) }
func (r backupFileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r backupFileDescriptors) Less(i, j int) bool {
	if cmp := bytes.Compare(r[i].Span.Key, r[j].Span.Key); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r[i].Span.EndKey, r[j].Span.EndKey) < 0
}

// Backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
// - <dir>/<unique_int>.sst
// - <dir> is given by the user and may be cloud storage
// - Each file contains data for a key range that doesn't overlap with any other
//   file.
func Backup(
	ctx context.Context,
	p sql.PlanHookState,
	uri string,
	targets parser.TargetList,
	startTime, endTime hlc.Timestamp,
	_ parser.KVOptions,
) (BackupDescriptor, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var sqlDescs []sqlbase.Descriptor

	exportStore, err := exportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer exportStore.Close()

	db := p.ExecCfg().DB

	{
		txn := client.NewTxn(db)
		opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
		err := txn.Exec(ctx, opt, func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			sql.SetTxnTimestamps(txn, endTime)
			sqlDescs, err = allSQLDescriptors(ctx, txn)
			return err
		})
		if err != nil {
			return BackupDescriptor{}, err
		}
	}

	// TODO(dan): Plumb the session database down.
	sessionDatabase := ""
	if sqlDescs, err = descriptorsMatchingTargets(sessionDatabase, sqlDescs, targets); err != nil {
		return BackupDescriptor{}, err
	}

	for _, desc := range sqlDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if err := p.CheckPrivilege(dbDesc, privilege.SELECT); err != nil {
				return BackupDescriptor{}, err
			}
		}
	}

	// Backup users, descriptors, and the entire keyspace for user data.
	tables := []*sqlbase.TableDescriptor{&sqlbase.DescriptorTable, &sqlbase.UsersTable}
	for _, desc := range sqlDescs {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			tables = append(tables, tableDesc)
		}
	}

	for _, desc := range tables {
		if err := p.CheckPrivilege(desc, privilege.SELECT); err != nil {
			return BackupDescriptor{}, err
		}
	}

	spans := spansForAllTableIndexes(tables)

	mu := struct {
		syncutil.Mutex
		files    []BackupDescriptor_File
		dataSize int64
	}{}

	header := roachpb.Header{Timestamp: endTime}
	storageConf := exportStore.Conf()
	g, gCtx := errgroup.WithContext(ctx)
	for i := range spans {
		span := spans[i]
		g.Go(func() error {
			req := &roachpb.ExportRequest{
				Span:      span,
				Storage:   storageConf,
				StartTime: startTime,
			}
			res, pErr := client.SendWrappedWith(gCtx, db.GetSender(), header, req)
			if pErr != nil {
				return pErr.GoError()
			}
			mu.Lock()
			defer mu.Unlock()
			for _, file := range res.(*roachpb.ExportResponse).Files {
				mu.files = append(mu.files, BackupDescriptor_File{
					Span:   file.Span,
					Path:   file.Path,
					Sha512: file.Sha512,
				})
				mu.dataSize += file.DataSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return BackupDescriptor{}, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}
	files, dataSize := mu.files, mu.dataSize // No more concurrency, so this is safe.

	desc := BackupDescriptor{
		StartTime:   startTime,
		EndTime:     endTime,
		Descriptors: sqlDescs,
		Spans:       spans,
		Files:       files,
		DataSize:    dataSize,
	}
	sort.Sort(backupFileDescriptors(desc.Files))

	descBuf, err := desc.Marshal()
	if err != nil {
		return BackupDescriptor{}, err
	}
	writer, err := exportStore.PutFile(ctx, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer writer.Cleanup()
	if err = ioutil.WriteFile(writer.LocalFile(), descBuf, 0600); err != nil {
		return BackupDescriptor{}, err
	}
	if err := writer.Finish(); err != nil {
		return BackupDescriptor{}, err
	}

	return desc, nil
}

func backupPlanHook(
	baseCtx context.Context, stmt parser.Statement, p sql.PlanHookState,
) (func() ([]parser.Datums, error), sql.ResultColumns, error) {
	backup, ok := stmt.(*parser.Backup)
	if !ok {
		return nil, nil, nil
	}
	if err := utilccl.CheckEnterpriseEnabled("BACKUP"); err != nil {
		return nil, nil, err
	}
	if err := p.RequireSuperUser("BACKUP"); err != nil {
		return nil, nil, err
	}

	toFn, err := p.TypeAsString(&backup.To)
	if err != nil {
		return nil, nil, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(&backup.IncrementalFrom)
	if err != nil {
		return nil, nil, err
	}

	header := sql.ResultColumns{
		{Name: "to", Typ: parser.TypeString},
		{Name: "startTs", Typ: parser.TypeString},
		{Name: "endTs", Typ: parser.TypeString},
		{Name: "dataSize", Typ: parser.TypeInt},
	}
	fn := func() ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(baseCtx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		var startTime hlc.Timestamp
		if backup.IncrementalFrom != nil {
			var err error
			startTime, err = ValidatePreviousBackups(ctx, incrementalFromFn())
			if err != nil {
				return nil, err
			}
		}
		endTime := p.ExecCfg().Clock.Now()
		if backup.AsOf.Expr != nil {
			var err error
			if endTime, err = sql.EvalAsOfTimestamp(nil, backup.AsOf, endTime); err != nil {
				return nil, err
			}
		}

		to := toFn()
		desc, err := Backup(ctx,
			p,
			to,
			backup.Targets,
			startTime, endTime,
			backup.Options,
		)
		if err != nil {
			return nil, err
		}
		ret := []parser.Datums{{
			parser.NewDString(to),
			parser.NewDString(startTime.String()),
			parser.NewDString(endTime.String()),
			parser.NewDInt(parser.DInt(desc.DataSize)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
}
