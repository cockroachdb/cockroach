// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"bytes"
	"io/ioutil"
	"sort"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// BackupDescriptorName is the file name used for serialized
	// BackupDescriptor protos.
	BackupDescriptorName = "BACKUP"
	// BackupDescriptorCheckpointName is the file name used to store the
	// serialized BackupDescriptor proto while the backup is in progress.
	BackupDescriptorCheckpointName = "BACKUP-CHECKPOINT"
	// BackupFormatInitialVersion is the first version of backup and its files.
	BackupFormatInitialVersion uint32 = 0
)

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = time.Minute

// BackupImplicitSQLDescriptors are descriptors for tables that are implicitly
// included in every backup, plus their parent database descriptors.
var BackupImplicitSQLDescriptors = []sqlbase.Descriptor{
	*sqlbase.WrapDescriptor(&sqlbase.SystemDB),
	*sqlbase.WrapDescriptor(&sqlbase.DescriptorTable),
	*sqlbase.WrapDescriptor(&sqlbase.UsersTable),
}

// exportStorageFromURI returns an ExportStorage for the given URI.
func exportStorageFromURI(ctx context.Context, uri string) (storageccl.ExportStorage, error) {
	conf, err := storageccl.ExportStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return storageccl.MakeExportStorage(ctx, conf)
}

// readBackupDescriptor reads and unmarshals a BackupDescriptor from given base.
func readBackupDescriptor(ctx context.Context, uri string) (BackupDescriptor, error) {
	dir, err := exportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer dir.Close()
	conf := dir.Conf()
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
	backupDesc.Dir = conf
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
		desc, err := readBackupDescriptor(ctx, uri)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		backups[i] = desc
	}

	// This reuses Restore's logic for lining up all the start and end
	// timestamps to validate the previous backups that this one is incremental
	// from.
	_, endTime, err := makeImportSpans(nil, backups)
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

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(tables []*sqlbase.TableDescriptor) []roachpb.Span {
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(index.ID)), false); err != nil {
				panic(errors.Wrap(err, "IndexSpan"))
			}
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

// splitSpansByRanges takes a slice of non-overlapping spans and a slice of
// range descriptors and returns a new slice of spans in which each span has
// been split so that no single span extends beyond the boundaries of a range.
func splitSpansByRanges(spans []roachpb.Span, ranges []roachpb.RangeDescriptor) []roachpb.Span {
	type spanMarker struct{}

	var spanCovering intervalccl.Covering
	for _, span := range spans {
		spanCovering = append(spanCovering, intervalccl.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering intervalccl.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, intervalccl.Range{
			Start: []byte(rangeDesc.StartKey),
			End:   []byte(rangeDesc.EndKey),
		})
	}

	splits := intervalccl.OverlapCoveringMerge([]intervalccl.Covering{spanCovering, rangeCovering})

	var splitSpans []roachpb.Span
	for _, split := range splits {
		needed := false
		for _, payload := range split.Payload.([]interface{}) {
			if _, needed = payload.(spanMarker); needed {
				break
			}
		}
		if needed {
			splitSpans = append(splitSpans, roachpb.Span{
				Key:    roachpb.Key(split.Start),
				EndKey: roachpb.Key(split.End),
			})
		}
	}
	return splitSpans
}

func backupJobDescription(
	backup *parser.Backup, to string, incrementalFrom []string,
) (string, error) {
	b := parser.Backup{
		AsOf:    backup.AsOf,
		Options: backup.Options,
		Targets: backup.Targets,
	}

	to, err := storageccl.SanitizeExportStorageURI(to)
	if err != nil {
		return "", err
	}
	b.To = parser.NewDString(to)

	for _, from := range incrementalFrom {
		sanitizedFrom, err := storageccl.SanitizeExportStorageURI(from)
		if err != nil {
			return "", err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, parser.NewDString(sanitizedFrom))
	}

	return b.String(), nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	for k := range g.GetInfoStatus().Infos {
		if gossip.IsNodeIDKey(k) {
			nodes++
		}
	}
	return nodes
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

func writeBackupDescriptor(
	ctx context.Context,
	exportStore storageccl.ExportStorage,
	filename string,
	desc *BackupDescriptor,
) error {
	sort.Sort(backupFileDescriptors(desc.Files))

	descBuf, err := desc.Marshal()
	if err != nil {
		return err
	}

	if err := exportStore.WriteFile(ctx, filename, bytes.NewReader(descBuf)); err != nil {
		return err
	}

	return nil
}

func makeBackupDescriptor(
	ctx context.Context,
	p sql.PlanHookState,
	startTime, endTime hlc.Timestamp,
	targets parser.TargetList,
) (BackupDescriptor, error) {
	var err error
	var sqlDescs []sqlbase.Descriptor

	db := p.ExecCfg().DB

	{
		txn := client.NewTxn(db)
		opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
		err := txn.Exec(ctx, opt, func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			txn.SetFixedTimestamp(endTime)
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

	sqlDescs = append(sqlDescs, BackupImplicitSQLDescriptors...)

	// Dedupe. Duplicate descriptors will cause restore to fail.
	{
		descsByID := make(map[sqlbase.ID]sqlbase.Descriptor)
		for _, sqlDesc := range sqlDescs {
			descsByID[sqlDesc.GetID()] = sqlDesc
		}
		sqlDescs = sqlDescs[:0]
		for _, sqlDesc := range descsByID {
			sqlDescs = append(sqlDescs, sqlDesc)
		}
	}

	// Ensure interleaved tables appear after their parent. Since parents must be
	// created before their children, simply sorting by ID accomplishes this.
	sort.Slice(sqlDescs, func(i, j int) bool { return sqlDescs[i].GetID() < sqlDescs[j].GetID() })

	for _, desc := range sqlDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if err := p.CheckPrivilege(dbDesc, privilege.SELECT); err != nil {
				return BackupDescriptor{}, err
			}
		}
	}

	var tables []*sqlbase.TableDescriptor
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

	return BackupDescriptor{
		StartTime:     startTime,
		EndTime:       endTime,
		Descriptors:   sqlDescs,
		Spans:         spansForAllTableIndexes(tables),
		FormatVersion: BackupFormatInitialVersion,
		BuildInfo:     build.GetInfo(),
		NodeID:        p.ExecCfg().NodeID.Get(),
		ClusterID:     p.ExecCfg().ClusterID(),
	}, nil
}

// backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
// - <dir>/<unique_int>.sst
// - <dir> is given by the user and may be cloud storage
// - Each file contains data for a key range that doesn't overlap with any other
//   file.
func backup(
	ctx context.Context,
	db *client.DB,
	gossip *gossip.Gossip,
	exportStore storageccl.ExportStorage,
	job *jobs.Job,
	backupDesc *BackupDescriptor,
) error {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the backup to speed up small backups on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return err
	}

	// We split the spans into range-sized pieces so that we can use the number of
	// completed requests as a rough measure of progress.
	spans := splitSpansByRanges(backupDesc.Spans, ranges)

	mu := struct {
		syncutil.Mutex
		files          []BackupDescriptor_File
		exported       roachpb.BulkOpSummary
		lastCheckpoint time.Time
		checkpointed   bool
	}{}

	var checkpointMu syncutil.Mutex

	progressLogger := jobProgressLogger{
		job:         job,
		totalChunks: len(spans),
	}

	for _, desc := range backupDesc.Descriptors {
		if desc.GetTable() != nil {
			job.Record.DescriptorIDs = append(job.Record.DescriptorIDs, desc.GetID())
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	if err := job.Created(ctx, cancel); err != nil {
		return err
	}
	if err := job.Started(ctx); err != nil {
		return err
	}

	// We're already limiting these on the server-side, but sending all the
	// Export requests at once would fill up distsender/grpc/something and cause
	// all sorts of badness (node liveness timeouts leading to mass leaseholder
	// transfers, poor performance on SQL workloads, etc) as well as log spam
	// about slow distsender requests. Rate limit them here, too.
	//
	// Each node limits the number of running Export & Import requests it serves
	// to avoid overloading the network, so multiply that by the number of nodes
	// in the cluster and use that as the number of outstanding Export requests
	// for the rate limiting. This attempts to strike a balance between
	// simplicity, not getting slow distsender log spam, and keeping the server
	// side limiter full.
	//
	// TODO(dan): Make this limiting per node.
	//
	// TODO(dan): See if there's some better solution than rate-limiting #14798.
	maxConcurrentExports := clusterNodeCount(gossip) * storageccl.ExportRequestLimit
	exportsSem := make(chan struct{}, maxConcurrentExports)

	header := roachpb.Header{Timestamp: backupDesc.EndTime}
	g, gCtx := errgroup.WithContext(ctx)
	for i := range spans {
		select {
		case exportsSem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		span := spans[i]
		g.Go(func() error {
			defer func() { <-exportsSem }()

			req := &roachpb.ExportRequest{
				Span:      span,
				Storage:   exportStore.Conf(),
				StartTime: backupDesc.StartTime,
			}
			res, pErr := client.SendWrappedWith(gCtx, db.GetSender(), header, req)
			if pErr != nil {
				return pErr.GoError()
			}

			mu.Lock()
			for _, file := range res.(*roachpb.ExportResponse).Files {
				mu.files = append(mu.files, BackupDescriptor_File{
					Span:     file.Span,
					Path:     file.Path,
					Sha512:   file.Sha512,
					DataSize: uint64(file.Exported.DataSize),
				})
				mu.exported.Add(file.Exported)
			}
			var checkpointFiles backupFileDescriptors
			if timeutil.Since(mu.lastCheckpoint) > BackupCheckpointInterval {
				// We optimistically assume the checkpoint will succeed to prevent
				// multiple threads from attempting to checkpoint.
				mu.lastCheckpoint = timeutil.Now()
				checkpointFiles = append(checkpointFiles, mu.files...)
			}
			mu.Unlock()

			if err := progressLogger.chunkFinished(ctx); err != nil {
				// Errors while updating progress are not important enough to merit
				// failing the entire backup.
				log.Errorf(ctx, "BACKUP ignoring error while updating progress on job %d (%s): %+v",
					job.ID(), job.Record.Description, err)
			}

			if checkpointFiles != nil {
				checkpointMu.Lock()
				backupDesc.Files = checkpointFiles
				err := writeBackupDescriptor(
					ctx, exportStore, BackupDescriptorCheckpointName, backupDesc,
				)
				checkpointMu.Unlock()
				if err != nil {
					log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
					mu.Lock()
					mu.checkpointed = true
					mu.Unlock()
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrapf(err, "exporting %d ranges", len(spans))
	}

	// No more concurrency, so no need to acquire locks below.

	backupDesc.Files = mu.files
	backupDesc.EntryCounts = mu.exported

	if err := writeBackupDescriptor(ctx, exportStore, BackupDescriptorName, backupDesc); err != nil {
		return err
	}

	if mu.checkpointed {
		if err := exportStore.Delete(ctx, BackupDescriptorCheckpointName); err != nil {
			log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
		}
	}

	return nil
}

func backupPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context) ([]parser.Datums, error), sqlbase.ResultColumns, error) {
	backupStmt, ok := stmt.(*parser.Backup)
	if !ok {
		return nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().ClusterID(), p.ExecCfg().Organization.Get(), "BACKUP",
	); err != nil {
		return nil, nil, err
	}

	if err := p.RequireSuperUser("BACKUP"); err != nil {
		return nil, nil, err
	}

	toFn, err := p.TypeAsString(backupStmt.To, "BACKUP")
	if err != nil {
		return nil, nil, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(backupStmt.IncrementalFrom, "BACKUP")
	if err != nil {
		return nil, nil, err
	}

	header := sqlbase.ResultColumns{
		{Name: "job_id", Typ: parser.TypeInt},
		{Name: "status", Typ: parser.TypeString},
		{Name: "fraction_completed", Typ: parser.TypeFloat},
		{Name: "rows", Typ: parser.TypeInt},
		{Name: "index_entries", Typ: parser.TypeInt},
		{Name: "system_records", Typ: parser.TypeInt},
		{Name: "bytes", Typ: parser.TypeInt},
	}
	fn := func(ctx context.Context) ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		to, err := toFn()
		if err != nil {
			return nil, err
		}
		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return nil, err
		}

		var startTime hlc.Timestamp
		if backupStmt.IncrementalFrom != nil {
			var err error
			startTime, err = ValidatePreviousBackups(ctx, incrementalFrom)
			if err != nil {
				return nil, err
			}
		}
		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = sql.EvalAsOfTimestamp(nil, backupStmt.AsOf, endTime); err != nil {
				return nil, err
			}
		}

		exportStore, err := exportStorageFromURI(ctx, to)
		if err != nil {
			return nil, err
		}
		defer exportStore.Close()

		// Ensure there isn't already a readable backup desc.
		{
			r, err := exportStore.ReadFile(ctx, BackupDescriptorName)
			// TODO(dt): If we audit exactly what not-exists error each ExportStorage
			// returns (and then wrap/tag them), we could narrow this check.
			if err == nil {
				r.Close()
				return nil, errors.Errorf("a %s file already appears to exist in %s",
					BackupDescriptorName, to)
			}
		}

		backupDesc, err := makeBackupDescriptor(ctx, p, startTime, endTime, backupStmt.Targets)
		if err != nil {
			return nil, err
		}

		description, err := backupJobDescription(backupStmt, to, incrementalFrom)
		if err != nil {
			return nil, err
		}
		job := p.ExecCfg().JobRegistry.NewJob(jobs.Record{
			Description: description,
			Username:    p.User(),
			Details:     jobs.BackupDetails{},
		})
		backupErr := backup(ctx,
			p.ExecCfg().DB,
			p.ExecCfg().Gossip,
			exportStore,
			job,
			&backupDesc,
		)
		if err := job.FinishedWith(ctx, backupErr); err != nil {
			return nil, err
		}
		if backupErr != nil {
			return nil, backupErr
		}
		// TODO(benesch): emit periodic progress updates once we have the
		// infrastructure to stream responses.
		ret := []parser.Datums{{
			parser.NewDInt(parser.DInt(*job.ID())),
			parser.NewDString(string(jobs.StatusSucceeded)),
			parser.NewDFloat(parser.DFloat(1.0)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.Rows)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.IndexEntries)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.SystemRecords)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.DataSize)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func showBackupPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context) ([]parser.Datums, error), sqlbase.ResultColumns, error) {
	backup, ok := stmt.(*parser.ShowBackup)
	if !ok {
		return nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().ClusterID(), p.ExecCfg().Organization.Get(), "SHOW BACKUP",
	); err != nil {
		return nil, nil, err
	}

	if err := p.RequireSuperUser("SHOW BACKUP"); err != nil {
		return nil, nil, err
	}

	toFn, err := p.TypeAsString(backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, err
	}
	header := sqlbase.ResultColumns{
		{Name: "database", Typ: parser.TypeString},
		{Name: "table", Typ: parser.TypeString},
		{Name: "start_time", Typ: parser.TypeTimestamp},
		{Name: "end_time", Typ: parser.TypeTimestamp},
		{Name: "size_bytes", Typ: parser.TypeInt},
	}
	fn := func(ctx context.Context) ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return nil, err
		}
		desc, err := readBackupDescriptor(ctx, str)
		if err != nil {
			return nil, err
		}
		var ret []parser.Datums
		descs := make(map[sqlbase.ID]string)
		for _, descriptor := range desc.Descriptors {
			if database := descriptor.GetDatabase(); database != nil {
				if _, ok := descs[database.ID]; !ok {
					descs[database.ID] = database.Name
				}
			}
		}
		descSizes := make(map[sqlbase.ID]uint64)
		for _, file := range desc.Files {
			// TODO(dan): This assumes each file in the backup only contains
			// data from a single table, which is usually but not always
			// correct. It does not account for interleaved tables or if a
			// BACKUP happened to catch a newly created table that hadn't yet
			// been split into its own range.
			_, tableID, err := encoding.DecodeUvarintAscending(file.Span.Key)
			if err != nil {
				continue
			}
			descSizes[sqlbase.ID(tableID)] += file.DataSize
		}
		for _, descriptor := range desc.Descriptors {
			if table := descriptor.GetTable(); table != nil {
				dbName := descs[table.ParentID]
				temp := parser.Datums{
					parser.NewDString(dbName),
					parser.NewDString(table.Name),
					parser.MakeDTimestamp(time.Unix(0, desc.StartTime.WallTime), time.Nanosecond),
					parser.MakeDTimestamp(time.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
					parser.NewDInt(parser.DInt(descSizes[table.ID])),
				}
				ret = append(ret, temp)
			}
		}
		return ret, nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
	sql.AddPlanHook(showBackupPlanHook)
}
