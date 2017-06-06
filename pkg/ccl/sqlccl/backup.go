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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// BackupDescriptorName is the file name used for serialized
	// BackupDescriptor protos.
	BackupDescriptorName = "BACKUP"
	// BackupFormatInitialVersion is the first version of backup and its files.
	BackupFormatInitialVersion uint32 = 0
)

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
	sstIntervalTree := interval.Tree{Overlapper: interval.Range.OverlapExclusive}
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
	jobLogger *sql.JobLogger,
) (BackupDescriptor, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var sqlDescs []sqlbase.Descriptor

	exportStore, err := exportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer exportStore.Close()

	// Ensure there isn't already a readable backup desc.
	{
		r, err := exportStore.ReadFile(ctx, BackupDescriptorName)
		// TODO(dt): If we audit exactly what not-exists error each ExportStorage
		// returns (and then wrap/tag them), we could narrow this check.
		if err == nil {
			r.Close()
			return BackupDescriptor{}, errors.Errorf("a %s file already appears to exist in %s",
				BackupDescriptorName, uri)
		}
	}

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

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the backup to speed up small backups on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return BackupDescriptor{}, err
	}

	// We split the spans into range-sized pieces so that we can use the number of
	// completed requests as a rough measure of progress.
	spans := splitSpansByRanges(spansForAllTableIndexes(tables), ranges)

	mu := struct {
		syncutil.Mutex
		files    []BackupDescriptor_File
		dataSize int64
	}{}

	progressLogger := jobProgressLogger{
		jobLogger:   jobLogger,
		totalChunks: len(spans),
	}

	for _, desc := range tables {
		jobLogger.Job.DescriptorIDs = append(jobLogger.Job.DescriptorIDs, desc.GetID())
	}
	if err := jobLogger.Created(ctx); err != nil {
		return BackupDescriptor{}, err
	}
	if err := jobLogger.Started(ctx); err != nil {
		return BackupDescriptor{}, err
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
	maxConcurrentExports := clusterNodeCount(p.ExecCfg().Gossip) * storageccl.ExportRequestLimit
	exportsSem := make(chan struct{}, maxConcurrentExports)

	header := roachpb.Header{Timestamp: endTime}
	g, gCtx := errgroup.WithContext(ctx)
	for i := range spans {
		select {
		case exportsSem <- struct{}{}:
		case <-ctx.Done():
			return BackupDescriptor{}, ctx.Err()
		}

		span := spans[i]
		g.Go(func() error {
			defer func() { <-exportsSem }()

			req := &roachpb.ExportRequest{
				Span:      span,
				Storage:   exportStore.Conf(),
				StartTime: startTime,
			}
			res, pErr := client.SendWrappedWith(gCtx, db.GetSender(), header, req)
			if pErr != nil {
				return pErr.GoError()
			}
			mu.Lock()
			for _, file := range res.(*roachpb.ExportResponse).Files {
				mu.files = append(mu.files, BackupDescriptor_File{
					Span:   file.Span,
					Path:   file.Path,
					Sha512: file.Sha512,
				})
				mu.dataSize += file.DataSize
			}
			mu.Unlock()
			if err := progressLogger.chunkFinished(ctx); err != nil {
				// Errors while updating progress are not important enough to merit
				// failing the entire backup.
				log.Errorf(ctx, "BACKUP ignoring error while updating progress on job %d (%s): %+v",
					jobLogger.JobID(), jobLogger.Job.Description, err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return BackupDescriptor{}, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}
	files, dataSize := mu.files, mu.dataSize // No more concurrency, so this is safe.

	desc := BackupDescriptor{
		StartTime:     startTime,
		EndTime:       endTime,
		Descriptors:   sqlDescs,
		Spans:         spans,
		Files:         files,
		DataSize:      dataSize,
		FormatVersion: BackupFormatInitialVersion,
		BuildInfo:     build.GetInfo(),
		NodeID:        p.ExecCfg().NodeID.Get(),
		ClusterID:     p.ExecCfg().ClusterID(),
	}
	sort.Sort(backupFileDescriptors(desc.Files))

	descBuf, err := desc.Marshal()
	if err != nil {
		return BackupDescriptor{}, err
	}

	if err := exportStore.WriteFile(ctx, BackupDescriptorName, bytes.NewReader(descBuf)); err != nil {
		return BackupDescriptor{}, err
	}
	return desc, nil
}

func backupPlanHook(
	baseCtx context.Context, stmt parser.Statement, p sql.PlanHookState,
) (func() ([]parser.Datums, error), sqlbase.ResultColumns, error) {
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

	header := sqlbase.ResultColumns{
		{Name: "job_id", Typ: parser.TypeInt},
		{Name: "status", Typ: parser.TypeString},
		{Name: "fraction_completed", Typ: parser.TypeFloat},
		{Name: "bytes", Typ: parser.TypeInt},
	}
	fn := func() ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(baseCtx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		to := toFn()
		incrementalFrom := incrementalFromFn()

		var startTime hlc.Timestamp
		if backup.IncrementalFrom != nil {
			var err error
			startTime, err = ValidatePreviousBackups(ctx, incrementalFrom)
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
		description, err := backupJobDescription(backup, to, incrementalFrom)
		if err != nil {
			return nil, err
		}
		jobLogger := sql.NewJobLogger(p.ExecCfg().DB, p.LeaseMgr(), sql.JobRecord{
			Description: description,
			Username:    p.User(),
			Details:     sql.BackupJobDetails{},
		})
		desc, err := Backup(ctx,
			p,
			to,
			backup.Targets,
			startTime, endTime,
			backup.Options,
			&jobLogger,
		)
		if err != nil {
			jobLogger.Failed(ctx, err)
			return nil, err
		}
		if err := jobLogger.Succeeded(ctx); err != nil {
			// An error while marking the job as successful is not important enough to
			// merit failing the entire backup.
			log.Errorf(ctx, "BACKUP ignoring error while marking job %d (%s) as successful: %+v",
				jobLogger.JobID(), description, err)
		}
		// TODO(benesch): emit periodic progress updates once we have the
		// infrastructure to stream responses.
		ret := []parser.Datums{{
			parser.NewDInt(parser.DInt(*jobLogger.JobID())),
			parser.NewDString(string(sql.JobStatusSucceeded)),
			parser.NewDFloat(parser.DFloat(1.0)),
			parser.NewDInt(parser.DInt(desc.DataSize)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
}
