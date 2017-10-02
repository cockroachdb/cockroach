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

// readBackupDescriptorFromURI creates an export store from the given URI, then
// reads and unmarshals a BackupDescriptor at the standard location in the
// export storage.
func readBackupDescriptorFromURI(ctx context.Context, uri string) (BackupDescriptor, error) {
	exportStore, err := exportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer exportStore.Close()
	backupDesc, err := readBackupDescriptor(ctx, exportStore, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, err
	}
	backupDesc.Dir = exportStore.Conf()
	// TODO(dan): Sanity check this BackupDescriptor: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return backupDesc, nil
}

// readBackupDescriptor reads and unmarshals a BackupDescriptor from filename in
// the provided export store.
func readBackupDescriptor(
	ctx context.Context, exportStore storageccl.ExportStorage, filename string,
) (BackupDescriptor, error) {
	r, err := exportStore.ReadFile(ctx, filename)
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
	return backupDesc, err
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
		desc, err := readBackupDescriptorFromURI(ctx, uri)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		backups[i] = desc
	}

	// This reuses Restore's logic for lining up all the start and end
	// timestamps to validate the previous backups that this one is incremental
	// from.
	lowWaterMark := keys.MinKey
	_, endTime, err := makeImportSpans(nil, backups, lowWaterMark)
	return endTime, err
}

func allSQLDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.Descriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		// NB: Don't wrap this error, as wrapped HandledRetryableTxnErrors are not
		// automatically retried by db.Txn.
		//
		// TODO(benesch): teach the KV layer to use errors.Cause.
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
		// NB: Don't wrap this error, as wrapped HandledRetryableTxnErrors are not
		// automatically retried by db.Txn.
		//
		// TODO(benesch): teach the KV layer to use errors.Cause.
		return nil, err
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

// coveringFromSpans creates an intervalccl.Covering with a fixed payload from a
// slice of roachpb.Spans.
func coveringFromSpans(spans []roachpb.Span, payload interface{}) intervalccl.Covering {
	var covering intervalccl.Covering
	for _, span := range spans {
		covering = append(covering, intervalccl.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: payload,
		})
	}
	return covering
}

// splitAndFilterSpans returns the spans that represent the set difference
// (includes - excludes) while also guaranteeing that each output span does not
// cross the endpoint of a RangeDescriptor in ranges.
func splitAndFilterSpans(
	includes []roachpb.Span, excludes []roachpb.Span, ranges []roachpb.RangeDescriptor,
) []roachpb.Span {
	type includeMarker struct{}
	type excludeMarker struct{}

	includeCovering := coveringFromSpans(includes, includeMarker{})
	excludeCovering := coveringFromSpans(excludes, excludeMarker{})

	var rangeCovering intervalccl.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, intervalccl.Range{
			Start: []byte(rangeDesc.StartKey),
			End:   []byte(rangeDesc.EndKey),
		})
	}

	splits := intervalccl.OverlapCoveringMerge(
		[]intervalccl.Covering{includeCovering, excludeCovering, rangeCovering},
	)

	var out []roachpb.Span
	for _, split := range splits {
		include := false
		exclude := false
		for _, payload := range split.Payload.([]interface{}) {
			switch payload.(type) {
			case includeMarker:
				include = true
			case excludeMarker:
				exclude = true
			}
		}
		if include && !exclude {
			out = append(out, roachpb.Span{
				Key:    roachpb.Key(split.Start),
				EndKey: roachpb.Key(split.End),
			})
		}
	}
	return out
}

func backupJobDescription(
	backup *parser.Backup, to string, incrementalFrom []string,
) (string, error) {
	b := &parser.Backup{
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
	return parser.AsStringWithFlags(b, parser.FmtSimpleQualified), nil
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
		// TODO(andrei): Plumb a gatewayNodeID in here and also find a way to
		// express that whatever this txn does should not count towards lease
		// placement stats.
		txn := client.NewTxn(db, 0 /* gatewayNodeID */)
		opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
		err := txn.Exec(ctx, opt, func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			txn.SetFixedTimestamp(ctx, endTime)
			sqlDescs, err = allSQLDescriptors(ctx, txn)
			return err
		})
		if err != nil {
			return BackupDescriptor{}, err
		}
	}

	sessionDatabase := p.EvalContext().Database
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
	checkpointDesc *BackupDescriptor,
) error {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	mu := struct {
		syncutil.Mutex
		files          []BackupDescriptor_File
		exported       roachpb.BulkOpSummary
		lastCheckpoint time.Time
		checkpointed   bool
	}{}

	var checkpointMu syncutil.Mutex

	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		// TODO(benesch): limit the range descriptors we fetch to the ranges that
		// are actually relevant in the backup to speed up small backups on large
		// clusters.
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return errors.Wrap(err, "fetching range descriptors")
	}

	var completedSpans []roachpb.Span
	if checkpointDesc != nil {
		// TODO(benesch): verify these files, rather than accepting them as truth
		// blindly.
		// No concurrency yet, so these assignments are safe.
		mu.checkpointed = true
		mu.files = checkpointDesc.Files
		mu.exported = checkpointDesc.EntryCounts
		for _, file := range checkpointDesc.Files {
			completedSpans = append(completedSpans, file.Span)
		}
	}

	// Subtract out any completed spans and split the remaining spans into
	// range-sized pieces so that we can use the number of completed requests as a
	// rough measure of progress.
	spans := splitAndFilterSpans(backupDesc.Spans, completedSpans, ranges)

	progressLogger := jobProgressLogger{
		job:           job,
		totalChunks:   len(spans),
		startFraction: job.Payload().FractionCompleted,
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

	requestFinishedCh := make(chan struct{}, len(spans)) // enough buffer to never block
	g.Go(func() error {
		return progressLogger.loop(gCtx, requestFinishedCh)
	})

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
					Span:        file.Span,
					Path:        file.Path,
					Sha512:      file.Sha512,
					EntryCounts: file.Exported,
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

			requestFinishedCh <- struct{}{}

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
) (func(context.Context, chan<- parser.Datums) error, sqlbase.ResultColumns, error) {
	backupStmt, ok := stmt.(*parser.Backup)
	if !ok {
		return nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "BACKUP",
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

	fn := func(ctx context.Context, resultsCh chan<- parser.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := backupStmt.Targets.NormalizeTablesWithDatabase(p.EvalContext().Database); err != nil {
			return err
		}

		to, err := toFn()
		if err != nil {
			return err
		}
		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return err
		}

		var startTime hlc.Timestamp
		if backupStmt.IncrementalFrom != nil {
			var err error
			startTime, err = ValidatePreviousBackups(ctx, incrementalFrom)
			if err != nil {
				return err
			}
		}
		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = sql.EvalAsOfTimestamp(nil, backupStmt.AsOf, endTime); err != nil {
				return err
			}
		}

		exportStore, err := exportStorageFromURI(ctx, to)
		if err != nil {
			return err
		}
		defer exportStore.Close()

		// Ensure there isn't already a readable backup desc.
		{
			r, err := exportStore.ReadFile(ctx, BackupDescriptorName)
			// TODO(dt): If we audit exactly what not-exists error each ExportStorage
			// returns (and then wrap/tag them), we could narrow this check.
			if err == nil {
				r.Close()
				return errors.Errorf("a %s file already appears to exist in %s",
					BackupDescriptorName, to)
			}
		}

		backupDesc, err := makeBackupDescriptor(ctx, p, startTime, endTime, backupStmt.Targets)
		if err != nil {
			return err
		}

		description, err := backupJobDescription(backupStmt, to, incrementalFrom)
		if err != nil {
			return err
		}
		job := p.ExecCfg().JobRegistry.NewJob(jobs.Record{
			Description: description,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, sqlDesc := range backupDesc.Descriptors {
					sqlDescIDs = append(sqlDescIDs, sqlDesc.GetID())
				}
				return sqlDescIDs
			}(),
			Details: jobs.BackupDetails{
				StartTime: startTime,
				EndTime:   endTime,
				URI:       to,
			},
		})
		var checkpointDesc *BackupDescriptor
		backupErr := backup(ctx,
			p.ExecCfg().DB,
			p.ExecCfg().Gossip,
			exportStore,
			job,
			&backupDesc,
			checkpointDesc,
		)
		if err := job.FinishedWith(ctx, backupErr); err != nil {
			return err
		}
		if backupErr != nil {
			return backupErr
		}
		// TODO(benesch): emit periodic progress updates.
		resultsCh <- parser.Datums{
			parser.NewDInt(parser.DInt(*job.ID())),
			parser.NewDString(string(jobs.StatusSucceeded)),
			parser.NewDFloat(parser.DFloat(1.0)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.Rows)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.IndexEntries)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.SystemRecords)),
			parser.NewDInt(parser.DInt(backupDesc.EntryCounts.DataSize)),
		}
		return nil
	}
	return fn, header, nil
}

func backupResumeHook(typ jobs.Type) func(context.Context, *jobs.Job) error {
	if typ != jobs.TypeBackup {
		return nil
	}

	return func(ctx context.Context, job *jobs.Job) error {
		details := job.Record.Details.(jobs.BackupDetails)

		var sqlDescs []sqlbase.Descriptor
		var tables []*sqlbase.TableDescriptor

		{
			// TODO(andrei): Plumb a gatewayNodeID in here and also find a way to
			// express that whatever this txn does should not count towards lease
			// placement stats.
			txn := client.NewTxn(job.DB(), 0 /* gatewayNodeID */)
			opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
			if err := txn.Exec(ctx, opt, func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
				txn.SetFixedTimestamp(ctx, details.EndTime)
				for _, sqlDescID := range job.Payload().DescriptorIDs {
					desc := &sqlbase.Descriptor{}
					descKey := sqlbase.MakeDescMetadataKey(sqlDescID)
					if err := txn.GetProto(ctx, descKey, desc); err != nil {
						return err
					}
					sqlDescs = append(sqlDescs, *desc)
					if tableDesc := desc.GetTable(); tableDesc != nil {
						tables = append(tables, tableDesc)
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}

		backupDesc := BackupDescriptor{
			StartTime:     details.StartTime,
			EndTime:       details.EndTime,
			Descriptors:   sqlDescs,
			Spans:         spansForAllTableIndexes(tables),
			FormatVersion: BackupFormatInitialVersion,
			BuildInfo:     build.GetInfo(),
			NodeID:        job.NodeID(),
			ClusterID:     job.ClusterID(),
		}
		conf, err := storageccl.ExportStorageConfFromURI(details.URI)
		if err != nil {
			return err
		}
		exportStore, err := storageccl.MakeExportStorage(ctx, conf)
		if err != nil {
			return nil
		}
		var checkpointDesc *BackupDescriptor
		if desc, err := readBackupDescriptor(ctx, exportStore, BackupDescriptorCheckpointName); err == nil {
			checkpointDesc = &desc
		} else {
			// TODO(benesch): distinguish between a missing checkpoint, which simply
			// indicates the prior backup attempt made no progress, and a corrupted
			// checkpoint, which is more troubling. Sadly, storageccl doesn't provide a
			// "not found" error that's consistent across all ExportStorage
			// implementations.
			log.Warningf(ctx, "unable to load backup checkpoint while resuming job %d: %v", *job.ID(), err)
		}
		return backup(ctx, job.DB(), job.Gossip(), exportStore, job, &backupDesc, checkpointDesc)
	}
}

func showBackupPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- parser.Datums) error, sqlbase.ResultColumns, error) {
	backup, ok := stmt.(*parser.ShowBackup)
	if !ok {
		return nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "SHOW BACKUP",
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
		{Name: "rows", Typ: parser.TypeInt},
	}
	fn := func(ctx context.Context, resultsCh chan<- parser.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}
		desc, err := readBackupDescriptorFromURI(ctx, str)
		if err != nil {
			return err
		}
		descs := make(map[sqlbase.ID]string)
		for _, descriptor := range desc.Descriptors {
			if database := descriptor.GetDatabase(); database != nil {
				if _, ok := descs[database.ID]; !ok {
					descs[database.ID] = database.Name
				}
			}
		}
		descSizes := make(map[sqlbase.ID]roachpb.BulkOpSummary)
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
			s := descSizes[sqlbase.ID(tableID)]
			s.Add(file.EntryCounts)
			descSizes[sqlbase.ID(tableID)] = s
		}
		start := parser.DNull
		if desc.StartTime.WallTime != 0 {
			start = parser.MakeDTimestamp(timeutil.Unix(0, desc.StartTime.WallTime), time.Nanosecond)
		}
		for _, descriptor := range desc.Descriptors {
			if table := descriptor.GetTable(); table != nil {
				dbName := descs[table.ParentID]
				resultsCh <- parser.Datums{
					parser.NewDString(dbName),
					parser.NewDString(table.Name),
					start,
					parser.MakeDTimestamp(timeutil.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
					parser.NewDInt(parser.DInt(descSizes[table.ID].DataSize)),
					parser.NewDInt(parser.DInt(descSizes[table.ID].Rows)),
				}
			}
		}
		return nil
	}
	return fn, header, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
	sql.AddPlanHook(showBackupPlanHook)
	jobs.AddResumeHook(backupResumeHook)
}
