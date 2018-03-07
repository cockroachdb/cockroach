// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"io/ioutil"
	"sort"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	// BackupFormatDescriptorTrackingVersion added tracking of complete DBs.
	BackupFormatDescriptorTrackingVersion uint32 = 1
)

const (
	backupOptRevisionHistory = "revision_history"
)

var backupOptionExpectValues = map[string]bool{
	backupOptRevisionHistory: false,
}

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = time.Minute

// ReadBackupDescriptorFromURI creates an export store from the given URI, then
// reads and unmarshals a BackupDescriptor at the standard location in the
// export storage.
func ReadBackupDescriptorFromURI(
	ctx context.Context, uri string, settings *cluster.Settings,
) (BackupDescriptor, error) {
	exportStore, err := storageccl.ExportStorageFromURI(ctx, uri, settings)
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
	if err := protoutil.Unmarshal(descBytes, &backupDesc); err != nil {
		return BackupDescriptor{}, err
	}
	return backupDesc, err
}

// getRelevantDescChanges finds the changes between start and end time to the
// SQL descriptors matching `descs` or `expandedDBs`, ordered by time. A
// descriptor revision matches if it is an earlier revision of a descriptor in
// descs (same ID) or has parentID in `expanded`. Deleted descriptors are
// represented as nil. Fills in the `priorIDs` map in the process, which maps
// a descriptor the the ID by which it was previously known (e.g pre-TRUNCATE).
func getRelevantDescChanges(
	ctx context.Context,
	db *client.DB,
	startTime, endTime hlc.Timestamp,
	descs []sqlbase.Descriptor,
	expanded []sqlbase.ID,
	priorIDs map[sqlbase.ID]sqlbase.ID,
) ([]BackupDescriptor_DescriptorRevision, error) {

	allChanges, err := getAllDescChanges(ctx, db, startTime, endTime, priorIDs)
	if err != nil {
		return nil, err
	}

	// If no descriptors changed, we can just stop now and have RESTORE use the
	// normal list of descs (i.e. as of endTime).
	if len(allChanges) == 0 {
		return nil, nil
	}

	// interestingChanges will be every descriptor change relevant to the backup.
	var interestingChanges []BackupDescriptor_DescriptorRevision

	// interestingIDs are the descriptor for which we're interested in capturing
	// changes. This is initially the descriptors matched (as of endTime) by our
	// target spec, plus those that belonged to a DB that our spec expanded at any
	// point in the interval.
	interestingIDs := make(map[sqlbase.ID]struct{}, len(descs))

	// The descriptors that currently (endTime) match the target spec (desc) are
	// obviously interesting to our backup.
	for _, i := range descs {
		interestingIDs[i.GetID()] = struct{}{}
		if t := i.GetTable(); t != nil {
			for j := t.ReplacementOf.ID; j != sqlbase.InvalidID; j = priorIDs[j] {
				interestingIDs[j] = struct{}{}
			}
		}
	}

	// We're also interested in any desc that belonged to a DB we're backing up.
	// We'll start by looking at all descriptors as of the beginning of the
	// interval and add to the set of IDs that we are interested any descriptor that
	// belongs to one of the parents we care about.
	interestingParents := make(map[sqlbase.ID]struct{}, len(expanded))
	for _, i := range expanded {
		interestingParents[i] = struct{}{}
	}

	if !startTime.IsEmpty() {
		starting, err := loadAllDescs(ctx, db, startTime)
		if err != nil {
			return nil, err
		}
		for _, i := range starting {
			if table := i.GetTable(); table != nil {
				// We need to add to interestingIDs so that if we later see a delete for
				// this ID we still know it is interesting to us, even though we will not
				// have a parentID at that point (since the delete is a nil desc).
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
				}
			}
			if _, ok := interestingIDs[i.GetID()]; ok {
				desc := i
				// We inject a fake "revision" that captures the starting state for
				// matched descriptor, to allow restoring to times before its first rev
				// actually inside the window. This likely ends up duplicating the last
				// version in the previous BACKUP descriptor, but avoids adding more
				// complicated special-cases in RESTORE, so it only needs to look in a
				// single BACKUP to restore to a particular time.
				initial := BackupDescriptor_DescriptorRevision{Time: startTime, ID: i.GetID(), Desc: &desc}
				interestingChanges = append(interestingChanges, initial)
			}
		}
	}

	for _, change := range allChanges {
		// A change to an ID that we are interested in is obviously interesting --
		// a change is also interesting if it is to a table that has a parent that
		// we are interested and thereafter it also becomes an ID in which we are
		// interested in changes (since, as mentioned above, to decide if deletes
		// are interesting).
		if _, ok := interestingIDs[change.ID]; ok {
			interestingChanges = append(interestingChanges, change)
		} else if change.Desc != nil {
			if table := change.Desc.GetTable(); table != nil {
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
					interestingChanges = append(interestingChanges, change)
				}
			}
		}
	}

	sort.Slice(interestingChanges, func(i, j int) bool {
		return interestingChanges[i].Time.Less(interestingChanges[j].Time)
	})

	return interestingChanges, nil
}

// getAllDescChanges gets every sql descriptor change between start and end time
// returning its ID, content and the change time (with deletions represented as
// nil content).
func getAllDescChanges(
	ctx context.Context,
	db *client.DB,
	startTime, endTime hlc.Timestamp,
	priorIDs map[sqlbase.ID]sqlbase.ID,
) ([]BackupDescriptor_DescriptorRevision, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()

	allRevs, err := getAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var res []BackupDescriptor_DescriptorRevision

	for _, revs := range allRevs {
		id, err := keys.DecodeDescMetadataID(revs.Key)
		if err != nil {
			return nil, err
		}
		for _, rev := range revs.Values {
			r := BackupDescriptor_DescriptorRevision{ID: sqlbase.ID(id), Time: rev.Timestamp}
			if len(rev.RawBytes) != 0 {
				var desc sqlbase.Descriptor
				if err := rev.GetProto(&desc); err != nil {
					return nil, err
				}
				r.Desc = &desc
				if t := desc.GetTable(); t != nil && t.ReplacementOf.ID != sqlbase.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
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

func ensureInterleavesIncluded(tables []*sqlbase.TableDescriptor) error {
	inBackup := make(map[sqlbase.ID]bool, len(tables))
	for _, t := range tables {
		inBackup[t.ID] = true
	}

	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for _, a := range index.Interleave.Ancestors {
				if !inBackup[a.TableID] {
					return errors.Errorf(
						"cannot backup table %q without interleave parent (ID %d)", table.Name, a.TableID,
					)
				}
			}
			for _, c := range index.InterleavedBy {
				if !inBackup[c.Table] {
					return errors.Errorf(
						"cannot backup table %q without interleave child table (ID %d)", table.Name, c.Table,
					)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
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

type tableAndIndex struct {
	tableID sqlbase.ID
	indexID sqlbase.IndexID
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(
	tables []*sqlbase.TableDescriptor, revs []BackupDescriptor_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(index.ID)), false); err != nil {
				panic(errors.Wrap(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.ID, indexID: index.ID}] = true
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		if tbl := rev.Desc.GetTable(); tbl != nil {
			for _, idx := range tbl.AllNonDropIndexes() {
				key := tableAndIndex{tableID: tbl.ID, indexID: idx.ID}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(idx.ID)), false); err != nil {
						panic(errors.Wrap(err, "IndexSpan"))
					}
					added[key] = true
				}
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
	backup *tree.Backup, to string, incrementalFrom []string,
) (string, error) {
	b := &tree.Backup{
		AsOf:    backup.AsOf,
		Options: backup.Options,
		Targets: backup.Targets,
	}

	to, err := storageccl.SanitizeExportStorageURI(to)
	if err != nil {
		return "", err
	}
	b.To = tree.NewDString(to)

	for _, from := range incrementalFrom {
		sanitizedFrom, err := storageccl.SanitizeExportStorageURI(from)
		if err != nil {
			return "", err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, tree.NewDString(sanitizedFrom))
	}
	return tree.AsStringWithFlags(b, tree.FmtAlwaysQualifyTableNames), nil
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

// BackupFileDescriptors is an alias on which to implement sort's interface.
type BackupFileDescriptors []BackupDescriptor_File

func (r BackupFileDescriptors) Len() int      { return len(r) }
func (r BackupFileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r BackupFileDescriptors) Less(i, j int) bool {
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
	sort.Sort(BackupFileDescriptors(desc.Files))

	descBuf, err := protoutil.Marshal(desc)
	if err != nil {
		return err
	}

	return exportStore.WriteFile(ctx, filename, bytes.NewReader(descBuf))
}

func loadAllDescs(
	ctx context.Context, db *client.DB, asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, error) {
	var allDescs []sqlbase.Descriptor
	// TODO(andrei): Plumb a gatewayNodeID in here and also find a way to
	// express that whatever this txn does should not count towards lease
	// placement stats.
	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
	opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
	err := txn.Exec(ctx, opt, func(ctx context.Context, txn *client.Txn, opt *client.TxnExecOptions) error {
		var err error
		txn.SetFixedTimestamp(ctx, asOf)
		allDescs, err = allSQLDescriptors(ctx, txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return allDescs, nil
}

func resolveTargetsToDescriptors(
	ctx context.Context, p sql.PlanHookState, endTime hlc.Timestamp, targets tree.TargetList,
) ([]sqlbase.Descriptor, []sqlbase.ID, error) {
	allDescs, err := loadAllDescs(ctx, p.ExecCfg().DB, endTime)
	if err != nil {
		return nil, nil, err
	}

	var matched descriptorsMatched
	if matched, err = descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets); err != nil {
		return nil, nil, err
	}

	// Ensure interleaved tables appear after their parent. Since parents must be
	// created before their children, simply sorting by ID accomplishes this.
	sort.Slice(matched.descs, func(i, j int) bool { return matched.descs[i].GetID() < matched.descs[j].GetID() })
	return matched.descs, matched.expandedDB, nil
}

type spanAndTime struct {
	span       roachpb.Span
	start, end hlc.Timestamp
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
	settings *cluster.Settings,
	exportStore storageccl.ExportStorage,
	job *jobs.Job,
	backupDesc *BackupDescriptor,
	checkpointDesc *BackupDescriptor,
	resultsCh chan<- tree.Datums,
) (roachpb.BulkOpSummary, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	mu := struct {
		syncutil.Mutex
		files          []BackupDescriptor_File
		exported       roachpb.BulkOpSummary
		lastCheckpoint time.Time
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
		return mu.exported, errors.Wrap(err, "fetching range descriptors")
	}

	var completedSpans, completedIntroducedSpans []roachpb.Span
	if checkpointDesc != nil {
		// TODO(benesch): verify these files, rather than accepting them as truth
		// blindly.
		// No concurrency yet, so these assignments are safe.
		mu.files = checkpointDesc.Files
		mu.exported = checkpointDesc.EntryCounts
		for _, file := range checkpointDesc.Files {
			if file.StartTime.IsEmpty() && !file.EndTime.IsEmpty() {
				completedIntroducedSpans = append(completedIntroducedSpans, file.Span)
			} else {
				completedSpans = append(completedSpans, file.Span)
			}
		}
	}

	// Subtract out any completed spans and split the remaining spans into
	// range-sized pieces so that we can use the number of completed requests as a
	// rough measure of progress.
	spans := splitAndFilterSpans(backupDesc.Spans, completedSpans, ranges)
	introducedSpans := splitAndFilterSpans(backupDesc.IntroducedSpans, completedIntroducedSpans, ranges)

	allSpans := make([]spanAndTime, 0, len(spans)+len(introducedSpans))
	for _, s := range introducedSpans {
		allSpans = append(allSpans, spanAndTime{span: s, start: hlc.Timestamp{}, end: backupDesc.StartTime})
	}
	for _, s := range spans {
		allSpans = append(allSpans, spanAndTime{span: s, start: backupDesc.StartTime, end: backupDesc.EndTime})
	}

	progressLogger := jobs.ProgressLogger{
		Job:           job,
		TotalChunks:   len(spans),
		StartFraction: job.Payload().FractionCompleted,
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
	maxConcurrentExports := clusterNodeCount(gossip) * int(storage.ExportRequestsLimit.Get(&settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)

	g, gCtx := errgroup.WithContext(ctx)

	requestFinishedCh := make(chan struct{}, len(spans)) // enough buffer to never block

	// Only start the progress logger if there are spans, otherwise this will
	// block forever. This is needed for TestBackupRestoreResume which doesn't
	// have any spans. Users should never hit this.
	if len(spans) > 0 {
		g.Go(func() error {
			return progressLogger.Loop(gCtx, requestFinishedCh)
		})
	}

	for i := range allSpans {
		select {
		case exportsSem <- struct{}{}:
		case <-ctx.Done():
			return mu.exported, ctx.Err()
		}

		span := allSpans[i]
		g.Go(func() error {
			defer func() { <-exportsSem }()
			header := roachpb.Header{Timestamp: span.end}
			req := &roachpb.ExportRequest{
				Span:       span.span,
				Storage:    exportStore.Conf(),
				StartTime:  span.start,
				MVCCFilter: roachpb.MVCCFilter(backupDesc.MVCCFilter),
			}
			rawRes, pErr := client.SendWrappedWith(gCtx, db.GetSender(), header, req)
			if pErr != nil {
				return pErr.GoError()
			}
			res := rawRes.(*roachpb.ExportResponse)

			mu.Lock()
			if backupDesc.RevisionStartTime.Less(res.StartTime) {
				backupDesc.RevisionStartTime = res.StartTime
			}
			for _, file := range res.Files {
				f := BackupDescriptor_File{
					Span:        file.Span,
					Path:        file.Path,
					Sha512:      file.Sha512,
					EntryCounts: file.Exported,
				}
				if span.start != backupDesc.StartTime {
					f.StartTime = span.start
					f.EndTime = span.end
				}
				mu.files = append(mu.files, f)
				mu.exported.Add(file.Exported)
			}
			var checkpointFiles BackupFileDescriptors
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
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return mu.exported, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}

	// No more concurrency, so no need to acquire locks below.

	backupDesc.Files = mu.files
	backupDesc.EntryCounts = mu.exported

	if err := writeBackupDescriptor(ctx, exportStore, BackupDescriptorName, backupDesc); err != nil {
		return mu.exported, err
	}

	return mu.exported, nil
}

// VerifyUsableExportTarget ensures that the target location does not already
// contain a BACKUP or checkpoint and writes an empty checkpoint, both verifying
// that the location is writable and locking out accidental concurrent
// operations on that location if subsequently try this check. Callers must
// clean up the written checkpoint file (BackupDescriptorCheckpointName) only
// after writing to the backup file location (BackupDescriptorName).
func VerifyUsableExportTarget(
	ctx context.Context, exportStore storageccl.ExportStorage, readable string,
) error {
	if r, err := exportStore.ReadFile(ctx, BackupDescriptorName); err == nil {
		// TODO(dt): If we audit exactly what not-exists error each ExportStorage
		// returns (and then wrap/tag them), we could narrow this check.
		r.Close()
		return errors.Errorf("%s already contains a %s file",
			readable, BackupDescriptorName)
	}
	if r, err := exportStore.ReadFile(ctx, BackupDescriptorCheckpointName); err == nil {
		r.Close()
		return errors.Errorf("%s already contains a %s file (is another operation already in progress?)",
			readable, BackupDescriptorCheckpointName)
	}
	if err := writeBackupDescriptor(
		ctx, exportStore, BackupDescriptorCheckpointName, &BackupDescriptor{},
	); err != nil {
		return errors.Wrapf(err, "cannot write to %s", readable)
	}
	return nil
}

func backupPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- tree.Datums) error, sqlbase.ResultColumns, error) {
	backupStmt, ok := stmt.(*tree.Backup)
	if !ok {
		return nil, nil, nil
	}

	toFn, err := p.TypeAsString(backupStmt.To, "BACKUP")
	if err != nil {
		return nil, nil, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(backupStmt.IncrementalFrom, "BACKUP")
	if err != nil {
		return nil, nil, err
	}
	optsFn, err := p.TypeAsStringOpts(backupStmt.Options, backupOptionExpectValues)
	if err != nil {
		return nil, nil, err
	}

	header := sqlbase.ResultColumns{
		{Name: "job_id", Typ: types.Int},
		{Name: "status", Typ: types.String},
		{Name: "fraction_completed", Typ: types.Float},
		{Name: "rows", Typ: types.Int},
		{Name: "index_entries", Typ: types.Int},
		{Name: "system_records", Typ: types.Int},
		{Name: "bytes", Typ: types.Int},
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "BACKUP",
		); err != nil {
			return err
		}

		if err := p.RequireSuperUser(ctx, "BACKUP"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("BACKUP cannot be used inside a transaction")
		}

		// older nodes don't know about many new fields, e.g. MVCCAll and may
		// incorrectly evaluate either an export RPC, or a resumed backup job.
		// VersionClearRange was introduced after most of these new fields and the
		// jobs resume refactorings, though we may still wish to bump this to 2.0
		// when that is defined.
		if !p.ExecCfg().Settings.Version.IsMinSupported(cluster.VersionClearRange) {
			return errors.Errorf(
				"running BACKUP on a 2.x node requires cluster version >= %s (",
				cluster.VersionByKey(cluster.VersionClearRange).String(),
			)
		}

		to, err := toFn()
		if err != nil {
			return err
		}
		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return err
		}

		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = sql.EvalAsOfTimestamp(nil, backupStmt.AsOf, endTime); err != nil {
				return err
			}
		}

		exportStore, err := storageccl.ExportStorageFromURI(ctx, to, p.ExecCfg().Settings)
		if err != nil {
			return err
		}
		defer exportStore.Close()

		opts, err := optsFn()
		if err != nil {
			return err
		}

		mvccFilter := MVCCFilter_Latest
		if _, ok := opts[backupOptRevisionHistory]; ok {
			mvccFilter = MVCCFilter_All
		}

		targetDescs, completeDBs, err := resolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets)
		if err != nil {
			return err
		}

		var tables []*sqlbase.TableDescriptor
		for _, desc := range targetDescs {
			if dbDesc := desc.GetDatabase(); dbDesc != nil {
				if err := p.CheckPrivilege(ctx, dbDesc, privilege.SELECT); err != nil {
					return err
				}
			}
			if tableDesc := desc.GetTable(); tableDesc != nil {
				if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
					return err
				}
				tables = append(tables, tableDesc)
			}
		}

		if err := ensureInterleavesIncluded(tables); err != nil {
			return err
		}

		var prevBackups []BackupDescriptor
		if len(incrementalFrom) > 0 {
			clusterID := p.ExecCfg().ClusterID()
			prevBackups = make([]BackupDescriptor, len(incrementalFrom))
			for i, uri := range incrementalFrom {
				desc, err := ReadBackupDescriptorFromURI(ctx, uri, p.ExecCfg().Settings)
				if err != nil {
					return errors.Wrapf(err, "failed to read backup from %q", uri)
				}
				// IDs are how we identify tables, and those are only meaningful in the
				// context of their own cluster, so we need to ensure we only allow
				// incremental previous backups that we created.
				if !desc.ClusterID.Equal(clusterID) {
					return errors.Errorf("previous BACKUP %q belongs to cluster %s", uri, desc.ClusterID.String())
				}
				prevBackups[i] = desc
			}
		}

		var startTime hlc.Timestamp
		var newSpans roachpb.Spans
		if len(prevBackups) > 0 {
			startTime = prevBackups[len(prevBackups)-1].EndTime
		}

		var priorIDs map[sqlbase.ID]sqlbase.ID

		var revs []BackupDescriptor_DescriptorRevision
		if mvccFilter == MVCCFilter_All {
			priorIDs = make(map[sqlbase.ID]sqlbase.ID)
			revs, err = getRelevantDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, targetDescs, completeDBs, priorIDs)
			if err != nil {
				return err
			}
		}

		spans := spansForAllTableIndexes(tables, revs)

		if len(prevBackups) > 0 {
			tablesInPrev := make(map[sqlbase.ID]struct{})
			dbsInPrev := make(map[sqlbase.ID]struct{})
			for _, d := range prevBackups[len(prevBackups)-1].Descriptors {
				if t := d.GetTable(); t != nil {
					tablesInPrev[t.ID] = struct{}{}
				}
			}
			for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
				dbsInPrev[d] = struct{}{}
			}

			for _, d := range targetDescs {
				if t := d.GetTable(); t != nil {
					// If we're trying to use a previous backup for this table, ideally it
					// actually contains this table.
					if _, ok := tablesInPrev[t.ID]; ok {
						continue
					}
					// This table isn't in the previous backup... maybe was added to a
					// DB that the previous backup captured?
					if _, ok := dbsInPrev[t.ParentID]; ok {
						continue
					}
					// Maybe this table is missing from the previous backup because it was
					// truncated?
					if t.ReplacementOf.ID != sqlbase.InvalidID {

						// Check if we need to lazy-load the priorIDs (i.e. if this is the first
						// truncate we've encountered in non-MVCC backup).
						if priorIDs == nil {
							priorIDs = make(map[sqlbase.ID]sqlbase.ID)
							_, err := getAllDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, priorIDs)
							if err != nil {
								return err
							}
						}
						found := false
						for was := t.ReplacementOf.ID; was != sqlbase.InvalidID && !found; was = priorIDs[was] {
							_, found = tablesInPrev[was]
						}
						if found {
							continue
						}
					}
					return errors.Errorf("previous backup does not contain table %q", t.Name)
				}
			}

			var err error
			_, coveredTime, err := makeImportSpans(spans, prevBackups, keys.MinKey,
				func(span intervalccl.Range, start, end hlc.Timestamp) error {
					if (start == hlc.Timestamp{}) {
						newSpans = append(newSpans, roachpb.Span{Key: span.Start, EndKey: span.End})
						return nil
					}
					return errOnMissingRange(span, start, end)
				})
			if err != nil {
				return errors.Wrap(err, "invalid previous backups (a new full backup may be required if a table has been created, dropped or truncated)")
			}
			if coveredTime != startTime {
				return errors.Errorf("expected previous backups to cover until time %v, got %v", startTime, coveredTime)
			}
		}

		backupDesc := BackupDescriptor{
			StartTime:         startTime,
			EndTime:           endTime,
			MVCCFilter:        mvccFilter,
			Descriptors:       targetDescs,
			DescriptorChanges: revs,
			CompleteDbs:       completeDBs,
			Spans:             spans,
			IntroducedSpans:   newSpans,
			FormatVersion:     BackupFormatDescriptorTrackingVersion,
			BuildInfo:         build.GetInfo(),
			NodeID:            p.ExecCfg().NodeID.Get(),
			ClusterID:         p.ExecCfg().ClusterID(),
		}

		// Sanity check: re-run the validation that RESTORE will do, but this time
		// including this backup, to ensure that the this backup plus any previous
		// backups does cover the interval expected.
		if _, coveredEnd, err := makeImportSpans(
			spans, append(prevBackups, backupDesc), keys.MinKey, errOnMissingRange,
		); err != nil {
			return err
		} else if coveredEnd != endTime {
			return errors.Errorf("expected backup (along with any previous backups) to cover to %v, not %v", endTime, coveredEnd)
		}

		descBytes, err := protoutil.Marshal(&backupDesc)
		if err != nil {
			return err
		}

		description, err := backupJobDescription(backupStmt, to, incrementalFrom)
		if err != nil {
			return err
		}

		if err := VerifyUsableExportTarget(ctx, exportStore, to); err != nil {
			return err
		}

		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: description,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, sqlDesc := range backupDesc.Descriptors {
					sqlDescIDs = append(sqlDescIDs, sqlDesc.GetID())
				}
				return sqlDescIDs
			}(),
			Details: jobs.BackupDetails{
				StartTime:        startTime,
				EndTime:          endTime,
				URI:              to,
				BackupDescriptor: descBytes,
			},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, header, nil
}

type backupResumer struct {
	settings *cluster.Settings
	res      roachpb.BulkOpSummary
}

func (b *backupResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := job.Record.Details.(jobs.BackupDetails)
	p := phs.(sql.PlanHookState)

	if len(details.BackupDescriptor) == 0 {
		return errors.New("missing backup descriptor; cannot resume a backup from an older version")
	}

	var backupDesc BackupDescriptor
	if err := protoutil.Unmarshal(details.BackupDescriptor, &backupDesc); err != nil {
		return errors.Wrap(err, "unmarshal backup descriptor")
	}
	conf, err := storageccl.ExportStorageConfFromURI(details.URI)
	if err != nil {
		return err
	}
	exportStore, err := storageccl.MakeExportStorage(ctx, conf, b.settings)
	if err != nil {
		return err
	}
	var checkpointDesc *BackupDescriptor
	if desc, err := readBackupDescriptor(ctx, exportStore, BackupDescriptorCheckpointName); err == nil {
		// If the checkpoint is from a different cluster, it's meaningless to us.
		// More likely though are dummy/lock-out checkpoints with no ClusterID.
		if desc.ClusterID.Equal(p.ExecCfg().ClusterID()) {
			checkpointDesc = &desc
		}
	} else {
		// TODO(benesch): distinguish between a missing checkpoint, which simply
		// indicates the prior backup attempt made no progress, and a corrupted
		// checkpoint, which is more troubling. Sadly, storageccl doesn't provide a
		// "not found" error that's consistent across all ExportStorage
		// implementations.
		log.Warningf(ctx, "unable to load backup checkpoint while resuming job %d: %v", *job.ID(), err)
	}
	res, err := backup(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		p.ExecCfg().Settings,
		exportStore,
		job,
		&backupDesc,
		checkpointDesc,
		resultsCh,
	)
	b.res = res
	return err
}

func (b *backupResumer) OnFailOrCancel(context.Context, *client.Txn, *jobs.Job) error { return nil }
func (b *backupResumer) OnSuccess(context.Context, *client.Txn, *jobs.Job) error      { return nil }

func (b *backupResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	// Attempt to delete BACKUP-CHECKPOINT.
	if err := func() error {
		details := job.Record.Details.(jobs.BackupDetails)
		conf, err := storageccl.ExportStorageConfFromURI(details.URI)
		if err != nil {
			return err
		}
		exportStore, err := storageccl.MakeExportStorage(ctx, conf, b.settings)
		if err != nil {
			return err
		}
		return exportStore.Delete(ctx, BackupDescriptorCheckpointName)
	}(); err != nil {
		log.Warningf(ctx, "unable to delete checkpointed backup descriptor: %+v", err)
	}

	if status == jobs.StatusSucceeded {
		// TODO(benesch): emit periodic progress updates.

		// TODO(mjibson): if a restore was resumed, then these counts will only have
		// the current coordinator's counts.

		resultsCh <- tree.Datums{
			tree.NewDInt(tree.DInt(*job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(b.res.Rows)),
			tree.NewDInt(tree.DInt(b.res.IndexEntries)),
			tree.NewDInt(tree.DInt(b.res.SystemRecords)),
			tree.NewDInt(tree.DInt(b.res.DataSize)),
		}
	}
}

var _ jobs.Resumer = &backupResumer{}

func backupResumeHook(typ jobs.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobs.TypeBackup {
		return nil
	}

	return &backupResumer{
		settings: settings,
	}
}

func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- tree.Datums) error, sqlbase.ResultColumns, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "SHOW BACKUP",
	); err != nil {
		return nil, nil, err
	}

	if err := p.RequireSuperUser(ctx, "SHOW BACKUP"); err != nil {
		return nil, nil, err
	}

	toFn, err := p.TypeAsString(backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, err
	}
	header := sqlbase.ResultColumns{
		{Name: "database", Typ: types.String},
		{Name: "table", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	}
	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}
		desc, err := ReadBackupDescriptorFromURI(ctx, str, p.ExecCfg().Settings)
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
		start := tree.DNull
		if desc.StartTime.WallTime != 0 {
			start = tree.MakeDTimestamp(timeutil.Unix(0, desc.StartTime.WallTime), time.Nanosecond)
		}
		for _, descriptor := range desc.Descriptors {
			if table := descriptor.GetTable(); table != nil {
				dbName := descs[table.ParentID]
				resultsCh <- tree.Datums{
					tree.NewDString(dbName),
					tree.NewDString(table.Name),
					start,
					tree.MakeDTimestamp(timeutil.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
					tree.NewDInt(tree.DInt(descSizes[table.ID].DataSize)),
					tree.NewDInt(tree.DInt(descSizes[table.ID].Rows)),
				}
			}
		}
		return nil
	}
	return fn, header, nil
}

type versionedValues struct {
	Key    roachpb.Key
	Values []roachpb.Value
}

// getAllRevisions scans all keys between startKey and endKey getting all
// revisions between startTime and endTime.
// TODO(dt): if/when client gets a ScanRevisionsRequest or similar, use that.
func getAllRevisions(
	ctx context.Context,
	db *client.DB,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
) ([]versionedValues, error) {
	// TODO(dt): version check.
	header := roachpb.Header{Timestamp: endTime}
	req := &roachpb.ExportRequest{
		Span:       roachpb.Span{Key: startKey, EndKey: endKey},
		StartTime:  startTime,
		MVCCFilter: roachpb.MVCCFilter_All,
		ReturnSST:  true,
	}
	resp, pErr := client.SendWrappedWith(ctx, db.GetSender(), header, req)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	var res []versionedValues
	for _, file := range resp.(*roachpb.ExportResponse).Files {
		sst := engine.MakeRocksDBSstFileReader()
		defer sst.Close()

		if err := sst.IngestExternalFile(file.SST); err != nil {
			return nil, err
		}
		start, end := engine.MVCCKey{Key: startKey}, engine.MVCCKey{Key: endKey}
		if err := sst.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
			if len(res) == 0 || !res[len(res)-1].Key.Equal(kv.Key.Key) {
				res = append(res, versionedValues{Key: kv.Key.Key})
			}
			res[len(res)-1].Values = append(res[len(res)-1].Values, roachpb.Value{Timestamp: kv.Key.Timestamp, RawBytes: kv.Value})
			return false, nil
		}); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
	sql.AddPlanHook(showBackupPlanHook)
	jobs.AddResumeHook(backupResumeHook)
}
