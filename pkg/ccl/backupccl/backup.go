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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// BackupDescriptorName is the file name used for serialized
	// BackupDescriptor protos.
	BackupDescriptorName = "BACKUP"
	// BackupManifestName is a future name for the serialized
	// BackupDescriptor proto.
	BackupManifestName = "BACKUP_MANIFEST"

	// BackupPartitionDescriptorPrefix is the file name prefix for serialized
	// BackupPartitionDescriptor protos.
	BackupPartitionDescriptorPrefix = "BACKUP_PART"
	// BackupDescriptorCheckpointName is the file name used to store the
	// serialized BackupDescriptor proto while the backup is in progress.
	BackupDescriptorCheckpointName = "BACKUP-CHECKPOINT"
	// BackupFormatDescriptorTrackingVersion added tracking of complete DBs.
	BackupFormatDescriptorTrackingVersion uint32 = 1
)

const (
	backupOptRevisionHistory = "revision_history"
	localityURLParam         = "COCKROACH_LOCALITY"
	defaultLocalityValue     = "default"
)

var useTBI = settings.RegisterBoolSetting(
	"kv.bulk_io_write.experimental_incremental_export_enabled",
	"use experimental time-bound file filter when exporting in BACKUP",
	false,
)

var backupOptionExpectValues = map[string]sql.KVStringOptValidate{
	backupOptRevisionHistory: sql.KVStringOptRequireNoValue,
}

// BackupCheckpointInterval is the interval at which backup progress is saved
// to durable storage.
var BackupCheckpointInterval = time.Minute

// ReadBackupDescriptorFromURI creates an export store from the given URI, then
// reads and unmarshals a BackupDescriptor at the standard location in the
// export storage.
func ReadBackupDescriptorFromURI(
	ctx context.Context, uri string, makeExternalStorageFromURI cloud.ExternalStorageFromURIFactory,
) (BackupDescriptor, error) {
	exportStore, err := makeExternalStorageFromURI(ctx, uri)

	if err != nil {
		return BackupDescriptor{}, err
	}
	defer exportStore.Close()
	backupDesc, err := readBackupDescriptor(ctx, exportStore, BackupDescriptorName)
	if err != nil {
		backupManifest, manifestErr := readBackupDescriptor(ctx, exportStore, BackupManifestName)
		if manifestErr != nil {
			return BackupDescriptor{}, err
		}
		backupDesc = backupManifest
	}
	backupDesc.Dir = exportStore.Conf()
	// TODO(dan): Sanity check this BackupDescriptor: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return backupDesc, nil
}

// readBackupDescriptor reads and unmarshals a BackupDescriptor from filename in
// the provided export store.
func readBackupDescriptor(
	ctx context.Context, exportStore cloud.ExternalStorage, filename string,
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
	for _, d := range backupDesc.Descriptors {
		// Calls to GetTable are generally frowned upon.
		// This specific call exists to provide backwards compatibility with
		// backups created prior to version 19.1. Starting in v19.1 the
		// ModificationTime is always written in backups for all versions
		// of table descriptors. In earlier cockroach versions only later
		// table descriptor versions contain a non-empty ModificationTime.
		// Later versions of CockroachDB use the MVCC timestamp to fill in
		// the ModificationTime for table descriptors. When performing a restore
		// we no longer have access to that MVCC timestamp but we can set it
		// to a value we know will be safe.
		if t := d.GetTable(); t == nil {
			continue
		} else if t.Version == 1 && t.ModificationTime.IsEmpty() {
			t.ModificationTime = hlc.Timestamp{WallTime: 1}
		}
	}
	return backupDesc, err
}

func readBackupPartitionDescriptor(
	ctx context.Context, exportStore cloud.ExternalStorage, filename string,
) (BackupPartitionDescriptor, error) {
	r, err := exportStore.ReadFile(ctx, filename)
	if err != nil {
		return BackupPartitionDescriptor{}, err
	}
	defer r.Close()
	descBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return BackupPartitionDescriptor{}, err
	}
	var backupDesc BackupPartitionDescriptor
	if err := protoutil.Unmarshal(descBytes, &backupDesc); err != nil {
		return BackupPartitionDescriptor{}, err
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
		if t := i.Table(hlc.Timestamp{}); t != nil {
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
			if table := i.Table(hlc.Timestamp{}); table != nil {
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
			if table := change.Desc.Table(hlc.Timestamp{}); table != nil {
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
				t := desc.Table(rev.Timestamp)
				if t != nil && t.ReplacementOf.ID != sqlbase.InvalidID {
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
		return nil, err
	}

	sqlDescs := make([]sqlbase.Descriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&sqlDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if row.Value != nil {
			sqlDescs[i].Table(row.Value.Timestamp)
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
		return nil, errors.Wrapf(err,
			"unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal range descriptor", row.Key)
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
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.ID, indexID: index.ID}] = true
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		if tbl := rev.Desc.Table(hlc.Timestamp{}); tbl != nil {
			for _, idx := range tbl.AllNonDropIndexes() {
				key := tableAndIndex{tableID: tbl.ID, indexID: idx.ID}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(idx.ID)), false); err != nil {
						panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
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

// coveringFromSpans creates an interval.Covering with a fixed payload from a
// slice of roachpb.Spans.
func coveringFromSpans(spans []roachpb.Span, payload interface{}) covering.Covering {
	var c covering.Covering
	for _, span := range spans {
		c = append(c, covering.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: payload,
		})
	}
	return c
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

	var rangeCovering covering.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start: []byte(rangeDesc.StartKey),
			End:   []byte(rangeDesc.EndKey),
		})
	}

	splits := covering.OverlapCoveringMerge(
		[]covering.Covering{includeCovering, excludeCovering, rangeCovering},
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

func optsToKVOptions(opts map[string]string) tree.KVOptions {
	if len(opts) == 0 {
		return nil
	}
	sortedOpts := make([]string, 0, len(opts))
	for k := range opts {
		sortedOpts = append(sortedOpts, k)
	}
	sort.Strings(sortedOpts)
	kvopts := make(tree.KVOptions, 0, len(opts))
	for _, k := range sortedOpts {
		opt := tree.KVOption{Key: tree.Name(k)}
		if v := opts[k]; v != "" {
			opt.Value = tree.NewDString(v)
		}
		kvopts = append(kvopts, opt)
	}
	return kvopts
}

func backupJobDescription(
	p sql.PlanHookState,
	backup *tree.Backup,
	to []string,
	incrementalFrom []string,
	opts map[string]string,
) (string, error) {
	b := &tree.Backup{
		AsOf:    backup.AsOf,
		Options: optsToKVOptions(opts),
		Targets: backup.Targets,
	}

	for _, t := range to {
		sanitizedTo, err := cloud.SanitizeExternalStorageURI(t)
		if err != nil {
			return "", err
		}
		b.To = append(b.To, tree.NewDString(sanitizedTo))
	}

	for _, from := range incrementalFrom {
		sanitizedFrom, err := cloud.SanitizeExternalStorageURI(from)
		if err != nil {
			return "", err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, tree.NewDString(sanitizedFrom))
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(b, ann), nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
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
	settings *cluster.Settings,
	exportStore cloud.ExternalStorage,
	filename string,
	desc *BackupDescriptor,
) error {
	sort.Sort(BackupFileDescriptors(desc.Files))

	// When writing a backup descriptor, make sure to downgrade any new-style FKs
	// when we're in the 19.1/2 mixed state so that 19.1 clusters can still
	// restore backups taken on a 19.1/2 mixed cluster.
	// TODO(lucy, jordan): Remove in 20.1.
	downgradedDesc, err := maybeDowngradeTableDescsInBackupDescriptor(ctx, settings, desc)
	if err != nil {
		return err
	}

	descBuf, err := protoutil.Marshal(downgradedDesc)
	if err != nil {
		return err
	}
	return exportStore.WriteFile(ctx, filename, bytes.NewReader(descBuf))
}

// writeBackupPartitionDescriptor writes metadata (containing a locality KV and
// partial file listing) for a partitioned BACKUP to one of the stores in the
// backup.
func writeBackupPartitionDescriptor(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	filename string,
	desc *BackupPartitionDescriptor,
) error {
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
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *client.Txn) error {
			var err error
			txn.SetFixedTimestamp(ctx, asOf)
			allDescs, err = allSQLDescriptors(ctx, txn)
			return err
		}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// ResolveTargetsToDescriptors performs name resolution on a set of targets and
// returns the resulting descriptors.
func ResolveTargetsToDescriptors(
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
	defaultStore cloud.ExternalStorage,
	storageByLocalityKV map[string]*roachpb.ExternalStorage,
	job *jobs.Job,
	backupDesc *BackupDescriptor,
	checkpointDesc *BackupDescriptor,
	resultsCh chan<- tree.Datums,
	makeExternalStorage cloud.ExternalStorageFactory,
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
		return mu.exported, err
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

	// Sequential ranges may have clustered leaseholders, for example a
	// geo-partitioned table likely has all the leaseholders for some contiguous
	// span of the table (i.e. a partition) pinned to just the nodes in a region.
	// In such cases, sending spans sequentially may under-utilize the rest of the
	// cluster given that we have a limit on the number of spans we send out at
	// a given time. Randomizing the order of spans should help ensure a more even
	// distribution of work across the cluster regardless of how leaseholders may
	// or may not be clustered.
	rand.Shuffle(len(allSpans), func(i, j int) {
		allSpans[i], allSpans[j] = allSpans[j], allSpans[i]
	})

	progressLogger := jobs.NewChunkProgressLogger(job, len(spans), job.FractionCompleted(), jobs.ProgressUpdateOnly)

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
	maxConcurrentExports := clusterNodeCount(gossip) * int(storage.ExportRequestsLimit.Get(&settings.SV)) * 10
	exportsSem := make(chan struct{}, maxConcurrentExports)

	g := ctxgroup.WithContext(ctx)

	requestFinishedCh := make(chan struct{}, len(spans)) // enough buffer to never block

	// Only start the progress logger if there are spans, otherwise this will
	// block forever. This is needed for TestBackupRestoreResume which doesn't
	// have any spans. Users should never hit this.
	if len(spans) > 0 {
		g.GoCtx(func(ctx context.Context) error {
			return progressLogger.Loop(ctx, requestFinishedCh)
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		for i := range allSpans {
			{
				select {
				case exportsSem <- struct{}{}:
				case <-ctx.Done():
					// Break the for loop to avoid creating more work - the backup
					// has failed because either the context has been canceled or an
					// error has been returned. Either way, Wait() is guaranteed to
					// return an error now.
					return ctx.Err()
				}
			}

			span := allSpans[i]
			g.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				header := roachpb.Header{Timestamp: span.end}
				req := &roachpb.ExportRequest{
					RequestHeader:                       roachpb.RequestHeaderFromSpan(span.span),
					Storage:                             defaultStore.Conf(),
					StorageByLocalityKV:                 storageByLocalityKV,
					StartTime:                           span.start,
					EnableTimeBoundIteratorOptimization: useTBI.Get(&settings.SV),
					MVCCFilter:                          roachpb.MVCCFilter(backupDesc.MVCCFilter),
				}
				rawRes, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
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
						LocalityKV:  file.LocalityKV,
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
						ctx, settings, defaultStore, BackupDescriptorCheckpointName, backupDesc,
					)
					checkpointMu.Unlock()
					if err != nil {
						log.Errorf(ctx, "unable to checkpoint backup descriptor: %+v", err)
					}
				}
				return nil
			})
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return mu.exported, errors.Wrapf(err, "exporting %d ranges", errors.Safe(len(spans)))
	}

	// No more concurrency, so no need to acquire locks below.

	backupDesc.Files = mu.files
	backupDesc.EntryCounts = mu.exported

	backupID := uuid.MakeV4()
	backupDesc.ID = backupID
	// Write additional partial descriptors to each node for partitioned backups.
	if len(storageByLocalityKV) > 0 {
		filesByLocalityKV := make(map[string][]BackupDescriptor_File)
		for i := range mu.files {
			file := &mu.files[i]
			filesByLocalityKV[file.LocalityKV] = append(filesByLocalityKV[file.LocalityKV], *file)
		}

		nextPartitionedDescFilenameID := 1
		for kv, conf := range storageByLocalityKV {
			backupDesc.LocalityKVs = append(backupDesc.LocalityKVs, kv)
			// Set a unique filename for each partition backup descriptor. The ID
			// ensures uniqueness, and the kv string appended to the end is for
			// readability.
			filename := fmt.Sprintf("%s_%d_%s",
				BackupPartitionDescriptorPrefix, nextPartitionedDescFilenameID, sanitizeLocalityKV(kv))
			nextPartitionedDescFilenameID++
			backupDesc.PartitionDescriptorFilenames = append(backupDesc.PartitionDescriptorFilenames, filename)
			desc := BackupPartitionDescriptor{
				LocalityKV: kv,
				Files:      filesByLocalityKV[kv],
				BackupID:   backupID,
			}

			if err := func() error {
				store, err := makeExternalStorage(ctx, *conf)
				if err != nil {
					return err
				}
				defer store.Close()
				return writeBackupPartitionDescriptor(ctx, store, filename, &desc)
			}(); err != nil {
				return mu.exported, err
			}
		}
	}

	if err := writeBackupDescriptor(ctx, settings, defaultStore, BackupDescriptorName, backupDesc); err != nil {
		return mu.exported, err
	}

	return mu.exported, nil
}

// sanitizeLocalityKV returns a sanitized version of the input string where all
// characters that are not alphanumeric or -, =, or _ are replaced with _.
func sanitizeLocalityKV(kv string) string {
	sanitizedKV := make([]byte, len(kv))
	for i := 0; i < len(kv); i++ {
		if (kv[i] >= 'a' && kv[i] <= 'z') ||
			(kv[i] >= 'A' && kv[i] <= 'Z') ||
			(kv[i] >= '0' && kv[i] <= '9') || kv[i] == '-' || kv[i] == '=' {
			sanitizedKV[i] = kv[i]
		} else {
			sanitizedKV[i] = '_'
		}
	}
	return string(sanitizedKV)
}

// VerifyUsableExportTarget ensures that the target location does not already
// contain a BACKUP or checkpoint and writes an empty checkpoint, both verifying
// that the location is writable and locking out accidental concurrent
// operations on that location if subsequently try this check. Callers must
// clean up the written checkpoint file (BackupDescriptorCheckpointName) only
// after writing to the backup file location (BackupDescriptorName).
func VerifyUsableExportTarget(
	ctx context.Context,
	settings *cluster.Settings,
	exportStore cloud.ExternalStorage,
	readable string,
) error {
	if r, err := exportStore.ReadFile(ctx, BackupDescriptorName); err == nil {
		// TODO(dt): If we audit exactly what not-exists error each ExternalStorage
		// returns (and then wrap/tag them), we could narrow this check.
		r.Close()
		return pgerror.Newf(pgcode.DuplicateFile,
			"%s already contains a %s file",
			readable, BackupDescriptorName)
	}
	if r, err := exportStore.ReadFile(ctx, BackupManifestName); err == nil {
		// TODO(dt): If we audit exactly what not-exists error each ExternalStorage
		// returns (and then wrap/tag them), we could narrow this check.
		r.Close()
		return pgerror.Newf(pgcode.DuplicateFile,
			"%s already contains a %s file",
			readable, BackupManifestName)
	}
	if r, err := exportStore.ReadFile(ctx, BackupDescriptorCheckpointName); err == nil {
		r.Close()
		return pgerror.Newf(pgcode.DuplicateFile,
			"%s already contains a %s file (is another operation already in progress?)",
			readable, BackupDescriptorCheckpointName)
	}
	if err := writeBackupDescriptor(
		ctx, settings, exportStore, BackupDescriptorCheckpointName, &BackupDescriptor{},
	); err != nil {
		return errors.Wrapf(err, "cannot write to %s", readable)
	}
	return nil
}

// backupPlanHook implements PlanHookFn.
func backupPlanHook(
	_ context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	backupStmt, ok := stmt.(*tree.Backup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	toFn, err := p.TypeAsStringArray(tree.Exprs(backupStmt.To), "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(backupStmt.IncrementalFrom, "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}
	optsFn, err := p.TypeAsStringOpts(backupStmt.Options, backupOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
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

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "BACKUP",
		); err != nil {
			return err
		}

		if err := p.RequireAdminRole(ctx, "BACKUP"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("BACKUP cannot be used inside a transaction")
		}

		to, err := toFn()
		if err != nil {
			return err
		}
		if len(to) > 1 &&
			!cluster.Version.IsActive(ctx, p.ExecCfg().Settings, cluster.VersionPartitionedBackup) {
			return errors.Errorf("partitioned backups can only be made on a cluster that has been fully upgraded to version 19.2")
		}

		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return err
		}

		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = p.EvalAsOfTimestamp(backupStmt.AsOf); err != nil {
				return err
			}
		}

		defaultURI, urisByLocalityKV, err := getURIsByLocalityKV(to)
		if err != nil {
			return nil
		}
		defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI)
		if err != nil {
			return err
		}
		defer defaultStore.Close()

		opts, err := optsFn()
		if err != nil {
			return err
		}

		mvccFilter := MVCCFilter_Latest
		if _, ok := opts[backupOptRevisionHistory]; ok {
			mvccFilter = MVCCFilter_All
		}

		targetDescs, completeDBs, err := ResolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets)
		if err != nil {
			return err
		}

		statsCache := p.ExecCfg().TableStatsCache
		tableStatistics := make([]*stats.TableStatisticProto, 0)
		var tables []*sqlbase.TableDescriptor
		for _, desc := range targetDescs {
			if dbDesc := desc.GetDatabase(); dbDesc != nil {
				if err := p.CheckPrivilege(ctx, dbDesc, privilege.SELECT); err != nil {
					return err
				}
			}
			if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
				if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
					return err
				}
				tables = append(tables, tableDesc)

				// Collect all the table stats for this table.
				tableStatisticsAcc, err := statsCache.GetTableStats(ctx, tableDesc.GetID())
				if err != nil {
					return err
				}
				for i := range tableStatisticsAcc {
					tableStatistics = append(tableStatistics, &tableStatisticsAcc[i].TableStatisticProto)
				}
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
				// TODO(lucy): We may want to upgrade the table descs to the newer
				// foreign key representation here, in case there are backups from an
				// older cluster. Keeping the descriptors as they are works for now
				// since all we need to do is get the past backups' table/index spans,
				// but it will be safer for future code to avoid having older-style
				// descriptors around.
				desc, err := ReadBackupDescriptorFromURI(ctx, uri, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI)
				if err != nil {
					return errors.Wrapf(err, "failed to read backup from %q", uri)
				}
				// IDs are how we identify tables, and those are only meaningful in the
				// context of their own cluster, so we need to ensure we only allow
				// incremental previous backups that we created.
				if !desc.ClusterID.Equal(clusterID) {
					return errors.Newf("previous BACKUP %q belongs to cluster %s", uri, desc.ClusterID.String())
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
				if t := d.Table(hlc.Timestamp{}); t != nil {
					tablesInPrev[t.ID] = struct{}{}
				}
			}
			for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
				dbsInPrev[d] = struct{}{}
			}

			for _, d := range targetDescs {
				if t := d.Table(hlc.Timestamp{}); t != nil {
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
			_, coveredTime, err := makeImportSpans(
				spans,
				prevBackups,
				nil, /*backupLocalityInfo*/
				keys.MinKey,
				func(span covering.Range, start, end hlc.Timestamp) error {
					if (start == hlc.Timestamp{}) {
						newSpans = append(newSpans, roachpb.Span{Key: span.Start, EndKey: span.End})
						return nil
					}
					return errOnMissingRange(span, start, end)
				},
			)
			if err != nil {
				return errors.Wrapf(err, "invalid previous backups (a new full backup may be required if a table has been created, dropped or truncated)")
			}
			if coveredTime != startTime {
				return errors.Wrapf(err, "expected previous backups to cover until time %v, got %v", startTime, coveredTime)
			}
		}

		// if CompleteDbs is lost by a 1.x node, FormatDescriptorTrackingVersion
		// means that a 2.0 node will disallow `RESTORE DATABASE foo`, but `RESTORE
		// foo.table1, foo.table2...` will still work. MVCCFilter would be
		// mis-handled, but is disallowed above. IntroducedSpans may also be lost by
		// a 1.x node, meaning that if 1.1 nodes may resume a backup, the limitation
		// of requiring full backups after schema changes remains.

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
			Statistics:        tableStatistics,
		}

		// Sanity check: re-run the validation that RESTORE will do, but this time
		// including this backup, to ensure that the this backup plus any previous
		// backups does cover the interval expected.
		if _, coveredEnd, err := makeImportSpans(
			spans,
			append(prevBackups, backupDesc),
			nil, /*backupLocalityInfo*/
			keys.MinKey,
			errOnMissingRange,
		); err != nil {
			return err
		} else if coveredEnd != endTime {
			return errors.Errorf("expected backup (along with any previous backups) to cover to %v, not %v", endTime, coveredEnd)
		}

		// When writing a backup descriptor, make sure to downgrade any new-style FKs
		// when we're in the 19.1/2 mixed state so that 19.1 clusters can still
		// restore backups taken on a 19.1/2 mixed cluster.
		// TODO(lucy, jordan): Remove in 20.1.
		downgradedBackupDesc, err := maybeDowngradeTableDescsInBackupDescriptor(ctx, p.ExecCfg().Settings, &backupDesc)
		if err != nil {
			return err
		}
		descBytes, err := protoutil.Marshal(downgradedBackupDesc)
		if err != nil {
			return err
		}

		description, err := backupJobDescription(p, backupStmt, to, incrementalFrom, opts)
		if err != nil {
			return err
		}

		// TODO (lucy): For partitioned backups, also add verification for other
		// stores we are writing to in addition to the default.
		if err := VerifyUsableExportTarget(ctx, p.ExecCfg().Settings, defaultStore, defaultURI); err != nil {
			return err
		}

		_, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, jobs.Record{
			Description: description,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, sqlDesc := range backupDesc.Descriptors {
					sqlDescIDs = append(sqlDescIDs, sqlDesc.GetID())
				}
				return sqlDescIDs
			}(),
			Details: jobspb.BackupDetails{
				StartTime:        startTime,
				EndTime:          endTime,
				URI:              defaultURI,
				URIsByLocalityKV: urisByLocalityKV,
				BackupDescriptor: descBytes,
			},
			Progress: jobspb.BackupProgress{},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, header, nil, false, nil
}

type backupResumer struct {
	job                 *jobs.Job
	settings            *cluster.Settings
	res                 roachpb.BulkOpSummary
	makeExternalStorage cloud.ExternalStorageFactory
}

// Resume is part of the jobs.Resumer interface.
func (b *backupResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	details := b.job.Details().(jobspb.BackupDetails)
	p := phs.(sql.PlanHookState)
	b.makeExternalStorage = p.ExecCfg().DistSQLSrv.ExternalStorage

	if len(details.BackupDescriptor) == 0 {
		return errors.Newf("missing backup descriptor; cannot resume a backup from an older version")
	}

	var backupDesc BackupDescriptor
	if err := protoutil.Unmarshal(details.BackupDescriptor, &backupDesc); err != nil {
		return pgerror.Wrapf(err, pgcode.DataCorrupted,
			"unmarshal backup descriptor")
	}
	// For all backups, partitioned or not, the main BACKUP manifest is stored at
	// details.URI.
	defaultConf, err := cloud.ExternalStorageConfFromURI(details.URI)
	if err != nil {
		return errors.Wrapf(err, "export configuration")
	}
	defaultStore, err := b.makeExternalStorage(ctx, defaultConf)
	if err != nil {
		return errors.Wrapf(err, "make storage")
	}
	storageByLocalityKV := make(map[string]*roachpb.ExternalStorage)
	for kv, uri := range details.URIsByLocalityKV {
		conf, err := cloud.ExternalStorageConfFromURI(uri)
		if err != nil {
			return err
		}
		storageByLocalityKV[kv] = &conf
	}
	var checkpointDesc *BackupDescriptor
	// We don't read the table descriptors from the backup descriptor, but
	// they could be using either the new or the old foreign key
	// representations. We should just preserve whatever representation the
	// table descriptors were using and leave them alone.
	if desc, err := readBackupDescriptor(ctx, defaultStore, BackupDescriptorCheckpointName); err == nil {
		// If the checkpoint is from a different cluster, it's meaningless to us.
		// More likely though are dummy/lock-out checkpoints with no ClusterID.
		if desc.ClusterID.Equal(p.ExecCfg().ClusterID()) {
			checkpointDesc = &desc
		}
	} else {
		// TODO(benesch): distinguish between a missing checkpoint, which simply
		// indicates the prior backup attempt made no progress, and a corrupted
		// checkpoint, which is more troubling. Sadly, storageccl doesn't provide a
		// "not found" error that's consistent across all ExternalStorage
		// implementations.
		log.Warningf(ctx, "unable to load backup checkpoint while resuming job %d: %v", *b.job.ID(), err)
	}
	res, err := backup(
		ctx,
		p.ExecCfg().DB,
		p.ExecCfg().Gossip,
		p.ExecCfg().Settings,
		defaultStore,
		storageByLocalityKV,
		b.job,
		&backupDesc,
		checkpointDesc,
		resultsCh,
		b.makeExternalStorage,
	)
	b.res = res
	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (b *backupResumer) OnFailOrCancel(context.Context, *client.Txn) error { return nil }

// OnSuccess is part of the jobs.Resumer interface.
func (b *backupResumer) OnSuccess(context.Context, *client.Txn) error { return nil }

// OnTerminal is part of the jobs.Resumer interface.
func (b *backupResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	// Attempt to delete BACKUP-CHECKPOINT.
	if err := func() error {
		details := b.job.Details().(jobspb.BackupDetails)
		// For all backups, partitioned or not, the main BACKUP manifest is stored at
		// details.URI.
		conf, err := cloud.ExternalStorageConfFromURI(details.URI)
		if err != nil {
			return err
		}
		exportStore, err := b.makeExternalStorage(ctx, conf)
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
			tree.NewDInt(tree.DInt(*b.job.ID())),
			tree.NewDString(string(jobs.StatusSucceeded)),
			tree.NewDFloat(tree.DFloat(1.0)),
			tree.NewDInt(tree.DInt(b.res.Rows)),
			tree.NewDInt(tree.DInt(b.res.IndexEntries)),
			tree.NewDInt(tree.DInt(b.res.SystemRecords)),
			tree.NewDInt(tree.DInt(b.res.DataSize)),
		}
	}
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
		RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey},
		StartTime:     startTime,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
		OmitChecksum:  true,
	}
	resp, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
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
		if err := sst.Iterate(startKey, endKey, func(kv engine.MVCCKeyValue) (bool, error) {
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

var _ jobs.Resumer = &backupResumer{}

func init() {
	sql.AddPlanHook(backupPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeBackup,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &backupResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}

// getURIsByLocalityKV takes a slice of URIs for a single (possibly partitioned)
// backup, and returns the default backup destination URI and a map of all other
// URIs by locality KV. The URIs in the result do not include the
// COCKROACH_LOCALITY parameter.
func getURIsByLocalityKV(to []string) (string, map[string]string, error) {
	localityAndBaseURI := func(uri string) (string, string, error) {
		parsedURI, err := url.Parse(uri)
		if err != nil {
			return "", "", err
		}
		q := parsedURI.Query()
		localityKV := q.Get(localityURLParam)
		// Remove the backup locality parameter.
		q.Del(localityURLParam)
		parsedURI.RawQuery = q.Encode()
		baseURI := parsedURI.String()
		return localityKV, baseURI, nil
	}

	urisByLocalityKV := make(map[string]string)
	if len(to) == 1 {
		localityKV, baseURI, err := localityAndBaseURI(to[0])
		if err != nil {
			return "", nil, err
		}
		if localityKV != "" && localityKV != defaultLocalityValue {
			return "", nil, errors.Errorf("%s %s is invalid for a single BACKUP location",
				localityURLParam, localityKV)
		}
		return baseURI, urisByLocalityKV, nil
	}

	var defaultURI string
	for _, uri := range to {
		localityKV, baseURI, err := localityAndBaseURI(uri)
		if err != nil {
			return "", nil, err
		}
		if localityKV == "" {
			return "", nil, errors.Errorf(
				"multiple URLs are provided for partitioned BACKUP, but %s is not specified",
				localityURLParam,
			)
		}
		if localityKV == defaultLocalityValue {
			if defaultURI != "" {
				return "", nil, errors.Errorf("multiple default URLs provided for partition backup")
			}
			defaultURI = baseURI
		} else {
			kv := roachpb.Tier{}
			if err := kv.FromString(localityKV); err != nil {
				return "", nil, errors.Wrap(err, "failed to parse backup locality")
			}
			if _, ok := urisByLocalityKV[localityKV]; ok {
				return "", nil, errors.Errorf("duplicate URIs for locality %s", localityKV)
			}
			urisByLocalityKV[localityKV] = baseURI
		}
	}
	if defaultURI == "" {
		return "", nil, errors.Errorf("no default URL provided for partitioned backup")
	}
	return defaultURI, urisByLocalityKV, nil
}

// maybeDowngradeTableDescsInBackupDescriptor returns the backup descriptor
// with its table descriptors downgraded to the older 19.1-style foreign key
// representation, if they are not already downgraded, and if the cluster is not
// fully upgraded to 19.2. It returns a *shallow* copy to avoid mutating the
// original backup descriptor. This function facilitates writing 19.1-compatible
// backups when the cluster hasn't been fully upgraded.
// TODO(lucy, jordan): Remove in 20.1.
func maybeDowngradeTableDescsInBackupDescriptor(
	ctx context.Context, settings *cluster.Settings, backupDesc *BackupDescriptor,
) (*BackupDescriptor, error) {
	backupDescCopy := &(*backupDesc)
	// Copy Descriptors so we can return a shallow copy without mutating the slice.
	copy(backupDescCopy.Descriptors, backupDesc.Descriptors)
	for i := range backupDesc.Descriptors {
		if tableDesc := backupDesc.Descriptors[i].Table(hlc.Timestamp{}); tableDesc != nil {
			downgraded, newDesc, err := tableDesc.MaybeDowngradeForeignKeyRepresentation(ctx, settings)
			if err != nil {
				return nil, err
			}
			if downgraded {
				backupDescCopy.Descriptors[i] = *sqlbase.WrapDescriptor(newDesc)
			}
		}
	}
	return backupDescCopy, nil
}

// maybeUpgradeTableDescsInBackupDescriptors updates the backup descriptors'
// table descriptors to use the newer 19.2-style foreign key representation,
// if they are not already upgraded. This requires resolving cross-table FK
// references, which is done by looking up all table descriptors across all
// backup descriptors provided. if skipFKsWithNoMatchingTable is set, FKs whose
// "other" table is missing from the set provided are omitted during the
// upgrade, instead of causing an error to be returned.
func maybeUpgradeTableDescsInBackupDescriptors(
	ctx context.Context, backupDescs []BackupDescriptor, skipFKsWithNoMatchingTable bool,
) error {
	protoGetter := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	// Populate the protoGetter with all table descriptors in all backup
	// descriptors so that they can be looked up.
	for _, backupDesc := range backupDescs {
		for _, desc := range backupDesc.Descriptors {
			if table := desc.Table(hlc.Timestamp{}); table != nil {
				protoGetter.Protos[string(sqlbase.MakeDescMetadataKey(table.ID))] =
					sqlbase.WrapDescriptor(protoutil.Clone(table).(*sqlbase.TableDescriptor))
			}
		}
	}

	for i := range backupDescs {
		backupDesc := &backupDescs[i]
		for j := range backupDesc.Descriptors {
			if table := backupDesc.Descriptors[j].Table(hlc.Timestamp{}); table != nil {
				if _, err := table.MaybeUpgradeForeignKeyRepresentation(ctx, protoGetter, skipFKsWithNoMatchingTable); err != nil {
					return err
				}
				// TODO(lucy): Is this necessary?
				backupDesc.Descriptors[j] = *sqlbase.WrapDescriptor(table)
			}
		}
	}
	return nil
}
