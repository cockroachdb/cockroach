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
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/ingesting"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/rewrite"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbackup"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// restoreStatsInsertBatchSize is an arbitrarily chosen value of the number of
// tables we process in a single txn when restoring their table statistics.
var restoreStatsInsertBatchSize = 10

// rewriteBackupSpanKey rewrites a backup span start key for the purposes of
// splitting up the target key-space to send out the actual work of restoring.
//
// Keys for the primary index of the top-level table are rewritten to the just
// the overall start of the table. That is, /Table/51/1 becomes /Table/51.
//
// Any suffix of the key that does is not rewritten by kr's configured rewrites
// is truncated. For instance if a passed span has key /Table/51/1/77#/53/2/1
// but kr only configured with a rewrite for 51, it would return /Table/51/1/77.
// Such span boundaries are usually due to a interleaved table which has since
// been dropped -- any splits that happened to pick one of its rows live on, but
// include an ID of a table that no longer exists.
//
// Note that the actual restore process (i.e the restore processor method which
// writes the KVs) does not use these keys -- they are only used to split the
// key space and distribute those requests, thus truncation is fine. In the rare
// case where multiple backup spans are truncated to the same prefix (i.e.
// entire spans resided under the same interleave parent row) we'll generate
// some no-op splits and route the work to the same range, but the actual
// imported data is unaffected.
func rewriteBackupSpanKey(
	codec keys.SQLCodec, kr *KeyRewriter, key roachpb.Key,
) (roachpb.Key, error) {
	newKey, rewritten, err := kr.RewriteKey(append([]byte(nil), key...))
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	}
	if !rewritten && bytes.Equal(newKey, key) {
		// if nothing was changed, we didn't match the top-level key at all.
		return nil, errors.AssertionFailedf(
			"no rewrite for span start key: %s", key)
	}
	if bytes.HasPrefix(newKey, keys.TenantPrefix) {
		return newKey, nil
	}

	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := codec.DecodeIndexPrefix(newKey); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"could not rewrite span start key: %s", key)
	} else if idx == 1 && len(b) == 0 {
		newKey = codec.TablePrefix(id)
	}
	return newKey, nil
}

func restoreWithRetry(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	numNodes int,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	job *jobs.Job,
	encryption *jobspb.BackupEncryptionOptions,
) (roachpb.RowCount, error) {
	// We retry on pretty generic failures -- any rpc error. If a worker node were
	// to restart, it would produce this kind of error, but there may be other
	// errors that are also rpc errors. Don't retry to aggressively.
	retryOpts := retry.Options{
		MaxBackoff: 1 * time.Second,
		MaxRetries: 5,
	}

	// We want to retry a restore if there are transient failures (i.e. worker nodes
	// dying), so if we receive a retryable error, re-plan and retry the backup.
	var res roachpb.RowCount
	var err error
	for r := retry.StartWithCtx(restoreCtx, retryOpts); r.Next(); {
		res, err = restore(
			restoreCtx,
			execCtx,
			numNodes,
			backupManifests,
			backupLocalityInfo,
			endTime,
			dataToRestore,
			job,
			encryption,
		)
		if err == nil {
			break
		}

		if joberror.IsPermanentBulkJobError(err) {
			return roachpb.RowCount{}, err
		}

		log.Warningf(restoreCtx, `encountered retryable error: %+v`, err)
	}

	if err != nil {
		return roachpb.RowCount{}, errors.Wrap(err, "exhausted retries")
	}
	return res, nil
}

type storeByLocalityKV map[string]roachpb.ExternalStorage

func makeBackupLocalityMap(
	backupLocalityInfos []jobspb.RestoreDetails_BackupLocalityInfo, user security.SQLUsername,
) (map[int]storeByLocalityKV, error) {

	backupLocalityMap := make(map[int]storeByLocalityKV)
	for i, localityInfo := range backupLocalityInfos {
		storesByLocalityKV := make(storeByLocalityKV)
		if localityInfo.URIsByOriginalLocalityKV != nil {
			for kv, uri := range localityInfo.URIsByOriginalLocalityKV {
				conf, err := cloud.ExternalStorageConfFromURI(uri, user)
				if err != nil {
					return nil, errors.Wrap(err,
						"creating locality external storage configuration")
				}
				storesByLocalityKV[kv] = conf
			}
		}
		backupLocalityMap[i] = storesByLocalityKV
	}

	return backupLocalityMap, nil
}

// restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func restore(
	restoreCtx context.Context,
	execCtx sql.JobExecContext,
	numNodes int,
	backupManifests []BackupManifest,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	endTime hlc.Timestamp,
	dataToRestore restorationData,
	job *jobs.Job,
	encryption *jobspb.BackupEncryptionOptions,
) (roachpb.RowCount, error) {
	user := execCtx.User()
	// A note about contexts and spans in this method: the top-level context
	// `restoreCtx` is used for orchestration logging. All operations that carry
	// out work get their individual contexts.
	emptyRowCount := roachpb.RowCount{}

	// If there isn't any data to restore, then return early.
	if dataToRestore.isEmpty() {
		return emptyRowCount, nil
	}

	// If we've already migrated some of the system tables we're about to
	// restore, this implies that a previous attempt restored all of this data.
	// We want to avoid restoring again since we'll be shadowing migrated keys.
	details := job.Details().(jobspb.RestoreDetails)
	if alreadyMigrated := checkForMigratedData(details, dataToRestore); alreadyMigrated {
		return emptyRowCount, nil
	}

	mu := struct {
		syncutil.Mutex
		highWaterMark     int
		res               roachpb.RowCount
		requestsCompleted []bool
	}{
		highWaterMark: -1,
	}

	backupLocalityMap, err := makeBackupLocalityMap(backupLocalityInfo, user)
	if err != nil {
		return emptyRowCount, errors.Wrap(err, "resolving locality locations")
	}

	if err := checkCoverage(restoreCtx, dataToRestore.getSpans(), backupManifests); err != nil {
		return emptyRowCount, err
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	highWaterMark := job.Progress().Details.(*jobspb.Progress_Restore).Restore.HighWater

	importSpans := makeSimpleImportSpans(dataToRestore.getSpans(), backupManifests, backupLocalityMap,
		highWaterMark)

	if len(importSpans) == 0 {
		// There are no files to restore.
		return emptyRowCount, nil
	}

	for i := range importSpans {
		importSpans[i].ProgressIdx = int64(i)
	}
	mu.requestsCompleted = make([]bool, len(importSpans))

	// TODO(pbardea): This not super principled. I just wanted something that
	// wasn't a constant and grew slower than linear with the length of
	// importSpans. It seems to be working well for BenchmarkRestore2TB but
	// worth revisiting.
	// It tries to take the cluster size into account so that larger clusters
	// distribute more chunks amongst them so that after scattering there isn't
	// a large varience in the distribution of entries.
	chunkSize := int(math.Sqrt(float64(len(importSpans)))) / numNodes
	if chunkSize == 0 {
		chunkSize = 1
	}
	importSpanChunks := make([][]execinfrapb.RestoreSpanEntry, 0, len(importSpans)/chunkSize)
	for start := 0; start < len(importSpans); {
		importSpanChunk := importSpans[start:]
		end := start + chunkSize
		if end < len(importSpans) {
			importSpanChunk = importSpans[start:end]
		}
		importSpanChunks = append(importSpanChunks, importSpanChunk)
		start = end
	}

	requestFinishedCh := make(chan struct{}, len(importSpans)) // enough buffer to never block
	progCh := make(chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	// tasks are the concurrent tasks that are run during the restore.
	var tasks []func(ctx context.Context) error
	if dataToRestore.isMainBundle() {
		// Only update the job progress on the main data bundle. This should account
		// for the bulk of the data to restore. Other data (e.g. zone configs in
		// cluster restores) may be restored first. When restoring that data, we
		// don't want to update the high-water mark key, so instead progress is just
		// defined on the main data bundle (of which there should only be one).
		progressLogger := jobs.NewChunkProgressLogger(job, len(importSpans), job.FractionCompleted(),
			func(progressedCtx context.Context, details jobspb.ProgressDetails) {
				switch d := details.(type) {
				case *jobspb.Progress_Restore:
					mu.Lock()
					if mu.highWaterMark >= 0 {
						d.Restore.HighWater = importSpans[mu.highWaterMark].Span.Key
					}
					mu.Unlock()
				default:
					log.Errorf(progressedCtx, "job payload had unexpected type %T", d)
				}
			})

		jobProgressLoop := func(ctx context.Context) error {
			ctx, progressSpan := tracing.ChildSpan(ctx, "progress-log")
			defer progressSpan.Finish()
			return progressLogger.Loop(ctx, requestFinishedCh)
		}
		tasks = append(tasks, jobProgressLoop)
	}

	jobCheckpointLoop := func(ctx context.Context) error {
		defer close(requestFinishedCh)
		// When a processor is done importing a span, it will send a progress update
		// to progCh.
		for progress := range progCh {
			mu.Lock()
			var progDetails RestoreProgress
			if err := pbtypes.UnmarshalAny(&progress.ProgressDetails, &progDetails); err != nil {
				log.Errorf(ctx, "unable to unmarshal restore progress details: %+v", err)
			}

			mu.res.Add(progDetails.Summary)
			idx := progDetails.ProgressIdx

			// Assert that we're actually marking the correct span done. See #23977.
			if !importSpans[progDetails.ProgressIdx].Span.Key.Equal(progDetails.DataSpan.Key) {
				mu.Unlock()
				return errors.Newf("request %d for span %v does not match import span for same idx: %v",
					idx, progDetails.DataSpan, importSpans[idx],
				)
			}
			mu.requestsCompleted[idx] = true
			for j := mu.highWaterMark + 1; j < len(mu.requestsCompleted) && mu.requestsCompleted[j]; j++ {
				mu.highWaterMark = j
			}
			mu.Unlock()

			// Signal that the processor has finished importing a span, to update job
			// progress.
			requestFinishedCh <- struct{}{}
		}
		return nil
	}
	tasks = append(tasks, jobCheckpointLoop)

	runRestore := func(ctx context.Context) error {
		return distRestore(
			ctx,
			execCtx,
			int64(job.ID()),
			importSpanChunks,
			dataToRestore.getPKIDs(),
			encryption,
			dataToRestore.getRekeys(),
			dataToRestore.getTenantRekeys(),
			endTime,
			progCh,
		)
	}
	tasks = append(tasks, runRestore)

	if err := ctxgroup.GoAndWait(restoreCtx, tasks...); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return emptyRowCount, errors.Wrapf(err, "importing %d ranges", len(importSpans))
	}

	return mu.res, nil
}

// loadBackupSQLDescs extracts the backup descriptors, the latest backup
// descriptor, and all the Descriptors for a backup to be restored. It upgrades
// the table descriptors to the new FK representation if necessary. FKs that
// can't be restored because the necessary tables are missing are omitted; if
// skip_missing_foreign_keys was set, we should have aborted the RESTORE and
// returned an error prior to this.
// TODO(anzoteh96): this method returns two things: backup manifests
// and the descriptors of the relevant manifests. Ideally, this should
// be broken down into two methods.
func loadBackupSQLDescs(
	ctx context.Context,
	mem *mon.BoundAccount,
	p sql.JobExecContext,
	details jobspb.RestoreDetails,
	encryption *jobspb.BackupEncryptionOptions,
) ([]BackupManifest, BackupManifest, []catalog.Descriptor, int64, error) {
	backupManifests, sz, err := loadBackupManifests(ctx, mem, details.URIs,
		p.User(), p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, encryption)
	if err != nil {
		return nil, BackupManifest{}, nil, 0, err
	}

	allDescs, latestBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, details.EndTime)

	for _, m := range details.DatabaseModifiers {
		for _, typ := range m.ExtraTypeDescs {
			allDescs = append(allDescs, typedesc.NewBuilder(typ).BuildCreatedMutableType())
		}
	}

	var sqlDescs []catalog.Descriptor
	for _, desc := range allDescs {
		id := desc.GetID()
		switch desc := desc.(type) {
		case *dbdesc.Mutable:
			if m, ok := details.DatabaseModifiers[id]; ok {
				desc.SetRegionConfig(m.RegionConfig)
			}
		}
		if _, ok := details.DescriptorRewrites[id]; ok {
			sqlDescs = append(sqlDescs, desc)
		}
	}

	if err := maybeUpgradeDescriptors(sqlDescs, true /* skipFKsWithNoMatchingTable */); err != nil {
		mem.Shrink(ctx, sz)
		return nil, BackupManifest{}, nil, 0, err
	}

	return backupManifests, latestBackupManifest, sqlDescs, sz, nil
}

// restoreResumer should only store a reference to the job it's running. State
// should not be stored here, but rather in the job details.
type restoreResumer struct {
	job *jobs.Job

	settings     *cluster.Settings
	execCfg      *sql.ExecutorConfig
	restoreStats roachpb.RowCount

	testingKnobs struct {
		// beforePublishingDescriptors is called right before publishing
		// descriptors, after any data has been restored.
		beforePublishingDescriptors func() error
		// afterPublishingDescriptors is called after committing the transaction to
		// publish new descriptors in the public state.
		afterPublishingDescriptors func() error
		// duringSystemTableRestoration is called once for every system table we
		// restore. It is used to simulate any errors that we may face at this point
		// of the restore.
		duringSystemTableRestoration func(systemTableName string) error
		// afterOfflineTableCreation is called after creating the OFFLINE table
		// descriptors we're ingesting. If an error is returned, we fail the
		// restore.
		afterOfflineTableCreation func() error
		// afterPreRestore runs on cluster restores after restoring the "preRestore"
		// data.
		afterPreRestore func() error
	}
}

// getStatisticsFromBackup retrieves Statistics from backup manifest,
// either through the Statistics field or from the files.
func getStatisticsFromBackup(
	ctx context.Context,
	exportStore cloud.ExternalStorage,
	encryption *jobspb.BackupEncryptionOptions,
	backup BackupManifest,
) ([]*stats.TableStatisticProto, error) {
	// This part deals with pre-20.2 stats format where backup statistics
	// are stored as a field in backup manifests instead of in their
	// individual files.
	if backup.DeprecatedStatistics != nil {
		return backup.DeprecatedStatistics, nil
	}
	tableStatistics := make([]*stats.TableStatisticProto, 0, len(backup.StatisticsFilenames))
	uniqueFileNames := make(map[string]struct{})
	for _, fname := range backup.StatisticsFilenames {
		if _, exists := uniqueFileNames[fname]; !exists {
			uniqueFileNames[fname] = struct{}{}
			myStatsTable, err := readTableStatistics(ctx, exportStore, fname, encryption)
			if err != nil {
				return tableStatistics, err
			}
			tableStatistics = append(tableStatistics, myStatsTable.Statistics...)
		}
	}

	return tableStatistics, nil
}

// remapRelevantStatistics changes the table ID references in the stats
// from those they had in the backed up database to what they should be
// in the restored database.
// It also selects only the statistics which belong to one of the tables
// being restored. If the descriptorRewrites can re-write the table ID, then that
// table is being restored.
func remapRelevantStatistics(
	ctx context.Context,
	tableStatistics []*stats.TableStatisticProto,
	descriptorRewrites jobspb.DescRewriteMap,
	tableDescs []*descpb.TableDescriptor,
) []*stats.TableStatisticProto {
	relevantTableStatistics := make([]*stats.TableStatisticProto, 0, len(tableStatistics))

	tableHasStatsInBackup := make(map[descpb.ID]struct{})
	for _, stat := range tableStatistics {
		tableHasStatsInBackup[stat.TableID] = struct{}{}
		if tableRewrite, ok := descriptorRewrites[stat.TableID]; ok {
			// Statistics imported only when table re-write is present.
			stat.TableID = tableRewrite.ID
			relevantTableStatistics = append(relevantTableStatistics, stat)
		}
	}

	// Check if we are missing stats for any table that is being restored. This
	// could be because we ran into an error when computing stats during the
	// backup.
	for _, desc := range tableDescs {
		if _, ok := tableHasStatsInBackup[desc.GetID()]; !ok {
			log.Warningf(ctx, "statistics for table: %s, table ID: %d not found in the backup. "+
				"Query performance on this table could suffer until statistics are recomputed.",
				desc.GetName(), desc.GetID())
		}
	}

	return relevantTableStatistics
}

// isDatabaseEmpty checks if there exists any tables in the given database.
// It pretends that the ignoredChildren do not exist for the purposes of
// checking if a database is empty.
//
// It is used to construct a transaction which deletes a set of tables as well
// as some empty databases. However, we want to check that the databases are
// empty _after_ the transaction would have completed, so we want to ignore
// the tables that we're deleting in the same transaction. It is done this way
// to avoid having 2 transactions reading and writing the same keys one right
// after the other.
func isDatabaseEmpty(
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	allDescs []catalog.Descriptor,
	ignoredChildren map[descpb.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if desc.GetParentID() == dbID {
			return false, nil
		}
	}
	return true, nil
}

// isSchemaEmpty is like isDatabaseEmpty for schemas: it returns whether the
// schema is empty, disregarding the contents of ignoredChildren.
func isSchemaEmpty(
	ctx context.Context,
	txn *kv.Txn,
	schemaID descpb.ID,
	allDescs []catalog.Descriptor,
	ignoredChildren map[descpb.ID]struct{},
) (bool, error) {
	for _, desc := range allDescs {
		if _, ok := ignoredChildren[desc.GetID()]; ok {
			continue
		}
		if desc.GetParentSchemaID() == schemaID {
			return false, nil
		}
	}
	return true, nil
}

// spansForAllRestoreTableIndexes returns non-overlapping spans for every index
// and table passed in. They would normally overlap if any of them are
// interleaved.
func spansForAllRestoreTableIndexes(
	codec keys.SQLCodec, tables []catalog.TableDescriptor, revs []BackupManifest_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		// We only import spans for physical tables.
		if !table.IsPhysicalTable() {
			continue
		}
		for _, index := range table.ActiveIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(codec, index.GetID())), false); err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.GetID(), indexID: index.GetID()}] = true
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		// If the table was dropped during the last interval, it will have
		// at least 2 revisions, and the first one should have the table in a PUBLIC
		// state. We want (and do) ignore tables that have been dropped for the
		// entire interval. DROPPED tables should never later become PUBLIC.
		// TODO(pbardea): Consider and test the interaction between revision_history
		// backups and OFFLINE tables.
		rawTbl, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTbl != nil && !rawTbl.Dropped() {
			tbl := tabledesc.NewBuilder(rawTbl).BuildImmutableTable()
			// We only import spans for physical tables.
			if !tbl.IsPhysicalTable() {
				continue
			}
			for _, idx := range tbl.ActiveIndexes() {
				key := tableAndIndex{tableID: tbl.GetID(), indexID: idx.GetID()}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(codec, idx.GetID())), false); err != nil {
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

func shouldPreRestore(table *tabledesc.Mutable) bool {
	if table.GetParentID() != keys.SystemDatabaseID {
		return false
	}
	tablesToPreRestore := getSystemTablesToRestoreBeforeData()
	_, ok := tablesToPreRestore[table.GetName()]
	return ok
}

// createImportingDescriptors create the tables that we will restore into. It also
// fetches the information from the old tables that we need for the restore.
func createImportingDescriptors(
	ctx context.Context,
	p sql.JobExecContext,
	backupCodec keys.SQLCodec,
	sqlDescs []catalog.Descriptor,
	r *restoreResumer,
) (*restorationDataBase, *mainRestorationData, error) {
	details := r.job.Details().(jobspb.RestoreDetails)

	var databases []catalog.DatabaseDescriptor
	var writtenTypes []catalog.TypeDescriptor
	var schemas []*schemadesc.Mutable
	var types []*typedesc.Mutable
	// Store the tables as both the concrete mutable structs and the interface
	// to deal with the lack of slice covariance in go. We want the slice of
	// mutable descriptors for rewriting but ultimately want to return the
	// tables as the slice of interfaces.
	var mutableTables []*tabledesc.Mutable
	var mutableDatabases []*dbdesc.Mutable

	oldTableIDs := make([]descpb.ID, 0)

	tables := make([]catalog.TableDescriptor, 0)
	postRestoreTables := make([]catalog.TableDescriptor, 0)

	preRestoreTables := make([]catalog.TableDescriptor, 0)

	for _, desc := range sqlDescs {
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			mut := tabledesc.NewBuilder(desc.TableDesc()).BuildCreatedMutableTable()
			if shouldPreRestore(mut) {
				preRestoreTables = append(preRestoreTables, mut)
			} else {
				postRestoreTables = append(postRestoreTables, mut)
			}
			tables = append(tables, mut)
			mutableTables = append(mutableTables, mut)
			oldTableIDs = append(oldTableIDs, mut.GetID())
		case catalog.DatabaseDescriptor:
			if _, ok := details.DescriptorRewrites[desc.GetID()]; ok {
				mut := dbdesc.NewBuilder(desc.DatabaseDesc()).BuildCreatedMutableDatabase()
				databases = append(databases, mut)
				mutableDatabases = append(mutableDatabases, mut)
			}
		case catalog.SchemaDescriptor:
			mut := schemadesc.NewBuilder(desc.SchemaDesc()).BuildCreatedMutableSchema()
			schemas = append(schemas, mut)
		case catalog.TypeDescriptor:
			mut := typedesc.NewBuilder(desc.TypeDesc()).BuildCreatedMutableType()
			types = append(types, mut)
		}
	}

	tempSystemDBID := tempSystemDatabaseID(details)
	if tempSystemDBID != descpb.InvalidID {
		tempSystemDB := dbdesc.NewInitial(tempSystemDBID, restoreTempSystemDB,
			security.AdminRoleName(), dbdesc.WithPublicSchemaID(keys.SystemPublicSchemaID))
		databases = append(databases, tempSystemDB)
	}

	// We get the spans of the restoring tables _as they appear in the backup_,
	// that is, in the 'old' keyspace, before we reassign the table IDs.
	preRestoreSpans := spansForAllRestoreTableIndexes(backupCodec, preRestoreTables, nil)
	postRestoreSpans := spansForAllRestoreTableIndexes(backupCodec, postRestoreTables, nil)

	log.Eventf(ctx, "starting restore for %d tables", len(mutableTables))

	// Assign new IDs to the database descriptors.
	if err := rewrite.DatabaseDescs(mutableDatabases, details.DescriptorRewrites); err != nil {
		return nil, nil, err
	}

	databaseDescs := make([]*descpb.DatabaseDescriptor, len(mutableDatabases))
	for i, database := range mutableDatabases {
		databaseDescs[i] = database.DatabaseDesc()
	}

	// Collect all schemas that are going to be restored.
	var schemasToWrite []*schemadesc.Mutable
	var writtenSchemas []catalog.SchemaDescriptor
	for i := range schemas {
		sc := schemas[i]
		rw, ok := details.DescriptorRewrites[sc.ID]
		if ok {
			if !rw.ToExisting {
				schemasToWrite = append(schemasToWrite, sc)
				writtenSchemas = append(writtenSchemas, sc)
			}
		}
	}

	if err := rewrite.SchemaDescs(schemasToWrite, details.DescriptorRewrites); err != nil {
		return nil, nil, err
	}

	if err := remapPublicSchemas(ctx, p, mutableDatabases, &schemasToWrite, &writtenSchemas, &details); err != nil {
		return nil, nil, err
	}

	// Assign new IDs and privileges to the tables, and update all references to
	// use the new IDs.
	if err := rewrite.TableDescs(
		mutableTables, details.DescriptorRewrites, details.OverrideDB,
	); err != nil {
		return nil, nil, err
	}
	tableDescs := make([]*descpb.TableDescriptor, len(mutableTables))
	for i, table := range mutableTables {
		tableDescs[i] = table.TableDesc()
	}

	// For each type, we might be writing the type in the backup, or we could be
	// remapping to an existing type descriptor. Split up the descriptors into
	// these two groups.
	var typesToWrite []*typedesc.Mutable
	existingTypeIDs := make(map[descpb.ID]struct{})
	for i := range types {
		typ := types[i]
		rewrite := details.DescriptorRewrites[typ.GetID()]
		if rewrite.ToExisting {
			existingTypeIDs[rewrite.ID] = struct{}{}
		} else {
			typesToWrite = append(typesToWrite, typ)
			writtenTypes = append(writtenTypes, typ)
		}
	}

	// Perform rewrites on ALL type descriptors that are present in the rewrite
	// mapping.
	//
	// `types` contains a mix of existing type descriptors in the restoring
	// cluster, and new type descriptors we will write from the backup.
	//
	// New type descriptors need to be rewritten with their generated IDs before
	// they are written out to disk.
	//
	// Existing type descriptors need to be rewritten to the type ID of the type
	// they are referring to in the restoring cluster. This ID is different from
	// the ID the descriptor had when it was backed up. Changes to existing type
	// descriptors will not be written to disk, and is only for accurate,
	// in-memory resolution hereon out.
	if err := rewrite.TypeDescs(types, details.DescriptorRewrites); err != nil {
		return nil, nil, err
	}

	// Set the new descriptors' states to offline.
	for _, desc := range mutableTables {
		desc.SetOffline("restoring")
	}
	for _, desc := range typesToWrite {
		desc.SetOffline("restoring")
	}
	for _, desc := range schemasToWrite {
		desc.SetOffline("restoring")
	}
	for _, desc := range mutableDatabases {
		desc.SetOffline("restoring")
	}

	if tempSystemDBID != descpb.InvalidID {
		for _, desc := range mutableTables {
			if desc.GetParentID() == tempSystemDBID {
				desc.SetPublic()
			}
		}
	}

	// Collect all types after they have had their ID's rewritten.
	typesByID := make(map[descpb.ID]catalog.TypeDescriptor)
	for i := range types {
		typesByID[types[i].GetID()] = types[i]
	}

	// Collect all databases, for doing lookups of whether a database is new when
	// updating schema references later on.
	dbsByID := make(map[descpb.ID]catalog.DatabaseDescriptor)
	for i := range databases {
		dbsByID[databases[i].GetID()] = databases[i]
	}

	if !details.PrepareCompleted {
		err := sql.DescsTxn(ctx, p.ExecCfg(), func(
			ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
		) error {
			// A couple of pieces of cleanup are required for multi-region databases.
			// First, we need to find all of the MULTIREGION_ENUMs types and remap the
			// IDs stored in the corresponding database descriptors to match the type's
			// new ID. Secondly, we need to rebuild the zone configuration for each
			// multi-region database. We don't perform the zone configuration rebuild on
			// cluster restores, as they will have the zone configurations restored as
			// as the system tables are restored.
			mrEnumsFound := make(map[descpb.ID]descpb.ID)
			for _, t := range typesByID {
				typeDesc := typedesc.NewBuilder(t.TypeDesc()).BuildImmutableType()
				if typeDesc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM {
					// Check to see if we've found more than one multi-region enum on any
					// given database.
					if id, ok := mrEnumsFound[typeDesc.GetParentID()]; ok {
						return errors.AssertionFailedf(
							"unexpectedly found more than one MULTIREGION_ENUM (IDs = %d, %d) "+
								"on database %d during restore", id, typeDesc.GetID(), typeDesc.GetParentID())
					}
					mrEnumsFound[typeDesc.GetParentID()] = typeDesc.GetID()

					if db, ok := dbsByID[typeDesc.GetParentID()]; ok {
						desc := db.DatabaseDesc()
						if desc.RegionConfig == nil {
							return errors.AssertionFailedf(
								"found MULTIREGION_ENUM on non-multi-region database %s", desc.Name)
						}

						// Update the RegionEnumID to record the new multi-region enum ID.
						desc.RegionConfig.RegionEnumID = t.GetID()

						// If we're not in a cluster restore, rebuild the database-level zone
						// configuration.
						if details.DescriptorCoverage != tree.AllDescriptors {
							log.Infof(ctx, "restoring zone configuration for database %d", desc.ID)
							regionNames, err := typeDesc.RegionNames()
							if err != nil {
								return err
							}
							regionConfig := multiregion.MakeRegionConfig(
								regionNames,
								desc.RegionConfig.PrimaryRegion,
								desc.RegionConfig.SurvivalGoal,
								desc.RegionConfig.RegionEnumID,
								desc.RegionConfig.Placement,
							)
							if err := sql.ApplyZoneConfigFromDatabaseRegionConfig(
								ctx,
								desc.GetID(),
								regionConfig,
								txn,
								p.ExecCfg(),
							); err != nil {
								return err
							}
						}
					}
				}
			}

			// Allocate no schedule to the row-level TTL.
			// This will be re-written when the descriptor is published.
			if details.DescriptorCoverage != tree.AllDescriptors {
				for _, table := range mutableTables {
					if table.HasRowLevelTTL() {
						table.RowLevelTTL.ScheduleID = 0
					}
				}
			}

			// Write the new descriptors which are set in the OFFLINE state.
			if err := ingesting.WriteDescriptors(
				ctx, p.ExecCfg().Codec, txn, p.User(), descsCol, databases, writtenSchemas, tables, writtenTypes,
				details.DescriptorCoverage, nil /* extra */, restoreTempSystemDB,
			); err != nil {
				return errors.Wrapf(err, "restoring %d TableDescriptors from %d databases", len(tables), len(databases))
			}

			b := txn.NewBatch()

			// For new schemas with existing parent databases, the schema map on the
			// database descriptor needs to be updated.
			existingDBsWithNewSchemas := make(map[descpb.ID][]catalog.SchemaDescriptor)
			for _, sc := range writtenSchemas {
				parentID := sc.GetParentID()
				if _, ok := dbsByID[parentID]; !ok {
					existingDBsWithNewSchemas[parentID] = append(existingDBsWithNewSchemas[parentID], sc)
				}
			}
			// Write the updated databases.
			for dbID, schemas := range existingDBsWithNewSchemas {
				log.Infof(ctx, "writing %d schema entries to database %d", len(schemas), dbID)
				desc, err := descsCol.GetMutableDescriptorByID(ctx, txn, dbID)
				if err != nil {
					return err
				}
				db := desc.(*dbdesc.Mutable)
				for _, sc := range schemas {
					db.AddSchemaToDatabase(sc.GetName(), descpb.DatabaseDescriptor_SchemaInfo{ID: sc.GetID()})
				}
				if err := descsCol.WriteDescToBatch(
					ctx, false /* kvTrace */, db, b,
				); err != nil {
					return err
				}
			}

			// We could be restoring tables that point to existing types. We need to
			// ensure that those existing types are updated with back references pointing
			// to the new tables being restored.
			for _, table := range mutableTables {
				// Collect all types used by this table.
				_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
					ctx, txn, table.GetParentID(), tree.DatabaseLookupFlags{
						Required:       true,
						AvoidLeased:    true,
						IncludeOffline: true,
					})
				if err != nil {
					return err
				}
				typeIDs, _, err := table.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
					t, ok := typesByID[id]
					if !ok {
						return nil, errors.AssertionFailedf("type with id %d was not found in rewritten type mapping", id)
					}
					return t, nil
				})
				if err != nil {
					return err
				}
				for _, id := range typeIDs {
					// If the type was restored as part of the backup, then the backreference
					// already exists.
					_, ok := existingTypeIDs[id]
					if !ok {
						continue
					}
					// Otherwise, add a backreference to this table.
					typDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
					if err != nil {
						return err
					}
					typDesc.AddReferencingDescriptorID(table.GetID())
					if err := descsCol.WriteDescToBatch(
						ctx, false /* kvTrace */, typDesc, b,
					); err != nil {
						return err
					}
				}
			}
			if err := txn.Run(ctx, b); err != nil {
				return err
			}

			// Now that all of the descriptors have been written to disk, rebuild
			// the zone configurations for any multi-region tables. We only do this
			// in cases where this is not a full cluster restore, because in cluster
			// restore cases, the zone configurations will be restored when the
			// system tables are restored.
			if details.DescriptorCoverage != tree.AllDescriptors {
				for _, table := range tableDescs {
					if lc := table.GetLocalityConfig(); lc != nil {
						_, desc, err := descsCol.GetImmutableDatabaseByID(
							ctx,
							txn,
							table.ParentID,
							tree.DatabaseLookupFlags{
								Required:       true,
								AvoidLeased:    true,
								IncludeOffline: true,
							},
						)
						if err != nil {
							return err
						}
						if desc.GetRegionConfig() == nil {
							return errors.AssertionFailedf(
								"found multi-region table %d in non-multi-region database %d",
								table.ID, table.ParentID)
						}

						mutTable, err := descsCol.GetMutableTableVersionByID(ctx, table.GetID(), txn)
						if err != nil {
							return err
						}

						regionConfig, err := sql.SynthesizeRegionConfig(
							ctx,
							txn,
							desc.GetID(),
							descsCol,
							sql.SynthesizeRegionConfigOptionIncludeOffline,
						)
						if err != nil {
							return err
						}
						if err := sql.ApplyZoneConfigForMultiRegionTable(
							ctx,
							txn,
							p.ExecCfg(),
							regionConfig,
							mutTable,
							sql.ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
						); err != nil {
							return err
						}
					}
				}
			}

			for _, tenant := range details.Tenants {
				// Mark the tenant info as adding.
				tenant.State = descpb.TenantInfo_ADD
				if err := sql.CreateTenantRecord(ctx, p.ExecCfg(), txn, &tenant); err != nil {
					return err
				}
			}

			details.PrepareCompleted = true
			details.DatabaseDescs = databaseDescs
			details.TableDescs = tableDescs
			details.TypeDescs = make([]*descpb.TypeDescriptor, len(typesToWrite))
			for i := range typesToWrite {
				details.TypeDescs[i] = typesToWrite[i].TypeDesc()
			}
			details.SchemaDescs = make([]*descpb.SchemaDescriptor, len(schemasToWrite))
			for i := range schemasToWrite {
				details.SchemaDescs[i] = schemasToWrite[i].SchemaDesc()
			}

			// Update the job once all descs have been prepared for ingestion.
			err := r.job.SetDetails(ctx, txn, details)

			// Emit to the event log now that the job has finished preparing descs.
			emitRestoreJobEvent(ctx, p, jobs.StatusRunning, r.job)

			return err
		})
		if err != nil {
			return nil, nil, err
		}
	}

	// Get TableRekeys to use when importing raw data.
	var rekeys []execinfrapb.TableRekey
	for i := range tables {
		tableToSerialize := tables[i]
		newDescBytes, err := protoutil.Marshal(tableToSerialize.DescriptorProto())
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"marshaling descriptor")
		}
		rekeys = append(rekeys, execinfrapb.TableRekey{
			OldID:   uint32(oldTableIDs[i]),
			NewDesc: newDescBytes,
		})
	}

	pkIDs := make(map[uint64]bool)
	for _, tbl := range tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(tbl.GetID()), uint64(tbl.GetPrimaryIndexID()))] = true
	}

	dataToPreRestore := &restorationDataBase{
		spans:       preRestoreSpans,
		tableRekeys: rekeys,
		pkIDs:       pkIDs,
	}

	dataToRestore := &mainRestorationData{
		restorationDataBase{
			spans:       postRestoreSpans,
			tableRekeys: rekeys,
			pkIDs:       pkIDs,
		},
	}

	if tempSystemDBID != descpb.InvalidID {
		for _, table := range preRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				dataToPreRestore.systemTables = append(dataToPreRestore.systemTables, table)
			}
		}
		for _, table := range postRestoreTables {
			if table.GetParentID() == tempSystemDBID {
				dataToRestore.systemTables = append(dataToRestore.systemTables, table)
			}
		}
	}
	return dataToPreRestore, dataToRestore, nil
}

// remapPublicSchemas is used to create a descriptor backed public schema
// for databases that have virtual public schemas.
// The rewrite map is updated with the new public schema id.
func remapPublicSchemas(
	ctx context.Context,
	p sql.JobExecContext,
	mutableDatabases []*dbdesc.Mutable,
	schemasToWrite *[]*schemadesc.Mutable,
	writtenSchemas *[]catalog.SchemaDescriptor,
	details *jobspb.RestoreDetails,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.PublicSchemasWithDescriptors) {
		// If we're not on PublicSchemasWithDescriptors, there is no work to do as
		// we did not create any public schemas with descriptors.
		return nil
	}
	databaseToPublicSchemaID := make(map[descpb.ID]descpb.ID)
	for _, db := range mutableDatabases {
		if db.HasPublicSchemaWithDescriptor() {
			continue
		}
		// mutableDatabases contains the list of databases being restored,
		// if the database does not have a public schema backed by a descriptor
		// (meaning they were created before 22.1), we need to create a public
		// schema descriptor for it.
		id, err := descidgen.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return err
		}

		db.AddSchemaToDatabase(tree.PublicSchema, descpb.DatabaseDescriptor_SchemaInfo{ID: id})
		// Every database must be initialized with the public schema.
		// Create the SchemaDescriptor.
		publicSchemaPrivileges := catpb.NewPublicSchemaPrivilegeDescriptor()
		publicSchemaDesc := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
			ParentID:   db.GetID(),
			Name:       tree.PublicSchema,
			ID:         id,
			Privileges: publicSchemaPrivileges,
			Version:    1,
		}).BuildCreatedMutableSchema()

		*schemasToWrite = append(*schemasToWrite, publicSchemaDesc)
		*writtenSchemas = append(*writtenSchemas, publicSchemaDesc)
		databaseToPublicSchemaID[db.GetID()] = id
	}

	// Now we need to handle rewriting the table parent schema ids.
	for id, rw := range details.DescriptorRewrites {
		if publicSchemaID, ok := databaseToPublicSchemaID[rw.ParentID]; ok {
			// For all items that were previously mapped to a synthetic public
			// schemas ID, update the ParentSchemaID to be the newly allocated ID.
			//
			// We also have to consider restoring tables from the system table
			// where the system public schema still uses 29 as an ID.
			if details.DescriptorRewrites[id].ParentSchemaID == keys.PublicSchemaIDForBackup ||
				details.DescriptorRewrites[id].ParentSchemaID == descpb.InvalidID {
				details.DescriptorRewrites[id].ParentSchemaID = publicSchemaID
			}
		}
	}

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r *restoreResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if err := r.doResume(ctx, execCtx); err != nil {
		details := r.job.Details().(jobspb.RestoreDetails)
		if details.DebugPauseOn == "error" {
			const errorFmt = "job failed with error (%v) but is being paused due to the %s=%s setting"
			log.Warningf(ctx, errorFmt, err, restoreOptDebugPauseOn, details.DebugPauseOn)

			return jobs.MarkPauseRequestError(errors.Wrapf(err,
				"pausing job due to the %s=%s setting",
				restoreOptDebugPauseOn, details.DebugPauseOn))
		}
		return err
	}

	return nil
}

func (r *restoreResumer) doResume(ctx context.Context, execCtx interface{}) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	p := execCtx.(sql.JobExecContext)
	r.execCfg = p.ExecCfg()

	mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	backupManifests, latestBackupManifest, sqlDescs, memSize, err := loadBackupSQLDescs(
		ctx, &mem, p, details, details.Encryption,
	)
	if err != nil {
		return err
	}
	defer func() {
		mem.Shrink(ctx, memSize)
	}()
	// backupCodec is the codec that was used to encode the keys in the backup. It
	// is the tenant in which the backup was taken.
	backupCodec := keys.SystemSQLCodec
	backupTenantID := roachpb.SystemTenantID

	if len(sqlDescs) != 0 {
		if len(latestBackupManifest.Spans) != 0 && !latestBackupManifest.HasTenants() {
			// If there are no tenant targets, then the entire keyspace covered by
			// Spans must lie in 1 tenant.
			_, backupTenantID, err = keys.DecodeTenantPrefix(latestBackupManifest.Spans[0].Key)
			if err != nil {
				return err
			}
			backupCodec = keys.MakeSQLCodec(backupTenantID)
		}
	}

	lastBackupIndex, err := getBackupIndexAtTime(backupManifests, details.EndTime)
	if err != nil {
		return err
	}
	defaultConf, err := cloud.ExternalStorageConfFromURI(details.URIs[lastBackupIndex], p.User())
	if err != nil {
		return errors.Wrapf(err, "creating external store configuration")
	}
	defaultStore, err := p.ExecCfg().DistSQLSrv.ExternalStorage(ctx, defaultConf)
	if err != nil {
		return err
	}

	preData, mainData, err := createImportingDescriptors(ctx, p, backupCodec, sqlDescs, r)
	if err != nil {
		return err
	}

	if !backupCodec.TenantPrefix().Equal(p.ExecCfg().Codec.TenantPrefix()) {
		// Disallow cluster restores until values like jobs are relocatable.
		if details.DescriptorCoverage == tree.AllDescriptors {
			return unimplemented.NewWithIssuef(62277,
				"cannot cluster RESTORE backups taken from different tenant: %s",
				backupTenantID.String())
		}

		// Ensure old processors fail if this is a previously unsupported restore of
		// a tenant backup by the system tenant, which the old rekey processor would
		// mishandle since it assumed the system tenant always restored tenant keys
		// to tenant prefixes, i.e. as tenant restore.
		if backupTenantID != roachpb.SystemTenantID && p.ExecCfg().Codec.ForSystemTenant() {
			// This empty table rekey acts as a poison-pill, which will be ignored by
			// a current processor but reliably cause an older processor, which would
			// otherwise mishandle tenant-made backup keys, to fail as it will be
			// unable to decode the zero ID table desc.
			preData.tableRekeys = append(preData.tableRekeys, execinfrapb.TableRekey{})
			mainData.tableRekeys = append(preData.tableRekeys, execinfrapb.TableRekey{})
		}
	}

	// If, and only if, the backup was made by a system tenant, can it contain
	// backed up tenants, which the processor needs to know when is rekeying -- if
	// the backup contains tenants, then a key with a tenant prefix should be
	// restored if, and only if, we're restoring that tenant, and restored to a
	// tenant. Otherwise, if this backup was not made by a system tenant, it does
	// not contain tenants, so the rekey will assume if a key has a tenant prefix,
	// it is because the tenant produced the backup, and it should be removed to
	// then decode the remainder of the key. We communicate this distinction to
	// the processor with a special tenant rekey _into_ the system tenant, which
	// would never otherwise be valid. It will discard this rekey but it signals
	// to it that we're rekeying a system-made backup.
	if backupTenantID == roachpb.SystemTenantID {
		preData.tenantRekeys = append(preData.tenantRekeys, isBackupFromSystemTenantRekey)
		mainData.tenantRekeys = append(preData.tenantRekeys, isBackupFromSystemTenantRekey)
	}

	// Refresh the job details since they may have been updated when creating the
	// importing descriptors.
	details = r.job.Details().(jobspb.RestoreDetails)

	if fn := r.testingKnobs.afterOfflineTableCreation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	var remappedStats []*stats.TableStatisticProto
	backupStats, err := getStatisticsFromBackup(ctx, defaultStore, details.Encryption,
		latestBackupManifest)
	if err == nil {
		remappedStats = remapRelevantStatistics(ctx, backupStats, details.DescriptorRewrites,
			details.TableDescs)
	} else {
		// We don't want to fail the restore if we are unable to resolve statistics
		// from the backup, since they can be recomputed after the restore has
		// completed.
		log.Warningf(ctx, "failed to resolve table statistics from backup during restore: %+v",
			err.Error())
	}

	if len(details.TableDescs) == 0 && len(details.Tenants) == 0 && len(details.TypeDescs) == 0 {
		// We have no tables to restore (we are restoring an empty DB).
		// Since we have already created any new databases that we needed,
		// we can return without importing any data.
		log.Warning(ctx, "nothing to restore")
		// The database was created in the offline state and needs to be made
		// public.
		// TODO (lucy): Ideally we'd just create the database in the public state in
		// the first place, as a special case.
		publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
			return r.publishDescriptors(ctx, txn, p.ExecCfg(), p.User(), descsCol, details, nil)
		}
		if err := sql.DescsTxn(ctx, r.execCfg, publishDescriptors); err != nil {
			return err
		}

		p.ExecCfg().JobRegistry.NotifyToAdoptJobs()
		if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
		emitRestoreJobEvent(ctx, p, jobs.StatusSucceeded, r.job)
		return nil
	}

	for _, tenant := range details.Tenants {
		to := roachpb.MakeTenantID(tenant.ID)
		from := to
		if details.PreRewriteTenantId != nil {
			from = *details.PreRewriteTenantId
		}
		mainData.addTenant(from, to)
	}

	numNodes, err := clusterNodeCount(p.ExecCfg().Gossip)
	if err != nil {
		if !build.IsRelease() && p.ExecCfg().Codec.ForSystemTenant() {
			return err
		}
		log.Warningf(ctx, "unable to determine cluster node count: %v", err)
		numNodes = 1
	}

	var resTotal roachpb.RowCount
	if !preData.isEmpty() {
		res, err := restoreWithRetry(
			ctx,
			p,
			numNodes,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			preData,
			r.job,
			details.Encryption,
		)
		if err != nil {
			return err
		}

		resTotal.Add(res)

		if details.DescriptorCoverage == tree.AllDescriptors {
			if err := r.restoreSystemTables(ctx, p.ExecCfg().DB, preData.systemTables); err != nil {
				return err
			}
			// Reload the details as we may have updated the job.
			details = r.job.Details().(jobspb.RestoreDetails)
		}

		if fn := r.testingKnobs.afterPreRestore; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}
	}

	{
		// Restore the main data bundle. We notably only restore the system tables
		// later.
		res, err := restoreWithRetry(
			ctx,
			p,
			numNodes,
			backupManifests,
			details.BackupLocalityInfo,
			details.EndTime,
			mainData,
			r.job,
			details.Encryption,
		)
		if err != nil {
			return err
		}

		resTotal.Add(res)
	}

	if err := insertStats(ctx, r.job, p.ExecCfg(), remappedStats); err != nil {
		return errors.Wrap(err, "inserting table statistics")
	}

	var devalidateIndexes map[descpb.ID][]descpb.IndexID
	if toValidate := len(details.RevalidateIndexes); toValidate > 0 {
		if err := r.job.RunningStatus(ctx, nil /* txn */, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return jobs.RunningStatus(fmt.Sprintf("re-validating %d indexes", toValidate)), nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(r.job.ID()))
		}
		bad, err := revalidateIndexes(ctx, p.ExecCfg(), r.job, details.TableDescs, details.RevalidateIndexes)
		if err != nil {
			return err
		}
		devalidateIndexes = bad
	}

	publishDescriptors := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) (err error) {
		err = r.publishDescriptors(ctx, txn, p.ExecCfg(), p.User(), descsCol, details, devalidateIndexes)
		return err
	}
	if err := sql.DescsTxn(ctx, p.ExecCfg(), publishDescriptors); err != nil {
		return err
	}
	// Reload the details as we may have updated the job.
	details = r.job.Details().(jobspb.RestoreDetails)
	p.ExecCfg().JobRegistry.NotifyToAdoptJobs()

	if details.DescriptorCoverage == tree.AllDescriptors {
		// We restore the system tables from the main data bundle so late because it
		// includes the jobs that are being restored. As soon as we restore these
		// jobs, they become accessible to the user, and may start executing. We
		// need this to happen after the descriptors have been marked public.
		if err := r.restoreSystemTables(ctx, p.ExecCfg().DB, mainData.systemTables); err != nil {
			return err
		}
		// Reload the details as we may have updated the job.
		details = r.job.Details().(jobspb.RestoreDetails)

		if err := r.cleanupTempSystemTables(ctx, nil /* txn */); err != nil {
			return err
		}
	} else if details.RestoreSystemUsers {
		if err := r.restoreSystemUsers(ctx, p.ExecCfg().DB, mainData.systemTables); err != nil {
			return err
		}
		details = r.job.Details().(jobspb.RestoreDetails)

		if err := r.cleanupTempSystemTables(ctx, nil /* txn */); err != nil {
			return err
		}
	}

	if fn := r.testingKnobs.afterPublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	r.notifyStatsRefresherOfNewTables()

	r.restoreStats = resTotal

	// Emit an event now that the restore job has completed.
	emitRestoreJobEvent(ctx, p, jobs.StatusSucceeded, r.job)

	// Collect telemetry.
	{
		telemetry.Count("restore.total.succeeded")
		const mb = 1 << 20
		sizeMb := resTotal.DataSize / mb
		sec := int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds())
		var mbps int64
		if sec > 0 {
			mbps = mb / sec
		}
		telemetry.CountBucketed("restore.duration-sec.succeeded", sec)
		telemetry.CountBucketed("restore.size-mb.full", sizeMb)
		telemetry.CountBucketed("restore.speed-mbps.total", mbps)
		telemetry.CountBucketed("restore.speed-mbps.per-node", mbps/int64(numNodes))
		// Tiny restores may skew throughput numbers due to overhead.
		if sizeMb > 10 {
			telemetry.CountBucketed("restore.speed-mbps.over10mb", mbps)
			telemetry.CountBucketed("restore.speed-mbps.over10mb.per-node", mbps/int64(numNodes))
		}
	}
	return nil
}

func revalidateIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	job *jobs.Job,
	tables []*descpb.TableDescriptor,
	indexIDs []jobspb.RestoreDetails_RevalidateIndex,
) (map[descpb.ID][]descpb.IndexID, error) {
	indexIDsByTable := make(map[descpb.ID]map[descpb.IndexID]struct{})
	for _, idx := range indexIDs {
		if indexIDsByTable[idx.TableID] == nil {
			indexIDsByTable[idx.TableID] = make(map[descpb.IndexID]struct{})
		}
		indexIDsByTable[idx.TableID][idx.IndexID] = struct{}{}
	}

	// We don't actually need the 'historical' read the way the schema change does
	// since our table is offline.
	var runner sqlutil.HistoricalInternalExecTxnRunner = func(ctx context.Context, fn sqlutil.InternalExecFn) error {
		return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			ie := job.MakeSessionBoundInternalExecutor(ctx, sql.NewFakeSessionData(execCfg.SV())).(*sql.InternalExecutor)
			return fn(ctx, txn, ie)
		})
	}

	invalidIndexes := make(map[descpb.ID][]descpb.IndexID)

	for _, tbl := range tables {
		indexes := indexIDsByTable[tbl.ID]
		if len(indexes) == 0 {
			continue
		}
		tableDesc := tabledesc.NewBuilder(tbl).BuildExistingMutableTable()

		var forward, inverted []catalog.Index
		for _, idx := range tableDesc.AllIndexes() {
			if _, ok := indexes[idx.GetID()]; ok {
				switch idx.GetType() {
				case descpb.IndexDescriptor_FORWARD:
					forward = append(forward, idx)
				case descpb.IndexDescriptor_INVERTED:
					inverted = append(inverted, idx)
				}
			}
		}
		if len(forward) > 0 {
			if err := sql.ValidateForwardIndexes(
				ctx,
				tableDesc.MakePublic(),
				forward,
				runner,
				false, /* withFirstMutationPublic */
				true,  /* gatherAllInvalid */
				sessiondata.InternalExecutorOverride{},
			); err != nil {
				if invalid := (sql.InvalidIndexesError{}); errors.As(err, &invalid) {
					invalidIndexes[tableDesc.ID] = invalid.Indexes
				} else {
					return nil, err
				}
			}
		}
		if len(inverted) > 0 {
			if err := sql.ValidateInvertedIndexes(
				ctx,
				execCfg.Codec,
				tableDesc.MakePublic(),
				inverted,
				runner,
				false, /* withFirstMutationPublic */
				true,  /* gatherAllInvalid */
				sessiondata.InternalExecutorOverride{},
			); err != nil {
				if invalid := (sql.InvalidIndexesError{}); errors.As(err, &invalid) {
					invalidIndexes[tableDesc.ID] = append(invalidIndexes[tableDesc.ID], invalid.Indexes...)
				} else {
					return nil, err
				}
			}
		}
	}
	return invalidIndexes, nil
}

// ReportResults implements JobResultsReporter interface.
func (r *restoreResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.restoreStats.Rows)),
		tree.NewDInt(tree.DInt(r.restoreStats.IndexEntries)),
		tree.NewDInt(tree.DInt(r.restoreStats.DataSize)),
	}:
		return nil
	}
}

// Initiate a run of CREATE STATISTICS. We don't know the actual number of
// rows affected per table, so we use a large number because we want to make
// sure that stats always get created/refreshed here.
func (r *restoreResumer) notifyStatsRefresherOfNewTables() {
	details := r.job.Details().(jobspb.RestoreDetails)
	for i := range details.TableDescs {
		desc := tabledesc.NewBuilder(details.TableDescs[i]).BuildImmutableTable()
		r.execCfg.StatsRefresher.NotifyMutation(desc, math.MaxInt32 /* rowsAffected */)
	}
}

// tempSystemDatabaseID returns the ID of the descriptor for the temporary
// system database used in full cluster restores.
// This is the last of the IDs pre-allocated by the restore planner.
// TODO(postamar): Store it directly in the details instead? This is brittle.
func tempSystemDatabaseID(details jobspb.RestoreDetails) descpb.ID {
	if details.DescriptorCoverage != tree.AllDescriptors && !details.RestoreSystemUsers {
		return descpb.InvalidID
	}
	var maxPreAllocatedID descpb.ID
	for id := range details.DescriptorRewrites {
		// This map is never empty in this case (full cluster restore),
		// it contains at minimum the entry for the temporary system database.
		if id > maxPreAllocatedID {
			maxPreAllocatedID = id
		}
	}
	return maxPreAllocatedID
}

// Insert stats re-inserts the table statistics stored in the backup manifest.
func insertStats(
	ctx context.Context,
	job *jobs.Job,
	execCfg *sql.ExecutorConfig,
	latestStats []*stats.TableStatisticProto,
) error {
	details := job.Details().(jobspb.RestoreDetails)
	if details.StatsInserted {
		return nil
	}

	// We could be restoring hundreds of tables, so insert the new stats in
	// batches instead of all in a single, long-running txn. This prevents intent
	// buildup in the face of txn retries.
	for {
		if len(latestStats) == 0 {
			return nil
		}

		if len(latestStats) < restoreStatsInsertBatchSize {
			restoreStatsInsertBatchSize = len(latestStats)
		}

		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := stats.InsertNewStats(ctx, execCfg.Settings, execCfg.InternalExecutor, txn,
				latestStats[:restoreStatsInsertBatchSize]); err != nil {
				return errors.Wrapf(err, "inserting stats from backup")
			}

			// If this is the last batch, mark the stats insertion complete.
			if restoreStatsInsertBatchSize == len(latestStats) {
				details.StatsInserted = true
				if err := job.SetDetails(ctx, txn, details); err != nil {
					return errors.Wrapf(err, "updating job marking stats insertion complete")
				}
			}

			return nil
		}); err != nil {
			return err
		}

		// Truncate the stats that we have inserted in the txn above.
		latestStats = latestStats[restoreStatsInsertBatchSize:]
	}
}

// publishDescriptors updates the RESTORED descriptors' status from OFFLINE to
// PUBLIC. The schema change jobs are returned to be started after the
// transaction commits. The details struct is passed in rather than loaded
// from r.job as the call to r.job.SetDetails will overwrite the job details
// with a new value even if this transaction does not commit.
func (r *restoreResumer) publishDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *sql.ExecutorConfig,
	user security.SQLUsername,
	descsCol *descs.Collection,
	details jobspb.RestoreDetails,
	devalidateIndexes map[descpb.ID][]descpb.IndexID,
) (err error) {
	if details.DescriptorsPublished {
		return nil
	}
	if fn := r.testingKnobs.beforePublishingDescriptors; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	log.VEventf(ctx, 1, "making tables live")

	// Write the new descriptors and flip state over to public so they can be
	// accessed.

	// Pre-fetch all the descriptors into the collection to avoid doing
	// round-trips per descriptor.
	all, err := prefetchDescriptors(ctx, txn, descsCol, details)
	if err != nil {
		return err
	}

	// Create slices of raw descriptors for the restore job details.
	newTables := make([]*descpb.TableDescriptor, 0, len(details.TableDescs))
	newTypes := make([]*descpb.TypeDescriptor, 0, len(details.TypeDescs))
	newSchemas := make([]*descpb.SchemaDescriptor, 0, len(details.SchemaDescs))
	newDBs := make([]*descpb.DatabaseDescriptor, 0, len(details.DatabaseDescs))

	// Go through the descriptors and find any declarative schema change jobs
	// affecting them.
	//
	// If we're restoring all the descriptors, it means we're also restoring the
	// jobs.
	if details.DescriptorCoverage != tree.AllDescriptors {
		if err := scbackup.CreateDeclarativeSchemaChangeJobs(
			ctx, r.execCfg.JobRegistry, txn, all,
		); err != nil {
			return err
		}
	}

	// Write the new TableDescriptors and flip state over to public so they can be
	// accessed.
	for i := range details.TableDescs {
		mutTable := all.LookupDescriptorEntry(details.TableDescs[i].GetID()).(*tabledesc.Mutable)
		// Note that we don't need to worry about the re-validated indexes for descriptors
		// with a declarative schema change job.
		if mutTable.GetDeclarativeSchemaChangerState() != nil {
			newTables = append(newTables, mutTable.TableDesc())
			continue
		}

		badIndexes := devalidateIndexes[mutTable.ID]
		for _, badIdx := range badIndexes {
			found, err := mutTable.FindIndexWithID(badIdx)
			if err != nil {
				return err
			}
			newIdx := found.IndexDescDeepCopy()
			mutTable.RemovePublicNonPrimaryIndex(found.Ordinal())
			if err := mutTable.AddIndexMutation(ctx, &newIdx, descpb.DescriptorMutation_ADD, r.settings); err != nil {
				return err
			}
		}
		version := r.settings.Version.ActiveVersion(ctx)
		if err := mutTable.AllocateIDs(ctx, version); err != nil {
			return err
		}
		// Assign a TTL schedule before publishing.
		if details.DescriptorCoverage != tree.AllDescriptors && mutTable.HasRowLevelTTL() {
			j, err := sql.CreateRowLevelTTLScheduledJob(
				ctx,
				execCfg,
				txn,
				user,
				mutTable.GetID(),
				mutTable.GetRowLevelTTL(),
			)
			if err != nil {
				return err
			}
			mutTable.RowLevelTTL.ScheduleID = j.ScheduleID()
		}
		newTables = append(newTables, mutTable.TableDesc())
		// For cluster restores, all the jobs are restored directly from the jobs
		// table, so there is no need to re-create ongoing schema change jobs,
		// otherwise we'll create duplicate jobs.
		if details.DescriptorCoverage != tree.AllDescriptors || len(badIndexes) > 0 {
			// Convert any mutations that were in progress on the table descriptor
			// when the backup was taken, and convert them to schema change jobs.
			if err := createSchemaChangeJobsFromMutations(ctx,
				r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), mutTable,
			); err != nil {
				return err
			}
		}
	}
	// For all of the newly created types, make type schema change jobs for any
	// type descriptors that were backed up in the middle of a type schema change.
	for i := range details.TypeDescs {
		typ := all.LookupDescriptorEntry(details.TypeDescs[i].GetID()).(catalog.TypeDescriptor)
		newTypes = append(newTypes, typ.TypeDesc())
		if typ.GetDeclarativeSchemaChangerState() == nil &&
			typ.HasPendingSchemaChanges() && details.DescriptorCoverage != tree.AllDescriptors {
			if err := createTypeChangeJobFromDesc(
				ctx, r.execCfg.JobRegistry, r.execCfg.Codec, txn, r.job.Payload().UsernameProto.Decode(), typ,
			); err != nil {
				return err
			}
		}
	}
	for i := range details.SchemaDescs {
		sc := all.LookupDescriptorEntry(details.SchemaDescs[i].GetID()).(catalog.SchemaDescriptor)
		newSchemas = append(newSchemas, sc.SchemaDesc())
	}
	for i := range details.DatabaseDescs {
		db := all.LookupDescriptorEntry(details.DatabaseDescs[i].GetID()).(catalog.DatabaseDescriptor)
		newDBs = append(newDBs, db.DatabaseDesc())
	}
	b := txn.NewBatch()
	if err := all.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		d := desc.(catalog.MutableDescriptor)
		d.SetPublic()
		return descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, d, b,
		)
	}); err != nil {
		return err
	}
	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "publishing tables")
	}

	for _, tenant := range details.Tenants {
		if err := sql.ActivateTenant(ctx, r.execCfg, txn, tenant.ID); err != nil {
			return err
		}
	}

	// Update and persist the state of the job.
	details.DescriptorsPublished = true
	details.TableDescs = newTables
	details.TypeDescs = newTypes
	details.SchemaDescs = newSchemas
	details.DatabaseDescs = newDBs
	if err := r.job.SetDetails(ctx, txn, details); err != nil {
		return errors.Wrap(err,
			"updating job details after publishing tables")
	}
	return nil
}

// prefetchDescriptors calculates the set of descriptors needed by looking
// at the relevant fields of the job details. It then fetches all of those
// descriptors in a batch using the descsCol. It packages up that set of
// descriptors into an nstree.Catalog for easy use.
//
// This function also takes care of asserting that the retrieved version
// matches the expectation.
func prefetchDescriptors(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, details jobspb.RestoreDetails,
) (_ nstree.Catalog, _ error) {
	var all nstree.MutableCatalog
	var allDescIDs catalog.DescriptorIDSet
	expVersion := map[descpb.ID]descpb.DescriptorVersion{}
	for i := range details.TableDescs {
		expVersion[details.TableDescs[i].GetID()] = details.TableDescs[i].GetVersion()
		allDescIDs.Add(details.TableDescs[i].GetID())
	}
	for i := range details.TypeDescs {
		expVersion[details.TypeDescs[i].GetID()] = details.TypeDescs[i].GetVersion()
		allDescIDs.Add(details.TypeDescs[i].GetID())
	}
	for i := range details.SchemaDescs {
		expVersion[details.SchemaDescs[i].GetID()] = details.SchemaDescs[i].GetVersion()
		allDescIDs.Add(details.SchemaDescs[i].GetID())
	}
	for i := range details.DatabaseDescs {
		expVersion[details.DatabaseDescs[i].GetID()] = details.DatabaseDescs[i].GetVersion()
		allDescIDs.Add(details.DatabaseDescs[i].GetID())
	}
	// Note that no maximum size is put on the batch here because,
	// in general, we assume that we can fit all of the descriptors
	// in RAM (we have them in RAM as part of the details object,
	// and we're going to write them to KV very soon as part of a
	// single batch).
	ids := allDescIDs.Ordered()
	got, err := descsCol.GetMutableDescriptorsByID(ctx, txn, ids...)
	if err != nil {
		return nstree.Catalog{}, errors.Wrap(err, "prefetch descriptors")
	}
	for i, id := range ids {
		if got[i].GetVersion() != expVersion[id] {
			return nstree.Catalog{}, errors.Errorf(
				"version mismatch for descriptor %d, expected version %d, got %v",
				got[i].GetID(), got[i].GetVersion(), expVersion[id],
			)
		}
		all.UpsertDescriptorEntry(got[i])
	}
	return all.Catalog, nil
}

func emitRestoreJobEvent(
	ctx context.Context, p sql.JobExecContext, status jobs.Status, job *jobs.Job,
) {
	// Emit to the event log now that we have completed the prepare step.
	var restoreEvent eventpb.Restore
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.LogEventForJobs(ctx, p.ExecCfg(), txn, &restoreEvent, int64(job.ID()),
			job.Payload(), p.User(), status)
	}); err != nil {
		log.Warningf(ctx, "failed to log event: %v", err)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes KV data that
// has been committed from a restore that has failed or been canceled. It does
// this by adding the table descriptors in DROP state, which causes the schema
// change stuff to delete the keys in the background.
func (r *restoreResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	// Emit to the event log that the job has started reverting.
	emitRestoreJobEvent(ctx, p, jobs.StatusReverting, r.job)

	telemetry.Count("restore.total.failed")
	telemetry.CountBucketed("restore.duration-sec.failed",
		int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds()))

	details := r.job.Details().(jobspb.RestoreDetails)

	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	if err := sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		for _, tenant := range details.Tenants {
			tenant.State = descpb.TenantInfo_DROP
			// This is already a job so no need to spin up a gc job for the tenant;
			// instead just GC the data eagerly.
			if err := sql.GCTenantSync(ctx, execCfg, &tenant.TenantInfo); err != nil {
				return err
			}
		}

		if err := r.dropDescriptors(ctx, execCfg.JobRegistry, execCfg.Codec, txn, descsCol); err != nil {
			return err
		}

		if details.DescriptorCoverage == tree.AllDescriptors {
			// We've dropped defaultdb and postgres in the planning phase, we must
			// recreate them now if the full cluster restore failed.
			ie := p.ExecCfg().InternalExecutor
			_, err := ie.Exec(ctx, "recreate-defaultdb", txn, "CREATE DATABASE IF NOT EXISTS defaultdb")
			if err != nil {
				return err
			}

			_, err = ie.Exec(ctx, "recreate-postgres", txn, "CREATE DATABASE IF NOT EXISTS postgres")
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if details.DescriptorCoverage == tree.AllDescriptors {
		// The temporary system table descriptors should already have been dropped
		// in `dropDescriptors` but we still need to drop the temporary system db.
		if err := execCfg.DB.Txn(ctx, r.cleanupTempSystemTables); err != nil {
			return err
		}
	}

	// Emit to the event log that the job has completed reverting.
	emitRestoreJobEvent(ctx, p, jobs.StatusFailed, r.job)
	return nil
}

// dropDescriptors implements the OnFailOrCancel logic.
// TODO (lucy): If the descriptors have already been published, we need to queue
// drop jobs for all the descriptors.
func (r *restoreResumer) dropDescriptors(
	ctx context.Context,
	jr *jobs.Registry,
	codec keys.SQLCodec,
	txn *kv.Txn,
	descsCol *descs.Collection,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	// No need to mark the tables as dropped if they were not even created in the
	// first place.
	if !details.PrepareCompleted {
		return nil
	}

	b := txn.NewBatch()

	// Collect the tables into mutable versions.
	mutableTables := make([]*tabledesc.Mutable, len(details.TableDescs))
	for i := range details.TableDescs {
		var err error
		mutableTables[i], err = descsCol.GetMutableTableVersionByID(ctx, details.TableDescs[i].ID, txn)
		if err != nil {
			return err
		}
		// Ensure that the version matches what we expect. In the case that it
		// doesn't, it's not really clear what to do. Just log and carry on. If the
		// descriptors have already been published, then there's nothing to fuss
		// about so we only do this check if they have not been published.
		if !details.DescriptorsPublished {
			if got, exp := mutableTables[i].Version, details.TableDescs[i].Version; got != exp {
				log.Errorf(ctx, "version changed for restored descriptor %d before "+
					"drop: got %d, expected %d", mutableTables[i].GetVersion(), got, exp)
			}
		}

	}

	// Remove any back references installed from existing types to tables being restored.
	if err := r.removeExistingTypeBackReferences(
		ctx, txn, descsCol, b, mutableTables, &details,
	); err != nil {
		return err
	}

	// Drop the table descriptors that were created at the start of the restore.
	tablesToGC := make([]descpb.ID, 0, len(details.TableDescs))
	// Set the drop time as 1 (ns in Unix time), so that the table gets GC'd
	// immediately.
	dropTime := int64(1)
	for i := range mutableTables {
		tableToDrop := mutableTables[i]
		tablesToGC = append(tablesToGC, tableToDrop.ID)
		tableToDrop.SetDropped()

		// Drop any schedules we may have implicitly created.
		if tableToDrop.HasRowLevelTTL() {
			scheduleID := tableToDrop.RowLevelTTL.ScheduleID
			if scheduleID != 0 {
				if err := sql.DeleteSchedule(
					ctx, r.execCfg, txn, scheduleID,
				); err != nil {
					return err
				}
			}
		}

		// If the DropTime is set, a table uses RangeClear for fast data removal. This
		// operation starts at DropTime + the GC TTL. If we used now() here, it would
		// not clean up data until the TTL from the time of the error. Instead, use 1
		// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
		// possible. This is safe since the table data was never visible to users,
		// and so we don't need to preserve MVCC semantics.
		tableToDrop.DropTime = dropTime
		b.Del(catalogkeys.EncodeNameKey(codec, tableToDrop))
		descsCol.AddDeletedDescriptor(tableToDrop.GetID())
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, tableToDrop, b); err != nil {
			return errors.Wrap(err, "writing dropping table to batch")
		}
	}

	// Drop the type descriptors that this restore created.
	for i := range details.TypeDescs {
		// TypeDescriptors don't have a GC job process, so we can just write them
		// as dropped here.
		typDesc := details.TypeDescs[i]
		mutType, err := descsCol.GetMutableTypeByID(ctx, txn, typDesc.ID, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidLeased:    true,
				IncludeOffline: true,
			},
		})
		if err != nil {
			return err
		}

		b.Del(catalogkeys.EncodeNameKey(codec, typDesc))
		mutType.SetDropped()
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, mutType, b); err != nil {
			return errors.Wrap(err, "writing dropping type to batch")
		}
		// Remove the system.descriptor entry.
		b.Del(catalogkeys.MakeDescMetadataKey(codec, typDesc.ID))
		descsCol.AddDeletedDescriptor(mutType.GetID())
	}

	// Queue a GC job.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", r.job.Payload().Description),
		Username:      r.job.Payload().UsernameProto.Decode(),
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := jr.CreateJobWithTxn(ctx, gcJobRecord, jr.MakeJobID(), txn); err != nil {
		return err
	}

	// Drop the database and schema descriptors that were created at the start of
	// the restore if they are now empty (i.e. no user created a table, etc. in
	// the database or schema during the restore).
	ignoredChildDescIDs := make(map[descpb.ID]struct{})
	for _, table := range details.TableDescs {
		ignoredChildDescIDs[table.ID] = struct{}{}
	}
	for _, typ := range details.TypeDescs {
		ignoredChildDescIDs[typ.ID] = struct{}{}
	}
	for _, schema := range details.SchemaDescs {
		ignoredChildDescIDs[schema.ID] = struct{}{}
	}
	all, err := descsCol.GetAllDescriptors(ctx, txn)
	if err != nil {
		return err
	}
	allDescs := all.OrderedDescriptors()

	// Delete any schema descriptors that this restore created. Also collect the
	// descriptors so we can update their parent databases later.
	dbsWithDeletedSchemas := make(map[descpb.ID][]catalog.Descriptor)
	for _, schemaDesc := range details.SchemaDescs {
		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isSchemaEmpty, err := isSchemaEmpty(ctx, txn, schemaDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if schema %s is empty during restore cleanup", schemaDesc.GetName())
		}

		if !isSchemaEmpty {
			log.Warningf(ctx, "preserving schema %s on restore failure because it contains new child objects", schemaDesc.GetName())
			continue
		}

		mutSchema, err := descsCol.GetMutableDescriptorByID(ctx, txn, schemaDesc.GetID())
		if err != nil {
			return err
		}

		// Mark schema as dropped and add uncommitted version to pass pre-txn
		// descriptor validation.
		mutSchema.SetDropped()
		mutSchema.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(mutSchema); err != nil {
			return err
		}

		b.Del(catalogkeys.EncodeNameKey(codec, mutSchema))
		b.Del(catalogkeys.MakeDescMetadataKey(codec, mutSchema.GetID()))
		descsCol.AddDeletedDescriptor(mutSchema.GetID())
		dbsWithDeletedSchemas[mutSchema.GetParentID()] = append(dbsWithDeletedSchemas[mutSchema.GetParentID()], mutSchema)
	}

	// For each database that had a child schema deleted (regardless of whether
	// the db was created in the restore job), if it wasn't deleted just now,
	// delete the now-deleted child schema from its schema map.
	//
	// This cleanup must be done prior to dropping the database descriptors in the
	// loop below so that we do not accidentally `b.Put` the descriptor with the
	// modified schema slice after we have issued a `b.Del` to drop it.
	for dbID, schemas := range dbsWithDeletedSchemas {
		log.Infof(ctx, "deleting %d schema entries from database %d", len(schemas), dbID)
		desc, err := descsCol.GetMutableDescriptorByID(ctx, txn, dbID)
		if err != nil {
			return err
		}
		db := desc.(*dbdesc.Mutable)
		for _, sc := range schemas {
			if schemaInfo, ok := db.Schemas[sc.GetName()]; !ok {
				log.Warningf(ctx, "unexpected missing schema entry for %s from db %d; skipping deletion",
					sc.GetName(), dbID)
			} else if schemaInfo.ID != sc.GetID() {
				log.Warningf(ctx, "unexpected schema entry %d for %s from db %d, expecting %d; skipping deletion",
					schemaInfo.ID, sc.GetName(), dbID, sc.GetID())
			} else {
				delete(db.Schemas, sc.GetName())
			}
		}

		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, db, b,
		); err != nil {
			return err
		}
	}

	// Delete the database descriptors.
	//
	// This should be the last step in mutating the database descriptors to ensure
	// that no batch requests are queued after the `b.Del` to delete the dropped
	// database descriptor.
	deletedDBs := make(map[descpb.ID]struct{})
	for _, dbDesc := range details.DatabaseDescs {

		// We need to ignore descriptors we just added since we haven't committed the txn that deletes these.
		isDBEmpty, err := isDatabaseEmpty(ctx, txn, dbDesc.GetID(), allDescs, ignoredChildDescIDs)
		if err != nil {
			return errors.Wrapf(err, "checking if database %s is empty during restore cleanup", dbDesc.GetName())
		}
		if !isDBEmpty {
			log.Warningf(ctx, "preserving database %s on restore failure because it contains new child objects or schemas", dbDesc.GetName())
			continue
		}

		db, err := descsCol.GetMutableDescriptorByID(ctx, txn, dbDesc.GetID())
		if err != nil {
			return err
		}

		// Mark db as dropped and add uncommitted version to pass pre-txn
		// descriptor validation.
		db.SetDropped()
		db.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(db); err != nil {
			return err
		}

		descKey := catalogkeys.MakeDescMetadataKey(codec, db.GetID())
		b.Del(descKey)

		// We have explicitly to delete the system.namespace entry for the public schema
		// if the database does not have a public schema backed by a descriptor.
		if !db.(catalog.DatabaseDescriptor).HasPublicSchemaWithDescriptor() {
			b.Del(catalogkeys.MakeSchemaNameKey(codec, db.GetID(), tree.PublicSchema))
		}

		nameKey := catalogkeys.MakeDatabaseNameKey(codec, db.GetName())
		b.Del(nameKey)
		descsCol.AddDeletedDescriptor(db.GetID())
		deletedDBs[db.GetID()] = struct{}{}
	}

	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "dropping tables created at the start of restore caused by fail/cancel")
	}

	return nil
}

// removeExistingTypeBackReferences removes back references from types that
// exist in the cluster to tables restored. It is used when rolling back from
// a failed restore.
func (r *restoreResumer) removeExistingTypeBackReferences(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	b *kv.Batch,
	restoredTables []*tabledesc.Mutable,
	details *jobspb.RestoreDetails,
) error {
	// We first collect the restored types to be addressable by ID.
	restoredTypes := make(map[descpb.ID]catalog.TypeDescriptor)
	existingTypes := make(map[descpb.ID]*typedesc.Mutable)
	for i := range details.TypeDescs {
		typ := details.TypeDescs[i]
		restoredTypes[typ.ID] = typedesc.NewBuilder(typ).BuildImmutableType()
	}
	for _, tbl := range restoredTables {
		lookup := func(id descpb.ID) (catalog.TypeDescriptor, error) {
			// First see if the type was restored.
			restored, ok := restoredTypes[id]
			if ok {
				return restored, nil
			}
			// Finally, look it up using the transaction.
			typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
			if err != nil {
				return nil, err
			}
			existingTypes[typ.GetID()] = typ
			return typ, nil
		}

		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx, txn, tbl.GetParentID(), tree.DatabaseLookupFlags{
				Required:       true,
				AvoidLeased:    true,
				IncludeOffline: true,
			})
		if err != nil {
			return err
		}

		// Get all types that this descriptor references.
		referencedTypes, _, err := tbl.GetAllReferencedTypeIDs(dbDesc, lookup)
		if err != nil {
			return err
		}

		// For each type that is existing, remove the backreference from tbl.
		for _, id := range referencedTypes {
			_, restored := restoredTypes[id]
			if !restored {
				desc, err := lookup(id)
				if err != nil {
					return err
				}
				existing := desc.(*typedesc.Mutable)
				existing.MaybeIncrementVersion()
				existing.RemoveReferencingDescriptorID(tbl.ID)
			}
		}
	}

	// Now write any changed existing types.
	for _, typ := range existingTypes {
		if typ.IsUncommittedVersion() {
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, typ, b,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

type systemTableNameWithConfig struct {
	systemTableName  string
	stagingTableName string
	config           systemBackupConfiguration
}

// Restore system.users from the backup into the restoring cluster. Only recreate users
// which are in a backup of system.users but do not currently exist (ignoring those who do)
// and re-grant roles for users if the backup has system.role_members.
func (r *restoreResumer) restoreSystemUsers(
	ctx context.Context, db *kv.DB, systemTables []catalog.TableDescriptor,
) error {
	executor := r.execCfg.InternalExecutor
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		selectNonExistentUsers := "SELECT * FROM crdb_temp_system.users temp " +
			"WHERE NOT EXISTS (SELECT * FROM system.users u WHERE temp.username = u.username)"
		users, err := executor.QueryBuffered(ctx, "get-users",
			txn, selectNonExistentUsers)
		if err != nil {
			return err
		}

		insertUser := `INSERT INTO system.users ("username", "hashedPassword", "isRole") VALUES ($1, $2, $3)`
		newUsernames := make(map[string]bool)
		for _, user := range users {
			newUsernames[user[0].String()] = true
			if _, err = executor.Exec(ctx, "insert-non-existent-users", txn, insertUser,
				user[0], user[1], user[2]); err != nil {
				return err
			}
		}

		// We skip granting roles if the backup does not contain system.role_members.
		if len(systemTables) == 1 {
			return nil
		}

		selectNonExistentRoleMembers := "SELECT * FROM crdb_temp_system.role_members temp_rm WHERE " +
			"NOT EXISTS (SELECT * FROM system.role_members rm WHERE temp_rm.role = rm.role AND temp_rm.member = rm.member)"
		roleMembers, err := executor.QueryBuffered(ctx, "get-role-members",
			txn, selectNonExistentRoleMembers)
		if err != nil {
			return err
		}

		insertRoleMember := `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, $3)`
		for _, roleMember := range roleMembers {
			// Only grant roles to users that don't currently exist, i.e., new users we just added
			if _, ok := newUsernames[roleMember[1].String()]; ok {
				if _, err = executor.Exec(ctx, "insert-non-existent-role-members", txn, insertRoleMember,
					roleMember[0], roleMember[1], roleMember[2]); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// restoreSystemTables atomically replaces the contents of the system tables
// with the data from the restored system tables.
func (r *restoreResumer) restoreSystemTables(
	ctx context.Context, db *kv.DB, tables []catalog.TableDescriptor,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)
	if details.SystemTablesMigrated == nil {
		details.SystemTablesMigrated = make(map[string]bool)
	}
	tempSystemDBID := tempSystemDatabaseID(details)

	// Iterate through all the tables that we're restoring, and if it was restored
	// to the temporary system DB then populate the metadata required to restore
	// to the real system table.
	systemTablesToRestore := make([]systemTableNameWithConfig, 0)
	for _, table := range tables {
		if table.GetParentID() != tempSystemDBID {
			continue
		}
		systemTableName := table.GetName()
		stagingTableName := restoreTempSystemDB + "." + systemTableName

		config, ok := systemTableBackupConfiguration[systemTableName]
		if !ok {
			log.Warningf(ctx, "no configuration specified for table %s... skipping restoration",
				systemTableName)
		}
		systemTablesToRestore = append(systemTablesToRestore, systemTableNameWithConfig{
			systemTableName:  systemTableName,
			stagingTableName: stagingTableName,
			config:           config,
		})
	}

	// Sort the system tables to be restored based on the order specified in the
	// configuration.
	sort.SliceStable(systemTablesToRestore, func(i, j int) bool {
		return systemTablesToRestore[i].config.restoreInOrder < systemTablesToRestore[j].config.restoreInOrder
	})

	// Copy the data from the temporary system DB to the real system table.
	for _, systemTable := range systemTablesToRestore {
		if systemTable.config.migrationFunc != nil {
			if details.SystemTablesMigrated[systemTable.systemTableName] {
				continue
			}

			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if err := systemTable.config.migrationFunc(ctx, r.execCfg, txn,
					systemTable.stagingTableName); err != nil {
					return err
				}

				// Keep track of which system tables we've migrated so that future job
				// restarts don't try to import data over our migrated data. This would
				// fail since the restored data would shadow the migrated keys.
				details.SystemTablesMigrated[systemTable.systemTableName] = true
				return r.job.SetDetails(ctx, txn, details)
			}); err != nil {
				return err
			}
		}

		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			txn.SetDebugName("system-restore-txn")

			restoreFunc := defaultSystemTableRestoreFunc
			if systemTable.config.customRestoreFunc != nil {
				restoreFunc = systemTable.config.customRestoreFunc
				log.Eventf(ctx, "using custom restore function for table %s", systemTable.systemTableName)
			}

			log.Eventf(ctx, "restoring system table %s", systemTable.systemTableName)
			err := restoreFunc(ctx, r.execCfg, txn, systemTable.systemTableName, systemTable.stagingTableName)
			if err != nil {
				return errors.Wrapf(err, "restoring system table %s", systemTable.systemTableName)
			}
			return nil
		}); err != nil {
			return err
		}

		if fn := r.testingKnobs.duringSystemTableRestoration; fn != nil {
			if err := fn(systemTable.systemTableName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *restoreResumer) cleanupTempSystemTables(ctx context.Context, txn *kv.Txn) error {
	executor := r.execCfg.InternalExecutor
	// Check if the temp system database has already been dropped. This can happen
	// if the restore job fails after the system database has cleaned up.
	checkIfDatabaseExists := "SELECT database_name FROM [SHOW DATABASES] WHERE database_name=$1"
	if row, err := executor.QueryRow(ctx, "checking-for-temp-system-db" /* opName */, txn, checkIfDatabaseExists, restoreTempSystemDB); err != nil {
		return errors.Wrap(err, "checking for temporary system db")
	} else if row == nil {
		// Temporary system DB might already have been dropped by the restore job.
		return nil
	}

	// After restoring the system tables, drop the temporary database holding the
	// system tables.
	gcTTLQuery := fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING gc.ttlseconds=1", restoreTempSystemDB)
	if _, err := executor.Exec(ctx, "altering-gc-ttl-temp-system" /* opName */, txn, gcTTLQuery); err != nil {
		log.Errorf(ctx, "failed to update the GC TTL of %q: %+v", restoreTempSystemDB, err)
	}
	dropTableQuery := fmt.Sprintf("DROP DATABASE %s CASCADE", restoreTempSystemDB)
	if _, err := executor.Exec(ctx, "drop-temp-system-db" /* opName */, txn, dropTableQuery); err != nil {
		return errors.Wrap(err, "dropping temporary system db")
	}
	return nil
}

var _ jobs.Resumer = &restoreResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeRestore,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &restoreResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}
