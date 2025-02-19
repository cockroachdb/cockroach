// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	// MaxSQLGCInterval is the longest the polling interval between checking if
	// elements should be GC'd.
	MaxSQLGCInterval = 5 * time.Minute
)

// SetSmallMaxGCIntervalForTest sets the MaxSQLGCInterval and then returns a closure
// that resets it.
// This is to be used in tests like:
//
//	defer SetSmallMaxGCIntervalForTest()
func SetSmallMaxGCIntervalForTest() func() {
	oldInterval := MaxSQLGCInterval
	MaxSQLGCInterval = 500 * time.Millisecond
	return func() {
		MaxSQLGCInterval = oldInterval
	}
}

var idleWaitDuration = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"sql.gc_job.idle_wait_duration",
	"after this duration of waiting for an update, the gc job will mark itself idle",
	time.Second,
)

type schemaChangeGCResumer struct {
	job *jobs.Job
}

// performGC GCs any schema elements that are in the DELETING state and returns
// a bool indicating if it GC'd any elements.
func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if details.Tenant != nil {
		return errors.Wrapf(
			gcTenant(ctx, execCfg, details.Tenant.ID, progress),
			"attempting to GC tenant %+v", details.Tenant,
		)
	}
	if details.Indexes != nil {
		return errors.Wrap(gcIndexes(ctx, execCfg, details.ParentID, progress), "attempting to GC indexes")
	} else if details.Tables != nil {
		if err := gcTables(ctx, execCfg, progress); err != nil {
			return errors.Wrap(err, "attempting to GC tables")
		}

		// Drop database zone config when all the tables have been GCed.
		if details.ParentID != descpb.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(
				ctx,
				execCfg.DB,
				execCfg.Codec,
				execCfg.Settings,
				details.ParentID,
			); err != nil {
				return errors.Wrap(err, "deleting database zone config")
			}
		}
	}
	return nil
}

// performGC GCs any schema elements that are in the DELETING state and returns
// a bool indicating if it GC'd any elements.
func deleteData(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	switch {
	case details.Indexes != nil:
		return errors.Wrap(
			deleteIndexData(ctx, execCfg, details.ParentID, progress),
			"attempting to delete index data",
		)
	case details.Tables != nil:
		return errors.Wrap(
			deleteTableData(ctx, execCfg, progress),
			"attempted to delete table data",
		)
	default:
		return nil
	}
}

func deleteTableData(
	ctx context.Context, cfg *sql.ExecutorConfig, progress *jobspb.SchemaChangeGCProgress,
) error {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "GC is being considered for tables: %+v", progress.Tables)
	}
	for _, droppedTable := range progress.Tables {
		var table catalog.TableDescriptor
		if err := sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			table, err = col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, droppedTable.ID)
			return err
		}); err != nil {
			if isMissingDescriptorError(err) {
				// This can happen if another GC job created for the same table got to
				// the table first. See #50344.
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				// Update the details payload to indicate that the table was dropped.
				markTableGCed(ctx, droppedTable.ID, progress, jobspb.SchemaChangeGCProgress_CLEARED)
				continue
			}
			return errors.Wrapf(err, "fetching table %d", droppedTable.ID)
		}

		// TODO(ajwerner): How does this happen?
		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}
		if err := DeleteAllTableData(ctx, cfg.DB, cfg.DistSender, cfg.Codec, table); err != nil {
			return err
		}

		// Update the details payload to indicate that the table was dropped.
		markTableGCed(
			ctx, table.GetID(), progress, jobspb.SchemaChangeGCProgress_WAITING_FOR_MVCC_GC,
		)
	}
	return nil
}

// unsplitRangesInSpan unsplits any manually split ranges within a span.
func unsplitRangesInSpan(
	ctx context.Context, execCfg *sql.ExecutorConfig, span roachpb.Span,
) error {
	if !execCfg.Codec.ForSystemTenant() {
		return unsplitRangesInSpanForSecondaryTenant(ctx, execCfg, span)
	}

	kvDB := execCfg.DB
	ranges, err := kvclient.ScanMetaKVs(ctx, kvDB.NewTxn(ctx, "unsplit-ranges-in-span"), span)
	if err != nil {
		return err
	}
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			return err
		}

		if !span.ContainsKey(desc.StartKey.AsRawKey()) {
			continue
		}

		if !desc.StickyBit.IsEmpty() {
			// Swallow "key is not the start of a range" errors because it would mean
			// that the sticky bit was removed and merged concurrently. DROP TABLE
			// should not fail because of this.
			if err := kvDB.AdminUnsplit(ctx, desc.StartKey); err != nil &&
				!strings.Contains(err.Error(), "is not the start of a range") {
				return err
			}
		}
	}

	return nil
}

// unsplitRangesInSpanForSecondaryTenant unsplits any manually split
// ranges within a span using an implementation that is appropriate
// for a secondary tenant.
//
// When operating in a secondary tenant, unsplitting is not guaranteed
// because:
//
//   - The tenant may no longer be allowed to unsplit.
//
//   - We use the range cache to look up range start keys and our
//     range cache may be out of date.
func unsplitRangesInSpanForSecondaryTenant(
	ctx context.Context, execCfg *sql.ExecutorConfig, span roachpb.Span,
) error {
	rangeStartKeysToUnsplit, err := rangeStartKeysForSpanSecondaryTenant(ctx, execCfg, span)
	if err != nil {
		return err
	}

	for _, key := range rangeStartKeysToUnsplit {
		if err := execCfg.DB.AdminUnsplit(ctx, key); err != nil {
			// Swallow "key is not the start of a range" errors because it would mean
			// that the sticky bit was removed and merged concurrently. DROP TABLE
			// should not fail because of this.
			if strings.Contains(err.Error(), "is not the start of a range") {
				continue
			}
			// If we are in a secondary tenant and get an auth
			// error, the likely case is that we don't have
			// permission to AdminUnsplit.
			//
			// TODO(ssd): We've opted to log a warning and move on,
			// but this means in some cases the user may be left
			// with empty, unmergable ranges.
			if !execCfg.Codec.ForSystemTenant() && grpcutil.IsAuthError(err) {
				log.Warningf(ctx, "failed to unsplit range at %s: %s", key, err)
				continue
			}
			return err
		}
	}
	return nil
}

func rangeStartKeysForSpanSecondaryTenant(
	ctx context.Context, execCfg *sql.ExecutorConfig, span roachpb.Span,
) ([]roachpb.Key, error) {
	ret := []roachpb.Key{}
	rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(ctx, execCfg.Codec.TenantSpan())
	if err != nil {
		return nil, err
	}

	for rangeDescIterator.Valid() {
		rangeDesc := rangeDescIterator.CurRangeDescriptor()
		rangeDescIterator.Next()

		if !span.ContainsKey(rangeDesc.StartKey.AsRawKey()) {
			continue
		}
		if rangeDesc.StickyBit.IsEmpty() {
			continue
		}
		ret = append(ret, rangeDesc.StartKey.AsRawKey())
	}
	return ret, nil
}

func unsplitRangesForTables(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	droppedTables []jobspb.SchemaChangeGCDetails_DroppedID,
) error {
	for _, droppedTable := range droppedTables {
		startKey := execCfg.Codec.TablePrefix(uint32(droppedTable.ID))
		span := roachpb.Span{
			Key:    startKey,
			EndKey: startKey.PrefixEnd(),
		}
		if err := unsplitRangesInSpan(ctx, execCfg, span); err != nil {
			return err
		}
	}

	return nil
}

// unsplitRangesForIndexes unsplits ranges with dropped index in key prefix
func unsplitRangesForIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	indexes []jobspb.SchemaChangeGCDetails_DroppedIndex,
	parentTableID descpb.ID,
) error {
	for _, idx := range indexes {
		startKey := execCfg.Codec.IndexPrefix(uint32(parentTableID), uint32(idx.IndexID))
		idxSpan := roachpb.Span{
			Key:    startKey,
			EndKey: startKey.PrefixEnd(),
		}

		if err := unsplitRangesInSpan(ctx, execCfg, idxSpan); err != nil {
			return err
		}
	}

	return nil
}

func maybeUnsplitRanges(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	job *jobs.Job,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if progress.RangesUnsplitDone {
		return nil
	}

	if len(details.Indexes) > 0 {
		if err := unsplitRangesForIndexes(ctx, execCfg, details.Indexes, details.ParentID); err != nil {
			return err
		}
	}

	if len(details.Tables) > 0 {
		if err := unsplitRangesForTables(ctx, execCfg, details.Tables); err != nil {
			return err
		}
	}

	progress.RangesUnsplitDone = true
	persistProgress(ctx, execCfg, job, progress, statusGC(progress))

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) Resume(ctx context.Context, execCtx interface{}) (err error) {
	defer func() {
		if err != nil && !r.isPermanentGCError(err) {
			err = jobs.MarkAsRetryJobError(err)
		}
	}()
	p := execCtx.(sql.JobExecContext)

	// Clone the ExecConfig so that fields can be overwritten for testing knobs.
	execCfg := *p.ExecCfg()
	if n := execCfg.GCJobTestingKnobs.Notifier; n != nil {
		execCfg.GCJobNotifier = n
	}
	// Use the same SystemConfigProvider as the notifier.
	execCfg.SystemConfig = execCfg.GCJobNotifier.SystemConfigProvider()

	if err := execCfg.JobRegistry.CheckPausepoint("gcjob.before_resume"); err != nil {
		return err
	}

	if fn := execCfg.GCJobTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(r.job.ID()); err != nil {
			return err
		}
	}
	details, progress, err := initDetailsAndProgress(ctx, &execCfg, r.job)
	if err != nil {
		return err
	}
	if err := maybeUnsplitRanges(ctx, &execCfg, r.job, details, progress); err != nil {
		return err
	}

	if !shouldUseDelRange(ctx, details, execCfg.Settings, execCfg.GCJobTestingKnobs) {
		return r.legacyWaitAndClearTableData(ctx, execCfg, details, progress)
	}
	return r.deleteDataAndWaitForGC(ctx, execCfg, details, progress)
}

func (r schemaChangeGCResumer) deleteDataAndWaitForGC(
	ctx context.Context,
	execCfg sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	persistProgress(ctx, &execCfg, r.job, progress,
		sql.StatusDeletingData)
	if fn := execCfg.GCJobTestingKnobs.RunBeforePerformGC; fn != nil {
		if err := fn(r.job.ID()); err != nil {
			return err
		}
	}
	if err := deleteData(ctx, &execCfg, details, progress); err != nil {
		return err
	}
	persistProgress(ctx, &execCfg, r.job, progress, sql.StatusWaitingForMVCCGC)
	r.job.MarkIdle(true)
	return waitForGC(ctx, &execCfg, details, progress)
}

func waitForGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	switch {
	case details.Indexes != nil:
		return errors.Wrap(
			deleteIndexZoneConfigsAfterGC(ctx, execCfg, details.ParentID, progress),
			"attempting to delete index data",
		)
	case details.Tables != nil:
		return errors.Wrap(
			deleteTableDescriptorsAfterGC(ctx, execCfg, details, progress),
			"attempted to delete table data",
		)
	default:
		return nil
	}
}

// EmptySpanPollInterval is the interval at which the GC job will poll the
// spans to determine whether the data have been garbage collected.
var EmptySpanPollInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"sql.gc_job.wait_for_gc.interval",
	"interval at which the GC job should poll to see if the deleted data has been GC'd",
	5*time.Minute,
	settings.NonNegativeDuration,
)

func waitForEmptyPrefix(
	ctx context.Context,
	db *kv.DB,
	sv *settings.Values,
	skipWaiting bool,
	checkImmediately bool,
	prefix roachpb.Key,
) error {
	if skipWaiting {
		log.Infof(ctx, "not waiting for MVCC GC in %v due to testing knob", prefix)
		return nil
	}
	var timer timeutil.Timer
	defer timer.Stop()
	// TODO(ajwerner): Allow for settings watchers to be un-registered (#73830),
	// then observe changes to the setting.
	for {
		if checkImmediately {
			if empty, err := checkForEmptySpan(
				ctx, db, prefix, prefix.PrefixEnd(),
			); empty || err != nil {
				return err
			}
		}
		timer.Reset(EmptySpanPollInterval.Get(sv))
		select {
		case <-timer.C:
			timer.Read = true
			if empty, err := checkForEmptySpan(
				ctx, db, prefix, prefix.PrefixEnd(),
			); empty || err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func checkForEmptySpan(ctx context.Context, db *kv.DB, from, to roachpb.Key) (empty bool, _ error) {
	var ba kv.Batch
	ba.Header.MaxSpanRequestKeys = 1
	ba.AddRawRequest(&kvpb.IsSpanEmptyRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: from, EndKey: to,
		},
	})
	if err := db.Run(ctx, &ba); err != nil {
		return false, err
	}
	return ba.RawResponse().Responses[0].GetIsSpanEmpty().IsEmpty(), nil
}

func (r schemaChangeGCResumer) legacyWaitAndClearTableData(
	ctx context.Context,
	execCfg sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	tableDropTimes, indexDropTimes := getDropTimes(details)

	gossipUpdateC, cleanup := execCfg.GCJobNotifier.AddNotifyee(ctx)
	defer cleanup()

	// Now that we've registered to be notified, check to see if we raced
	// with the new version becoming active.
	if shouldUseDelRange(ctx, details, execCfg.Settings, execCfg.GCJobTestingKnobs) {
		return r.deleteDataAndWaitForGC(ctx, execCfg, details, progress)
	}

	var timerDuration time.Duration
	ts := timeutil.DefaultTimeSource{}

	for {
		idleWait := idleWaitDuration.Get(execCfg.SV())
		if err := waitForWork(
			ctx, r.job.MarkIdle, ts, timerDuration, idleWait, gossipUpdateC,
		); err != nil {
			return err
		}
		// We'll be notified if the new version becomes active, so check and
		// see if it's now time to change to the new protocol.
		if shouldUseDelRange(
			ctx, details, execCfg.Settings, execCfg.GCJobTestingKnobs,
		) {
			return r.deleteDataAndWaitForGC(ctx, execCfg, details, progress)
		}

		// Refresh the status of all elements in case any GC TTLs have changed.
		var expired bool
		var earliestDeadline time.Time
		if details.Tenant == nil {
			remainingTables := getAllTablesWaitingForGC(details, progress)
			expired, earliestDeadline = refreshTables(
				ctx, &execCfg, remainingTables, tableDropTimes, indexDropTimes, r.job, progress,
			)
		} else {
			var err error
			expired, earliestDeadline, err = refreshTenant(ctx, &execCfg, details.Tenant.DropTime, details, progress)
			if err != nil {
				return err
			}
		}
		timerDuration = time.Until(earliestDeadline)

		if expired {
			// Some elements have been marked as DELETING to save the progress.
			persistProgress(ctx, &execCfg, r.job, progress, statusGC(progress))
			if fn := execCfg.GCJobTestingKnobs.RunBeforePerformGC; fn != nil {
				if err := fn(r.job.ID()); err != nil {
					return err
				}
			}
			if err := performGC(ctx, &execCfg, details, progress); err != nil {
				return err
			}
			persistProgress(ctx, &execCfg, r.job, progress, sql.StatusWaitingGC)

			// Trigger immediate re-run in case of more expired elements.
			timerDuration = 0
		}

		if isDoneGC(progress) {
			return nil
		}

		// Schedule the next check for GC.
		if timerDuration > MaxSQLGCInterval {
			timerDuration = MaxSQLGCInterval
		}
	}
}

func shouldUseDelRange(
	ctx context.Context,
	details *jobspb.SchemaChangeGCDetails,
	s *cluster.Settings,
	knobs *sql.GCJobTestingKnobs,
) bool {
	// TODO(ajwerner): Adopt the DeleteRange protocol for tenant GC.
	return details.Tenant == nil
}

// waitForWork waits until there is work to do given the gossipUpDateC, the
// timer, or the context. It calls markIdle with true after waiting
// idleWaitDuration. It calls markIdle with false before returning.
func waitForWork(
	ctx context.Context,
	markIdle func(isIdle bool),
	source timeutil.TimeSource,
	workTimerDuration, idleWaitDuration time.Duration,
	gossipUpdateC <-chan struct{},
) error {
	var markedIdle bool
	defer func() {
		if markedIdle {
			markIdle(false)
		}
	}()

	markIdleTimer := source.NewTimer()
	markIdleTimer.Reset(idleWaitDuration)
	defer markIdleTimer.Stop()

	workTimer := source.NewTimer()
	workTimer.Reset(workTimerDuration)
	defer workTimer.Stop()

	wait := func() (done bool) {
		select {
		case <-markIdleTimer.Ch():
			markIdleTimer.MarkRead()
			markIdle(true)
			markedIdle = true
			return false

		case <-gossipUpdateC:
			if log.V(2) {
				log.Info(ctx, "received a new system config")
			}

		case <-workTimer.Ch():
			workTimer.MarkRead()
			if log.V(2) {
				log.Info(ctx, "SchemaChangeGC workTimer triggered")
			}

		case <-ctx.Done():
		}
		return true
	}
	if done := wait(); !done {
		wait()
	}
	return ctx.Err()
}

// isMissingDescriptorError checks whether the error has a code corresponding
// to a missing descriptor or if there is a lower-level catalog error with
// the same meaning.
//
// TODO(ajwerner,postamar): Nail down when we expect the lower-level error
// and tighten up the collection.
func isMissingDescriptorError(err error) bool {
	return errors.Is(err, catalog.ErrDescriptorNotFound) ||
		sqlerrors.IsMissingDescriptorError(err)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	return nil
}

// CollectProfile is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) CollectProfile(context.Context, interface{}) error {
	return nil
}

// isPermanentGCError returns true if the error is a permanent job failure,
// which indicates that the failed GC job cannot be retried.
func (r *schemaChangeGCResumer) isPermanentGCError(err error) bool {
	// Currently we classify errors based on Schema Change function to backport
	// to 20.2 and 21.1. This functionality should be changed once #44594 is
	// implemented.
	return sql.IsPermanentSchemaChangeError(err)
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeGCResumer{
			job: job,
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn, jobs.UsesTenantCostControl)
}
