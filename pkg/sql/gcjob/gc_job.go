// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
//    defer SetSmallMaxGCIntervalForTest()
func SetSmallMaxGCIntervalForTest() func() {
	oldInterval := MaxSQLGCInterval
	MaxSQLGCInterval = 500 * time.Millisecond
	return func() {
		MaxSQLGCInterval = oldInterval
	}
}

type schemaChangeGCResumer struct {
	jobID jobspb.JobID
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

// unsplitRange unsets the sticky bit of a range if it's manually split
func unsplitRange(
	ctx context.Context, execCfg *sql.ExecutorConfig, desc roachpb.RangeDescriptor,
) error {
	if !execCfg.Codec.ForSystemTenant() {
		return nil
	}

	if !desc.GetStickyBit().IsEmpty() {
		// Swallow "key is not the start of a range" errors because it would mean
		// that the sticky bit was removed and merged concurrently. DROP TABLE
		// should not fail because of this.
		if err := execCfg.DB.AdminUnsplit(ctx, desc.StartKey); err != nil &&
			!strings.Contains(err.Error(), "is not the start of a range") {
			return err
		}
	}

	return nil
}

// unsplitRangesForTable unsplit any manually split ranges within the table span.
func unsplitRangesForTable(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableDesc catalog.TableDescriptor,
) error {
	// Gate this on being the system tenant because secondary tenants aren't
	// allowed to scan the meta ranges directly.
	if !execCfg.Codec.ForSystemTenant() {
		return nil
	}

	span := tableDesc.TableSpan(execCfg.Codec)
	ranges, err := kvclient.ScanMetaKVs(ctx, execCfg.DB.NewTxn(ctx, "unsplit-ranges-for-table"), span)
	if err != nil {
		return err
	}
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			return err
		}

		if err := unsplitRange(ctx, execCfg, desc); err != nil {
			return nil
		}
	}

	return nil
}

func unsplitRangesForTables(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	droppedTables []jobspb.SchemaChangeGCDetails_DroppedID,
) error {
	if !execCfg.Codec.ForSystemTenant() {
		return nil
	}

	for _, droppedTable := range droppedTables {
		var table catalog.TableDescriptor
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			table, err = catalogkv.MustGetTableDescByID(ctx, txn, execCfg.Codec, droppedTable.ID)
			return err
		}); err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				log.Warningf(ctx, "table descriptor %d not found while attempting to GC, skipping", droppedTable.ID)
				continue
			}
			return errors.Wrapf(err, "failed to fetch table %d", droppedTable.ID)
		}

		if err := unsplitRangesForTable(ctx, execCfg, table); err != nil {
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
	if !execCfg.Codec.ForSystemTenant() {
		return nil
	}

	tableKeyPrefix := execCfg.Codec.TablePrefix(uint32(parentTableID))
	ranges, err := kvclient.ScanMetaKVs(
		ctx,
		execCfg.DB.NewTxn(ctx, "gc-unsplit-ranges-for-indexes"),
		roachpb.Span{
			Key:    tableKeyPrefix,
			EndKey: tableKeyPrefix.PrefixEnd(),
		},
	)
	if err != nil {
		return err
	}

	droppedIndexIDs := make(map[descpb.IndexID]struct{})
	for _, idx := range indexes {
		droppedIndexIDs[idx.IndexID] = struct{}{}
	}

	var desc roachpb.RangeDescriptor
	for i := range ranges {
		if err := ranges[i].ValueProto(&desc); err != nil {
			return err
		}

		_, foundTabldID, foundIndexID, err := execCfg.Codec.DecodeIndexPrefix(roachpb.Key(desc.StartKey))
		if err != nil {
			// If we get an error here, it means that either our key didn't contain
			// an index ID (because it was the first range in a table) or the key
			// didn't contain a table ID (because it's still the first range in the
			// system that hasn't split off yet).
			// In this case, we can't translate this range into the new keyspace,
			// so we just have to continue along.
			continue
		}

		if foundTabldID != uint32(parentTableID) {
			// We found a split point that started somewhere else in the database,
			// so we can't translate it to the new keyspace. Don't bother with this
			// range.
			continue
		}

		if _, ok := droppedIndexIDs[descpb.IndexID(foundIndexID)]; ok {
			if err := unsplitRange(ctx, execCfg, desc); err != nil {
				return err
			}
		}
	}

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
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	if fn := execCfg.GCJobTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(r.jobID); err != nil {
			return err
		}
	}
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}

	if !progress.TablesUnsplitDone {
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

		progress.TablesUnsplitDone = true
		persistProgress(ctx, execCfg, r.jobID, progress, runningStatusGC(progress))
	}

	tableDropTimes, indexDropTimes := getDropTimes(details)

	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(0)
	gossipUpdateC, cleanup := execCfg.GCJobNotifier.AddNotifyee(ctx)
	defer cleanup()
	for {
		select {
		case <-gossipUpdateC:
			if log.V(2) {
				log.Info(ctx, "received a new system config")
			}
		case <-timer.C:
			timer.Read = true
			if log.V(2) {
				log.Info(ctx, "SchemaChangeGC timer triggered")
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		// Refresh the status of all elements in case any GC TTLs have changed.
		var expired bool
		earliestDeadline := timeutil.Unix(0, math.MaxInt64)
		if details.Tenant == nil {
			remainingTables := getAllTablesWaitingForGC(details, progress)
			expired, earliestDeadline = refreshTables(
				ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress,
			)
		} else {
			expired, earliestDeadline = refreshTenant(ctx, execCfg, details.Tenant.DropTime, details, progress)
		}
		timerDuration := time.Until(earliestDeadline)

		if expired {
			// Some elements have been marked as DELETING so save the progress.
			persistProgress(ctx, execCfg, r.jobID, progress, runningStatusGC(progress))
			if fn := execCfg.GCJobTestingKnobs.RunBeforePerformGC; fn != nil {
				if err := fn(r.jobID); err != nil {
					return err
				}
			}
			if err := performGC(ctx, execCfg, details, progress); err != nil {
				return err
			}
			persistProgress(ctx, execCfg, r.jobID, progress, sql.RunningStatusWaitingGC)

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
		timer.Reset(timerDuration)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) OnFailOrCancel(context.Context, interface{}) error {
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
			jobID: job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn)
}
