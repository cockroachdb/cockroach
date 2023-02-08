// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func managePTSOnChangefeedStart(
	ctx context.Context,
	codec keys.SQLCodec,
	details jobspb.ChangefeedDetails,
	jobID jobspb.JobID,
	progress *jobspb.ChangefeedProgress,
	manager protectedts.Manager,
	txn isql.Txn,
) error {
	var ptr *ptpb.Record
	ptr = createProtectedTimestampRecord(
		ctx,
		codec,
		jobID,
		AllTargets(details),
		details.StatementTime,
		progress,
	)

	return manager.WithTxn(txn).Protect(ctx, ptr)
}

// managePTSOnCheckpoint advances the protected timestamp for
// the changefeed's targets to the current highwater mark as long as
// we have not violated the changefeedbase.ProtectTimestampInterval.
func managePTSOnCheckpoint(
	ctx context.Context,
	txn isql.Txn,
	progress *jobspb.ChangefeedProgress,
	details jobspb.ChangefeedDetails,
	manager protectedts.Manager,
	jobID jobspb.JobID,
	codec keys.SQLCodec,
	highWater hlc.Timestamp,
	lastUpdate time.Time,
	settings *cluster.Settings,
) (error, bool) {
	ptsUpdateInterval := changefeedbase.ProtectTimestampInterval.Get(&settings.SV)
	if timeutil.Since(lastUpdate) < ptsUpdateInterval {
		return nil, false
	}

	pts := manager.WithTxn(txn)

	recordID := progress.ProtectedTimestampRecord
	if recordID == uuid.Nil {
		ptr := createProtectedTimestampRecord(ctx, codec, jobID, AllTargets(details), highWater, progress)
		if err := pts.Protect(ctx, ptr); err != nil {
			return err, false
		}
	} else {
		log.VEventf(ctx, 2, "updating protected timestamp %v at %v", recordID, highWater)
		if err := pts.UpdateTimestamp(ctx, recordID, highWater); err != nil {
			return err, false
		}
	}

	return nil, true
}

// managePTSOnPause may clear the protected timestamp record.
func managePTSOnPause(
	ctx context.Context,
	progress *jobspb.Progress,
	details jobspb.ChangefeedDetails,
	provider protectedts.Manager,
	codec keys.SQLCodec,
	jobID jobspb.JobID,
	txn isql.Txn,
) error {
	cp := progress.GetChangefeed()
	if _, shouldProtect := details.Opts[changefeedbase.OptProtectDataFromGCOnPause]; !shouldProtect {
		// Release existing pts record to avoid a single changefeed left on pause
		// resulting in storage issues
		if cp.ProtectedTimestampRecord != uuid.Nil {
			pts := provider.WithTxn(txn)
			if err := pts.Release(ctx, cp.ProtectedTimestampRecord); err != nil {
				log.Warningf(ctx, "failed to release protected timestamp %v: %v", cp.ProtectedTimestampRecord, err)
			} else {
				cp.ProtectedTimestampRecord = uuid.Nil
			}
		}
		return nil
	}

	if cp.ProtectedTimestampRecord == uuid.Nil {
		resolved := progress.GetHighWater()
		if resolved == nil {
			return nil
		}
		pts := provider.WithTxn(txn)
		ptr := createProtectedTimestampRecord(ctx, codec, jobID, AllTargets(details), *resolved, cp)
		return pts.Protect(ctx, ptr)
	}

	return nil
}

// managePTSOnFailOrCancel tries to clear the PTS record on fail or cancel.
func managePTSOnFailOrCancel(
	ctx context.Context, db isql.DB, manager protectedts.Manager, progress jobspb.Progress,
) {
	ptsID := progress.GetChangefeed().ProtectedTimestampRecord

	if ptsID == uuid.Nil {
		return
	}
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return manager.WithTxn(txn).Release(ctx, ptsID)
	}); err != nil && !errors.Is(err, protectedts.ErrNotExists) {
		// NB: The record should get cleaned up by the reconciliation loop.
		// No good reason to cause more trouble by returning an error here.
		// Log and move on.
		log.Warningf(ctx, "failed to remove protected timestamp record %v: %v", ptsID, err)
	}
}

// createProtectedTimestampRecord will create a record to protect the spans for
// this changefeed at the resolved timestamp. The progress struct will be
// updated to refer to this new protected timestamp record.
func createProtectedTimestampRecord(
	ctx context.Context,
	codec keys.SQLCodec,
	jobID jobspb.JobID,
	targets changefeedbase.Targets,
	resolved hlc.Timestamp,
	progress *jobspb.ChangefeedProgress,
) *ptpb.Record {
	progress.ProtectedTimestampRecord = uuid.MakeV4()
	deprecatedSpansToProtect := makeSpansToProtect(codec, targets)
	targetToProtect := makeTargetToProtect(targets)

	log.VEventf(ctx, 2, "creating protected timestamp %v at %v", progress.ProtectedTimestampRecord, resolved)
	return jobsprotectedts.MakeRecord(
		progress.ProtectedTimestampRecord, int64(jobID), resolved, deprecatedSpansToProtect,
		jobsprotectedts.Jobs, targetToProtect)
}

func makeTargetToProtect(targets changefeedbase.Targets) *ptpb.Target {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	tablesToProtect := make(descpb.IDs, 0, targets.NumUniqueTables()+1)
	_ = targets.EachTableID(func(id descpb.ID) error {
		tablesToProtect = append(tablesToProtect, id)
		return nil
	})
	tablesToProtect = append(tablesToProtect, keys.DescriptorTableID)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

func makeSpansToProtect(codec keys.SQLCodec, targets changefeedbase.Targets) []roachpb.Span {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	spansToProtect := make([]roachpb.Span, 0, targets.NumUniqueTables()+1)
	addTablePrefix := func(id uint32) {
		tablePrefix := codec.TablePrefix(id)
		spansToProtect = append(spansToProtect, roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		})
	}
	_ = targets.EachTableID(func(id descpb.ID) error {
		addTablePrefix(uint32(id))
		return nil
	})
	addTablePrefix(keys.DescriptorTableID)
	return spansToProtect
}
