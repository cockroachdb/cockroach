// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// ScanSST scans the SSTable in the given RangeFeedSSTable within
// 'scanWithin' boundaries and execute given operations on each
// emitted MVCCKeyValue and MVCCRangeKeyValue.
func ScanSST(
	sst *kvpb.RangeFeedSSTable,
	scanWithin roachpb.Span,
	// TODO (msbutler): I think we can use a roachpb.kv instead, avoiding EncodeDecode roundtrip.
	mvccKeyValOp func(key storage.MVCCKeyValue) error,
	mvccRangeKeyValOp func(rangeKeyVal storage.MVCCRangeKeyValue) error,
) error {
	rangeKVs := make([]*storage.MVCCRangeKeyValue, 0)
	timestampToRangeKey := make(map[hlc.Timestamp]*storage.MVCCRangeKeyValue)
	// Iterator may release fragmented ranges, we try to de-fragment them
	// before we release kvpb.RangeFeedDeleteRange events.
	mergeRangeKV := func(rangeKV storage.MVCCRangeKeyValue) {
		// Range keys are emitted with increasing order in terms of start key,
		// so we only need to check if the current range key can be concatenated behind
		// previous one on the same timestamp.
		lastKV, ok := timestampToRangeKey[rangeKV.RangeKey.Timestamp]
		if ok && lastKV.RangeKey.EndKey.Equal(rangeKV.RangeKey.StartKey) {
			lastKV.RangeKey.EndKey = rangeKV.RangeKey.EndKey
			return
		}
		rangeKVs = append(rangeKVs, &rangeKV)
		timestampToRangeKey[rangeKV.RangeKey.Timestamp] = rangeKVs[len(rangeKVs)-1]
	}

	// We iterate points and ranges separately on the SST for clarity
	// and simplicity.
	pointIter, err := storage.NewMemSSTIterator(sst.Data, true,
		storage.IterOptions{
			KeyTypes: storage.IterKeyTypePointsOnly,
			// Only care about upper bound as we are iterating forward.
			UpperBound: scanWithin.EndKey,
		})
	if err != nil {
		return err
	}
	defer pointIter.Close()

	for pointIter.SeekGE(storage.MVCCKey{Key: scanWithin.Key}); ; pointIter.Next() {
		if valid, err := pointIter.Valid(); err != nil {
			return err
		} else if !valid {
			break
		}
		v, err := pointIter.Value()
		if err != nil {
			return err
		}
		if err = mvccKeyValOp(storage.MVCCKeyValue{
			Key:   pointIter.UnsafeKey().Clone(),
			Value: v,
		}); err != nil {
			return err
		}
	}

	rangeIter, err := storage.NewMemSSTIterator(sst.Data, true,
		storage.IterOptions{
			KeyTypes:   storage.IterKeyTypeRangesOnly,
			UpperBound: scanWithin.EndKey,
		})
	if err != nil {
		return err
	}
	defer rangeIter.Close()

	for rangeIter.SeekGE(storage.MVCCKey{Key: scanWithin.Key}); ; rangeIter.Next() {
		if valid, err := rangeIter.Valid(); err != nil {
			return err
		} else if !valid {
			break
		}
		for _, rangeKeyVersion := range rangeIter.RangeKeys().Versions {
			isTombstone, err := storage.EncodedMVCCValueIsTombstone(rangeKeyVersion.Value)
			if err != nil {
				return err
			}
			if !isTombstone {
				return errors.Errorf("only expect range tombstone from MVCC range key: %s", rangeIter.RangeBounds())
			}
			intersectedSpan := scanWithin.Intersect(rangeIter.RangeBounds())
			mergeRangeKV(storage.MVCCRangeKeyValue{
				RangeKey: storage.MVCCRangeKey{
					StartKey:  intersectedSpan.Key.Clone(),
					EndKey:    intersectedSpan.EndKey.Clone(),
					Timestamp: rangeKeyVersion.Timestamp},
				Value: rangeKeyVersion.Value,
			})
		}
	}
	for _, rangeKey := range rangeKVs {
		if err = mvccRangeKeyValOp(*rangeKey); err != nil {
			return err
		}
	}
	return nil
}

func GetStreamIngestionStats(
	ctx context.Context,
	streamIngestionDetails jobspb.StreamIngestionDetails,
	jobProgress jobspb.Progress,
) (*streampb.StreamIngestionStats, error) {
	stats := &streampb.StreamIngestionStats{
		IngestionDetails:  &streamIngestionDetails,
		IngestionProgress: jobProgress.GetStreamIngest(),
	}

	replicatedTime := ReplicatedTimeFromProgress(&jobProgress)
	if !replicatedTime.IsEmpty() {
		lagInfo := &streampb.StreamIngestionStats_ReplicationLagInfo{
			MinIngestedTimestamp: replicatedTime,
		}
		lagInfo.EarliestCheckpointedTimestamp = hlc.MaxTimestamp
		lagInfo.LatestCheckpointedTimestamp = hlc.MinTimestamp
		// TODO(casper): track spans that the slowest partition is associated
		for _, resolvedSpan := range jobProgress.GetStreamIngest().Checkpoint.ResolvedSpans {
			if resolvedSpan.Timestamp.Less(lagInfo.EarliestCheckpointedTimestamp) {
				lagInfo.EarliestCheckpointedTimestamp = resolvedSpan.Timestamp
			}

			if lagInfo.LatestCheckpointedTimestamp.Less(resolvedSpan.Timestamp) {
				lagInfo.LatestCheckpointedTimestamp = resolvedSpan.Timestamp
			}
		}
		lagInfo.SlowestFastestIngestionLag = lagInfo.LatestCheckpointedTimestamp.GoTime().
			Sub(lagInfo.EarliestCheckpointedTimestamp.GoTime())
		lagInfo.ReplicationLag = timeutil.Since(replicatedTime.GoTime())
		stats.ReplicationLagInfo = lagInfo
	}
	return stats, nil
}

func ReplicatedTimeFromProgress(p *jobspb.Progress) hlc.Timestamp {
	return p.Details.(*jobspb.Progress_StreamIngest).StreamIngest.ReplicatedTime
}

// LoadIngestionProgress loads the latest persisted stream ingestion progress.
// The method returns nil if the progress does not exist yet.
func LoadIngestionProgress(
	ctx context.Context, db isql.DB, jobID jobspb.JobID,
) (*jobspb.StreamIngestionProgress, error) {
	progress, err := jobs.LoadJobProgress(ctx, db, jobID)
	if err != nil || progress == nil {
		return nil, err
	}

	sp, ok := progress.GetDetails().(*jobspb.Progress_StreamIngest)
	if !ok {
		return nil, errors.Newf("unknown progress details type %T in stream ingestion job %d",
			progress.GetDetails(), jobID)
	}
	return sp.StreamIngest, nil
}

// LoadReplicationProgress loads the latest persisted stream replication progress.
// The method returns nil if the progress does not exist yet.
func LoadReplicationProgress(
	ctx context.Context, db isql.DB, jobID jobspb.JobID,
) (*jobspb.StreamReplicationProgress, error) {
	progress, err := jobs.LoadJobProgress(ctx, db, jobID)
	if err != nil || progress == nil {
		return nil, err
	}

	sp, ok := progress.GetDetails().(*jobspb.Progress_StreamReplication)
	if !ok {
		return nil, errors.Newf("unknown progress details type %T in stream replication job %d",
			progress.GetDetails(), jobID)
	}
	return sp.StreamReplication, nil
}

// InvestigateFingerprints checks that the src and dst cluster data match, table
// by table. It first computes and compares their stripped fingerprints to check
// that all the latest data matches; then it computes and compares their
// revision history fingerprints.
func InvestigateFingerprints(
	ctx context.Context, srcConn, dstConn *gosql.DB, startTime,
	cutoverTime hlc.Timestamp,
) error {
	strippedOpts := []func(*fingerprintutils.FingerprintOption){
		fingerprintutils.Stripped(),
		fingerprintutils.AOST(cutoverTime),
	}
	if err := fingerprintClustersByTable(ctx, srcConn, dstConn, strippedOpts...); err != nil {
		return fmt.Errorf("failed stripped fingerprint: %w", err)
	}

	opts := []func(*fingerprintutils.FingerprintOption){
		fingerprintutils.RevisionHistory(),
		fingerprintutils.StartTime(startTime),
		fingerprintutils.AOST(cutoverTime),
	}
	if err := fingerprintClustersByTable(ctx, srcConn, dstConn, opts...); err != nil {
		return fmt.Errorf("failed revision history fingerprint: %w", err)
	}
	return nil
}

func fingerprintClustersByTable(
	ctx context.Context,
	srcConn, dstConn *gosql.DB,
	optFuncs ...func(*fingerprintutils.FingerprintOption),
) error {
	g := ctxgroup.WithContext(ctx)
	var (
		srcFingerprints, dstFingerprints map[string]map[string]int64
	)
	g.Go(func() error {
		var err error
		srcFingerprints, err = fingerprintutils.FingerprintAllDatabases(ctx, srcConn, true,
			optFuncs...)
		if err != nil {
			return fmt.Errorf("failed getting src fingerprint: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		var err error
		dstFingerprints, err = fingerprintutils.FingerprintAllDatabases(ctx, dstConn, true,
			optFuncs...)
		if err != nil {
			return fmt.Errorf("failed getting dst fingerprint: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return fingerprintutils.CompareMultipleDatabaseFingerprints(srcFingerprints,
		dstFingerprints)
}

func TestingGetStreamIngestionStatsFromReplicationJob(
	t *testing.T, ctx context.Context, sqlRunner *sqlutils.SQLRunner, ingestionJobID int,
) *streampb.StreamIngestionStats {
	var payloadBytes []byte
	var progressBytes []byte
	var payload jobspb.Payload
	var progress jobspb.Progress
	stmt := fmt.Sprintf(`SELECT payload, progress FROM (%s)`, jobutils.InternalSystemJobsBaseQuery)
	sqlRunner.QueryRow(t, stmt, ingestionJobID).Scan(&payloadBytes, &progressBytes)
	require.NoError(t, protoutil.Unmarshal(payloadBytes, &payload))
	require.NoError(t, protoutil.Unmarshal(progressBytes, &progress))
	details := payload.GetStreamIngestion()
	stats, err := GetStreamIngestionStats(ctx, *details, progress)
	require.NoError(t, err)
	return stats
}
