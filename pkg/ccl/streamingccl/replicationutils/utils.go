// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package replicationutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

func GetStreamIngestionStatsNoHeartbeat(
	ctx context.Context,
	streamIngestionDetails jobspb.StreamIngestionDetails,
	jobProgress jobspb.Progress,
) (*streampb.StreamIngestionStats, error) {
	stats := &streampb.StreamIngestionStats{
		IngestionDetails:  &streamIngestionDetails,
		IngestionProgress: jobProgress.GetStreamIngest(),
	}
	if highwater := jobProgress.GetHighWater(); highwater != nil && !highwater.IsEmpty() {
		lagInfo := &streampb.StreamIngestionStats_ReplicationLagInfo{
			MinIngestedTimestamp: *highwater,
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
		lagInfo.ReplicationLag = timeutil.Since(highwater.GoTime())
		stats.ReplicationLagInfo = lagInfo
	}
	return stats, nil
}

func GetStreamIngestionStats(
	ctx context.Context,
	streamIngestionDetails jobspb.StreamIngestionDetails,
	jobProgress jobspb.Progress,
) (*streampb.StreamIngestionStats, error) {
	stats, err := GetStreamIngestionStatsNoHeartbeat(ctx, streamIngestionDetails, jobProgress)
	if err != nil {
		return nil, err
	}
	client, err := streamclient.GetFirstActiveClient(ctx, stats.IngestionProgress.StreamAddresses)
	if err != nil {
		return nil, err
	}
	streamStatus, err := client.Heartbeat(ctx, streampb.StreamID(stats.IngestionDetails.StreamID), hlc.MaxTimestamp)
	if err != nil {
		stats.ProducerError = err.Error()
	} else {
		stats.ProducerStatus = &streamStatus
	}
	return stats, client.Close(ctx)
}

func TestingGetStreamIngestionStatsNoHeartbeatFromReplicationJob(
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
	stats, err := GetStreamIngestionStatsNoHeartbeat(ctx, *details, progress)
	require.NoError(t, err)
	return stats
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
