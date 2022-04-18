// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// AddSSTableOpName is the operation name for the child span associated with an
	// AddSSTable request.
	AddSSTableOpName = "AddSSTable"

	// AdminSplitOpName is the operation name for the child span associated with an
	// AdminSplit request.
	AdminSplitOpName = "AdminSplit"

	// AdminScatterOpName is the operation name for the child span associated with an
	// AdminScatter request.
	AdminScatterOpName = "AdminScatter"
)

// addSSTableWithTracing is a utility method that sends an AddSSTable request,
// and returns the response, time taken to process the request, and stats
// associated with the request.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func addSSTableWithTracing(
	ctx context.Context,
	item *sstSpan,
	b *SSTBatcher,
	ingestAsWriteBatch bool,
	recordingType tracing.RecordingType,
) (*roachpb.BatchResponse, time.Duration, *roachpb.BulkRequestStats, *roachpb.Error) {
	ctx, childSpan := tracing.ChildSpan(ctx, AddSSTableOpName)
	req := &roachpb.AddSSTableRequest{
		RequestHeader:                          roachpb.RequestHeader{Key: item.start, EndKey: item.end},
		Data:                                   item.sstBytes,
		DisallowShadowing:                      !b.disallowShadowingBelow.IsEmpty(),
		DisallowShadowingBelow:                 b.disallowShadowingBelow,
		MVCCStats:                              &item.stats,
		IngestAsWrites:                         ingestAsWriteBatch,
		ReturnFollowingLikelyNonEmptySpanStart: true,
	}
	if b.writeAtBatchTS {
		req.SSTTimestampToRequestTimestamp = b.batchTS
	}

	ba := roachpb.BatchRequest{
		Header: roachpb.Header{
			Timestamp:       b.batchTS,
			ClientRangeInfo: roachpb.ClientRangeInfo{ExplicitlyRequested: true}},
		AdmissionHeader: roachpb.AdmissionHeader{
			Priority:                 int32(admission.BulkNormalPri),
			CreateTime:               timeutil.Now().UnixNano(),
			Source:                   roachpb.AdmissionHeader_FROM_SQL,
			NoMemoryReservedAtSource: true,
		},
	}
	ba.Add(req)
	br, pErr := b.db.NonTransactionalSender().Send(ctx, ba)
	if pErr != nil {
		childSpan.Finish()
		return nil, 0, nil, pErr
	}

	// Construct and emit a structured recording corresponding to this request.
	storeIDs := make([]roachpb.StoreID, 0)
	if br != nil {
		for _, ri := range br.BatchResponse_Header.RangeInfos {
			storeIDs = append(storeIDs, ri.Lease.Replica.StoreID)
		}
	}
	addSSTStats := &roachpb.AddSSTableStats{
		DataSizeBytes: int64(sz(len(req.Data))),
		Stores:        storeIDs,
		IngestAsWrite: req.IngestAsWrites,
	}
	childSpan.RecordStructured(addSSTStats)

	var duration time.Duration
	rec := childSpan.FinishAndGetRecording(recordingType)
	addSSTSpan, ok := rec.FindSpan(AddSSTableOpName)
	if ok {
		duration = addSSTSpan.Duration
	}

	stats := &roachpb.BulkRequestStats{
		Value: &roachpb.BulkRequestStats_AddSSTable{AddSSTable: addSSTStats}}
	return br, duration, stats, pErr
}

// adminSplitWithTracing is a utility method that return the time taken to
// process the request.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func adminSplitWithTracing(
	ctx context.Context,
	db *kv.DB,
	splitKey roachpb.Key,
	expirationTime hlc.Timestamp,
	recordingType tracing.RecordingType,
	predicateKeys ...roachpb.Key,
) (time.Duration, *roachpb.BulkRequestStats, error) {
	ctx, childSpan := tracing.ChildSpan(ctx, AdminSplitOpName)
	if err := db.AdminSplit(ctx, splitKey, expirationTime, predicateKeys...); err != nil {
		childSpan.Finish()
		return 0, nil, err
	}

	var duration time.Duration
	rec := childSpan.FinishAndGetRecording(recordingType)
	adminSplitSpan, ok := rec.FindSpan(AdminSplitOpName)
	if ok {
		duration = adminSplitSpan.Duration
	}

	stats := &roachpb.BulkRequestStats{
		Value: &roachpb.BulkRequestStats_AdminSplit{AdminSplit: &roachpb.AdminSplitStats{}}}
	return duration, stats, nil
}

// adminScatterWithTracing is a utility method that returns the response, time
// taken to process the request, and stats associated with the AdminScatter
// request.
//
// The recording mode used when fetching the span recording is specified using
// recordingType.
func adminScatterWithTracing(
	ctx context.Context,
	db *kv.DB,
	key roachpb.Key,
	maxSize int64,
	recordingType tracing.RecordingType,
) (roachpb.Response, time.Duration, *roachpb.BulkRequestStats, error) {
	ctx, childSpan := tracing.ChildSpan(ctx, AdminScatterOpName)
	rawRes, err := db.AdminScatter(ctx, key, maxSize)
	if err != nil {
		childSpan.Finish()
		return nil, 0, nil, err
	}

	var adminScatterStats *roachpb.AdminScatterStats
	if rawRes.MVCCStats != nil {
		adminScatterStats = &roachpb.AdminScatterStats{
			DataInScatteredRangesBytes: rawRes.MVCCStats.Total(),
		}
		childSpan.RecordStructured(adminScatterStats)
	}

	var duration time.Duration
	rec := childSpan.FinishAndGetRecording(recordingType)
	scatterSpan, ok := rec.FindSpan(AdminScatterOpName)
	if ok {
		duration = scatterSpan.Duration
	}

	stats := &roachpb.BulkRequestStats{
		Value: &roachpb.BulkRequestStats_AdminScatter{AdminScatter: adminScatterStats}}
	return rawRes, duration, stats, nil
}
