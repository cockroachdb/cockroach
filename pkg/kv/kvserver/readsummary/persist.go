// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package readsummary

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Load loads the range's prior read summary. The function returns a nil summary
// if one does not already exist.
func Load(
	ctx context.Context, reader storage.Reader, rangeID roachpb.RangeID,
) (*rspb.ReadSummary, error) {
	var sum rspb.ReadSummary
	key := keys.RangePriorReadSummaryKey(rangeID)
	found, err := storage.MVCCGetProto(ctx, reader, key, hlc.Timestamp{}, &sum, storage.MVCCGetOptions{})
	if !found {
		return nil, err
	}
	return &sum, err
}

// Set persists a range's prior read summary.
func Set(
	ctx context.Context,
	readWriter storage.ReadWriter,
	rangeID roachpb.RangeID,
	ms *enginepb.MVCCStats,
	sum *rspb.ReadSummary,
) error {
	key := keys.RangePriorReadSummaryKey(rangeID)
	return storage.MVCCPutProto(ctx, readWriter, key, hlc.Timestamp{}, sum, storage.MVCCWriteOptions{Stats: ms})
}
