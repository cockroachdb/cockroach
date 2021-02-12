// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	return storage.MVCCPutProto(ctx, readWriter, ms, key, hlc.Timestamp{}, nil, sum)
}
