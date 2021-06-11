// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.Barrier, declareKeysMigrate, Barrier)
}

func declareKeysBarrier(
	rs ImmutableRangeState,
	_ roachpb.Header,
	_ roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	// No-op. Look at the special case in executeBatchWithConcurrencyRetries
	// to see how this barrier works to wait on existing write requests.
}

// Barrier evaluation is a no-op, as all the latch waiting happens in
// the latch manager.
func Barrier(
	_ context.Context, _ storage.ReadWriter, _ CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	return result.Result{}, nil
}
