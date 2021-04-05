// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type errorWrapperEventBuffer struct {
	Buffer
}

// NewErrorWrapperEventBuffer returns kvevent Buffer which treats any errors
// as retryable.
func NewErrorWrapperEventBuffer(b Buffer) Buffer {
	return &errorWrapperEventBuffer{b}
}

// AddKV implements kvevent.Writer interface.
func (e errorWrapperEventBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	if err := e.Buffer.AddKV(ctx, kv, prevVal, backfillTimestamp); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

// AddResolved implements kvevent.Writer interface.
func (e errorWrapperEventBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	if err := e.Buffer.AddResolved(ctx, span, ts, boundaryType); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

var _ Buffer = (*errorWrapperEventBuffer)(nil)
