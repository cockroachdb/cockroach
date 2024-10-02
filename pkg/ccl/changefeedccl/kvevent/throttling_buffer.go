// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcutils"
)

type throttlingBuffer struct {
	Buffer
	throttle *cdcutils.Throttler
}

// NewThrottlingBuffer wraps specified event buffer with a throttle that
// regulates the rate of events returned by the buffer reader.
func NewThrottlingBuffer(b Buffer, throttle *cdcutils.Throttler) Buffer {
	return &throttlingBuffer{Buffer: b, throttle: throttle}
}

// Get implements kvevent.Reader interface.
func (b *throttlingBuffer) Get(ctx context.Context) (Event, error) {
	evt, err := b.Buffer.Get(ctx)
	if err != nil {
		return evt, err
	}

	if err := b.throttle.AcquireMessageQuota(ctx, evt.ApproximateSize()); err != nil {
		return Event{}, err
	}

	return evt, nil
}
