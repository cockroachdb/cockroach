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
func (b *throttlingBuffer) Get(ctx context.Context) (Event, Resource, error) {
	evt, r, err := b.Buffer.Get(ctx)
	if err != nil {
		return evt, r, err
	}

	sr := MakeScopedResource(r)
	defer sr.Release()

	if err := b.throttle.AcquireMessageQuota(ctx, evt.ApproximateSize()); err != nil {
		sr.Release()
		return Event{}, NoResource, err
	}

	return evt, sr.Move(), nil
}
