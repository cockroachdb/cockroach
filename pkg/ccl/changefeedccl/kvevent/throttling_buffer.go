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

type throttlingReader struct {
	Reader
	throttle *cdcutils.Throttler
}

// NewThrottlingReader wraps specified event buffer with a throttle that
// regulates the rate of events returned by the buffer reader.
func NewThrottlingReader(r Reader, throttle *cdcutils.Throttler) Reader {
	return &throttlingReader{Reader: r, throttle: throttle}
}

// Get implements kvevent.Reader interface.
func (b *throttlingReader) Get(ctx context.Context) (Event, error) {
	evt, err := b.Reader.Get(ctx)
	if err != nil {
		return evt, err
	}

	if err := b.throttle.AcquireMessageQuota(ctx, evt.ApproximateSize()); err != nil {
		return Event{}, err
	}

	return evt, nil
}
