// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import "context"

// BlockingBufferTestingKnobs are testing knobs for blocking buffers.
type BlockingBufferTestingKnobs struct {
	// BeforeAdd is called before adding a KV event to the buffer. If the function
	// returns false in the third return value, the event is skipped.
	BeforeAdd            func(ctx context.Context, e Event) (_ context.Context, _ Event, shouldAdd bool)
	BeforePop            func()
	BeforeDrain          func(ctx context.Context) context.Context
	AfterDrain           func(err error)
	AfterCloseWithReason func(err error)
}
