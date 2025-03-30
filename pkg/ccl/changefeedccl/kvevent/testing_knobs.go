// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import "context"

// BlockingBufferTestingKnobs are testing knobs for blocking buffers.
type BlockingBufferTestingKnobs struct {
	BeforeAdd            func(ctx context.Context, e Event) (context.Context, Event)
	BeforePop            func()
	BeforeDrain          func(ctx context.Context) context.Context
	AfterDrain           func(err error)
	AfterCloseWithReason func(err error)
}
