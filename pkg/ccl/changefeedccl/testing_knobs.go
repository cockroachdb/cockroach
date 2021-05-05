// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// TestingKnobs are the testing knobs for changefeed.
type TestingKnobs struct {
	// BeforeEmitRow is called before every sink emit row operation.
	BeforeEmitRow func(context.Context) error
	// AfterSinkFlush is called after a sink flush operation has returned without
	// error.
	AfterSinkFlush func() error
	// MemMonitor, if non-nil, overrides memory monitor to use for changefeed..
	MemMonitor *mon.BytesMonitor
	// Dial, if set, is a function that overrides normal sink "dial" functionality.
	Dial func(s Sink)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
