// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
)

type pebbleLogger struct {
	ctx   context.Context
	depth int
}

var _ pebble.LoggerAndTracer = pebbleLogger{}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	log.Storage.InfofDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Storage.FatalfDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Errorf(format string, args ...interface{}) {
	log.Storage.ErrorfDepth(l.ctx, l.depth, format, args...)
}

// pebble.LoggerAndTracer does not expose verbosity levels in its logging
// interface, and Pebble logs go to a separate STORAGE channel.
//
// The tracing part of the interface is meant for user-facing activities, so
// in addition to outputting the event when tracing is enabled, we also log.
// The eventAlsoLogVerbosityLevel of 2 is chosen semi-arbitrarily since this
// is the only verbosity level in this file.
const eventAlsoLogVerbosityLevel = 2

func (l pebbleLogger) Eventf(ctx context.Context, format string, args ...interface{}) {
	log.VEventfDepth(ctx, l.depth, eventAlsoLogVerbosityLevel, format, args...)
}

func (l pebbleLogger) IsTracingEnabled(ctx context.Context) bool {
	return log.HasSpan(ctx) || log.ExpensiveLogEnabledVDepth(ctx, l.depth, eventAlsoLogVerbosityLevel)
}
