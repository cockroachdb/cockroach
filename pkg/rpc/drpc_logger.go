// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"storj.io/drpc"
)

// drpcLogger implements the drpc.Logger interface, routing DRPC log messages
// to CockroachDB's DEV logging channel, the same channel used for gRPC
// library logs. Debugf is routed through verbose logging (VEventfDepth) at
// verbosity level 2, so debug messages are suppressed by default and only
// appear when an operator enables verbose logging (--vmodule or --verbosity).
type drpcLogger struct {
	ctx   context.Context
	depth int
}

var _ drpc.Logger = drpcLogger{}

func (l drpcLogger) Debugf(format string, args ...interface{}) {
	log.Dev.VEventfDepth(l.ctx, l.depth+1, 2, format, args...)
}

func (l drpcLogger) Infof(format string, args ...interface{}) {
	log.Dev.InfofDepth(l.ctx, l.depth+1, format, args...)
}

func (l drpcLogger) Errorf(format string, args ...interface{}) {
	log.Dev.ErrorfDepth(l.ctx, l.depth+1, format, args...)
}

func (l drpcLogger) Fatalf(format string, args ...interface{}) {
	log.Dev.FatalfDepth(l.ctx, l.depth+1, format, args...)
}
