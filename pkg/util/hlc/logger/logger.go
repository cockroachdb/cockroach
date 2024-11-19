// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// CRDBLogger is a hlc.Logger that is implemented using the CRDB
// standard util/log package.
var CRDBLogger = &logLogger{}

type logLogger struct{}

func (*logLogger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	log.Fatalf(ctx, format, args...) // nolint:fmtsafe
}
func (*logLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Warningf(ctx, format, args...) // nolint:fmtsafe
}

type Logfer interface {
	Logf(string, ...interface{})
}

// TestLogger is a hlc.Logger that can be used in tests to determine
// if a fatal error has been encountered by the hlc.Clock.
type TestLogger struct {
	l           Logfer
	fatalCalled atomic.Bool
}

func NewTestLogger(l Logfer) *TestLogger {
	return &TestLogger{l: l}
}

func (t *TestLogger) Warningf(_ context.Context, format string, args ...interface{}) {
	t.l.Logf("warning:"+format, args...)
}

func (t *TestLogger) Fatalf(_ context.Context, format string, args ...interface{}) {
	t.l.Logf("fatal:"+format, args...)
	t.fatalCalled.Store(true)
}
func (t *TestLogger) ResetFatal()       { t.fatalCalled.Store(false) }
func (t *TestLogger) FatalCalled() bool { return t.fatalCalled.Load() }
