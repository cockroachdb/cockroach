// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcutil

import "github.com/cockroachdb/redact"

// gRPC logging is a real redirect maze but luckily it doesn't matter to get
// this right since it actually uses the DepthLoggerV2 interface. The methods
// here are provided as a fallback implementation in case there are any stray
// callsites in gRPC using the old interface.

func (l *grpcLogger) Info(args ...interface{}) {
	l.InfoDepth(0, args...)
}

func (l *grpcLogger) Infoln(args ...interface{}) {
	l.InfoDepth(0, args...)
}

func (l *grpcLogger) Infof(format string, args ...interface{}) {
	l.InfoDepth(0, redact.Sprintf(format, args...))
}

func (l *grpcLogger) Warning(args ...interface{}) {
	l.WarningDepth(0, args...)
}

func (l *grpcLogger) Warningln(args ...interface{}) {
	l.WarningDepth(0, args...)
}

func (l *grpcLogger) Warningf(format string, args ...interface{}) {
	l.WarningDepth(0, args...)
}

func (l *grpcLogger) Error(args ...interface{}) {
	l.ErrorDepth(0, args...)
}

func (l *grpcLogger) Errorln(args ...interface{}) {
	l.ErrorDepth(0, args...)
}

func (l *grpcLogger) Errorf(format string, args ...interface{}) {
	l.ErrorDepth(0, redact.Sprintf(format, args...))
}

func (l *grpcLogger) Fatal(args ...interface{}) {
	l.FatalDepth(0, args...)
}

func (l *grpcLogger) Fatalln(args ...interface{}) {
	l.FatalDepth(0, args...)
}

func (l *grpcLogger) Fatalf(format string, args ...interface{}) {
	l.FatalDepth(0, redact.Sprintf(format, args...))
}

func (l *grpcLogger) V(i int) bool {
	// NB: V isn't used by gRPC despite being in the interface.
	return l.vDepth(i, 0)
}
