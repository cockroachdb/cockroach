// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logging

import "context"

// Logger defines an interface for logging.
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// MakeLogger returns a logger with the prefix
func MakeLogger(ctx context.Context, prefix string) Logger {
	return getZapLogger(ctx, prefix)
}
