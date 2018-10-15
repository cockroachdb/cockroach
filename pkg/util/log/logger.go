// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/logger"
)

type globalLogger struct{}

// Logger is a wrapper around logging statements using the global state of
// the log package. It implements logger.Log.
var Logger globalLogger

var _ logger.Log = globalLogger{}

func (globalLogger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	Fatalf(ctx, format, args...)
}

func (globalLogger) Flush() {
	Flush()
}

func (globalLogger) ResetExitFunc() {
	ResetExitFunc()
}

func (globalLogger) SetExitFunc(hideStack bool, f func(int)) {
	SetExitFunc(hideStack, f)
}

func (globalLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	Warningf(ctx, format, args...)
}
