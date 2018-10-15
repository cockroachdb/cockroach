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

// package logger defines a common logging interface.
package logger

import "context"

type Log interface {
	// Fatalf logs to the INFO, WARNING, ERROR, and FATAL logs and calls os.Exit.
	Fatalf(ctx context.Context, format string, args ...interface{})
	// Flush flushes all pending log I/O.
	Flush()
	// ResetExitFunc undoes any prior call to SetExitFunc.
	ResetExitFunc()
	// SetExitFunc allows setting a function that will be called to exit the
	// process when a Fatal message is generated. The supplied bool, if true,
	// suppresses the stack trace, which is useful for test callers wishing
	// to keep the logs reasonably clean.
	//
	// Call with a nil function to undo.
	SetExitFunc(hideStack bool, f func(int))
	// Warningf logs to the WARNING and INFO logs.
	Warningf(ctx context.Context, format string, args ...interface{})
}
