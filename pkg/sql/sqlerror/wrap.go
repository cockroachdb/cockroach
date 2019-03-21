// Copyright 2019 The Cockroach Authors.
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
//

package sqlerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WrapWithDepthf is an advanced replacement for pgerror.WrapWithDepthf.
// It preserves the errors.Causer() interface while being able to
// produce fully decorated pgerror.Error objects via the Flatten() function.
func WrapWithDepthf(depth int, err error, code, format string, args ...interface{}) error {
	if err == nil {
		// Shortcut: although the functions below do the same, this takes less time.
		return err
	}

	// Add a message prefix.
	err = WithMessagef(err, format, args...)

	// If there's a default code, add it.
	if code != "" && code != pgerror.CodeUncategorizedError {
		err = WithDefaultCode(err, code)
	}

	// For internal errors and if verbosity is sufficiently high, add
	// more reportable details.
	if errCode := GetCode(err, code); errCode == pgerror.CodeInternalError || log.V(2) {
		st := log.NewStackTrace(depth + 1)
		err = internalWithSafeDetailf(depth+1, err, st, format, args...)
		err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	} else {
		// Otherwise, simply add the source code context.
		err = WithSource(depth+1, err)
	}

	return err
}

func init() {
	// Override the base function with the advanced code.
	pgerror.WrapWithDepthf = WrapWithDepthf
}

// Wrapf aliases the function in pgerror for convenience.
var Wrapf = pgerror.Wrapf

// Wrap aliases the function in pgerror for convenience.
var Wrap = pgerror.Wrap

// NewError aliases the function in pgerror for convenience
var NewError = pgerror.NewError

// WithMessagef adds a message.
func WithMessagef(err error, format string, args ...interface{}) error {
	if err == nil || (format == "" && len(args) == 0) {
		// No error, or no message to add. Do nothing.
		return err
	}
	return &withMessage{cause: err, msg: fmt.Sprintf(format, args...)}
}
