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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// WithDefaultCode adds a code to the given error.
//
// If the error is not yet compatible with AnyError, it is converted
// into one. If the error already has a code other than CodeUncategorizedError,
// WithDefaultCode is a no-op.
// The code is added via WrapCodeError.
func WithDefaultCode(err error, code string) error {
	if err == nil || code == "" || code == pgerror.CodeUncategorizedError {
		return err
	}

	return &withCode{cause: err, code: code}
}

// GetCode finds the innermost code that applies to the error.
// defaultCode is returned if no code was found.
func GetCode(err error, defaultCode string) string {
	for {
		if isInternal, code := isInternalErrorLeaf(err); isInternal {
			// Any intermediate internal error object
			// overrides the inner code.
			return code
		}

		switch e := err.(type) {
		case *withCode:
			// An inner withCode overrides an outer one.
			defaultCode = e.code
			err = e.Cause()
			continue

		case *pgerror.Error:
			if e.Code == "" {
				return defaultCode
			}
			return e.Code

		case *roachpb.UnhandledRetryableError:
			return pgerror.CodeSerializationFailureError
		}

		if causer, ok := err.(causerI); ok {
			err = causer.Cause()
			continue
		}
		break
	}
	return defaultCode
}
