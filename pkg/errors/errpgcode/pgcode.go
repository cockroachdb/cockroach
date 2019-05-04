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

package errpgcode

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/errors/assert"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/issuelink"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/util/pgcode"
)

// WithCandidateCode decorates the error with a candidate postgres
// error code. It is called "candidate" because the code is only used
// by GetPGCode() below conditionally.
// The code is considered PII-free and is thus reportable.
func WithCandidateCode(err error, code string) error {
	if err == nil {
		return nil
	}

	return &withCandidateCode{cause: err, code: code}
}

// IsCandidateCode returns true iff the error (not its causes)
// has a candidate pg error code.
func IsCandidateCode(err error) bool {
	_, ok := err.(*withCandidateCode)
	return ok
}

// HasCandidateCode returns tue iff the error or one of its causes
// has a candidate pg error code.
func HasCandidateCode(err error) bool {
	_, ok := markers.If(err, func(err error) (v interface{}, ok bool) {
		v, ok = err.(*withCandidateCode)
		return
	})
	return ok
}

// GetPGCode retrieves a code for the error. It operates by
// combining the inner (cause) code and the code at the
// current level, at each level of cause.
//
// - at each level:
//
//   - if there is a candidate code at that level, that is used;
//   - otherwise, it calls computeDefaultCode().
//     if the function returns an empty string,
//     UncategorizedError is used.
//     An example implementation for computeDefaultCode is provided below.
//
// - after that, it combines the code computed already for the cause
//   (inner) and the new code just computed at the current level (outer)
//   as follows:
//
//   - if the outer code is uncategorized, the inner code is kept no
//     matter what.
//   - if the outer code has the special XX prefix, that is kept.
//     (The "XX" prefix signals importance in the pg code hierarchy.)
//   - if the inner code is not uncategorized, it is retained.
//   - otherwise the outer code is retained.
//
func GetPGCode(err error, computeDefaultCode func(err error) (code string)) (code string) {
	code = pgcode.Uncategorized
	if c, ok := err.(*withCandidateCode); ok {
		code = c.code
	} else if newCode := computeDefaultCode(err); newCode != "" {
		code = newCode
	}

	if c := errbase.UnwrapOnce(err); c != nil {
		innerCode := GetPGCode(c, computeDefaultCode)
		code = combineCodes(innerCode, code)
	}

	return code
}

// SimpleComputeDefaultCode returns InternalError
// for assertion failures and FeatureNotSupportedError
// for unimplemented errors.
func SimpleComputeDefaultCode(err error) string {
	if assert.IsAssertionFailure(err) {
		return pgcode.Internal
	}
	if issuelink.IsUnimplementedError(err) {
		return pgcode.FeatureNotSupported
	}
	return ""
}

// combineCodes combines the inner and outer codes.
func combineCodes(innerCode, outerCode string) string {
	if outerCode == pgcode.Uncategorized {
		return innerCode
	}
	if strings.HasPrefix(outerCode, "XX") {
		return outerCode
	}
	if innerCode != pgcode.Uncategorized {
		return innerCode
	}
	return outerCode
}
