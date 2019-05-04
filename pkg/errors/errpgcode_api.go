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

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/errpgcode"

// WithCandidateCode decorates the error with a candidate postgres
// error code. It is called "candidate" because the code is only used
// by GetPGCodeEx() below conditionally.
// The code is considered PII-free and is thus reportable.
var WithCandidateCode func(err error, code string) error = errpgcode.WithCandidateCode

// GetPGCodeEx retrieves a code for the error. It operates by
// combining the inner (cause) code and the code at the
// current level, at each level of cause.
// - at each level:
//   - if there is a candidate code at that level, that is used;
//   - otherwise, it calls computeDefaultCode().
//     if the function returns false in its second return value,
//     UncategorizedError is used.
// - after that, it calls combineCodes() with the code computed already
//   for the cause and the new code just computed at the current level.
// Two example functions for computeDefaultCode and combineCode are
// provided below
//
// For the actual GetPGCode() used throughout SQL, see pgerror.GetPGCode.
// It cannot be implemented here due to possible cyclical dependencies.
var GetPGCodeEx func(err error, compute ComputeCodeFn, combine CombineCodeFn) (code string) = errpgcode.GetPGCode

// ComputeCodeFn is the callback type suitable for GetPGCode.
type ComputeCodeFn = func(err error) (code string, ok bool)

// CombineCodeFn is the callback type suitable for GetPGCode.
type CombineCodeFn = func(innerCode, outerCode string) (code string)

// IsCandidateCode returns true iff the error (not its causes)
// has a candidate pg error code.
var IsCandidateCode func(err error) bool = errpgcode.IsCandidateCode

// HasCandidateCode returns tue iff the error or one of its causes
// has a candidate pg error code.
var HasCandidateCode func(err error) bool = errpgcode.HasCandidateCode
