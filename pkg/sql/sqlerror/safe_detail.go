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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// internalWithSafeDetailf adds a PII-free (safe) message and stack
// trace at the specified depth.
func internalWithSafeDetailf(
	depth int, err error, st *log.StackTrace, format string, args ...interface{},
) error {
	src := pgerror.MakeSrcCtx(depth + 1)
	return &withSafeDetail{
		cause: err,
		detail: &SafeDetailPayload{
			SafeMessage:       log.ReportablesToSafeError(depth+1, format, args).Error(),
			EncodedStackTrace: log.EncodeStackTrace(st),
			Source:            &src,
		},
	}
}

// WithSource adds the caller information at the specified depth.
func WithSource(depth int, err error) error {
	if err == nil {
		return err
	}

	src := pgerror.MakeSrcCtx(depth + 1)
	return &withSource{
		cause:  err,
		source: &src,
	}
}
