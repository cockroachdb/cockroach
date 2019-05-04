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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withCandidateCode struct {
	cause error
	code  string
}

func (w *withCandidateCode) Error() string         { return w.cause.Error() }
func (w *withCandidateCode) Cause() error          { return w.cause }
func (w *withCandidateCode) Unwrap() error         { return w.cause }
func (w *withCandidateCode) SafeDetails() []string { return []string{w.code} }

func (w *withCandidateCode) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.cause)
			fmt.Fprintf(s, "\n-- candidate pg code: %s", w.code)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, w.cause)
	}
}

func decodeWithCandidateCode(
	cause error, _ string, details []string, _ protoutil.SimpleMessage,
) error {
	code := pgcode.Uncategorized
	if len(details) > 0 {
		code = details[0]
	}
	return &withCandidateCode{cause: cause, code: code}
}

func init() {
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withCandidateCode{}), decodeWithCandidateCode)
}
