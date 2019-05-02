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

package secondary

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withSecondaryError struct {
	cause error

	// secondaryError is an additional error payload that provides
	// additional context towards troubleshooting.
	secondaryError error
}

// SafeDetails reports the PII-free details from the secondary error.
func (e *withSecondaryError) SafeDetails() []string {
	var details []string
	for err := e.secondaryError; err != nil; err = errbase.UnwrapOnce(err) {
		sd := errbase.GetSafeDetails(err)
		details = sd.Fill(details)
	}
	return details
}

// Printing a withSecondary reveals the details.
func (e *withSecondaryError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", e.cause)
			fmt.Fprintf(s, "\n-- additional error object:\n")
			errbase.FormatError(s, verb, e.secondaryError)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, e.cause)
	}
}

func (e *withSecondaryError) Error() string { return e.cause.Error() }
func (e *withSecondaryError) Cause() error  { return e.cause }
func (e *withSecondaryError) Unwrap() error { return e.cause }

func encodeWithSecondaryError(err error) (string, []string, protoutil.SimpleMessage) {
	e := err.(*withSecondaryError)
	enc := errbase.EncodeError(e.secondaryError)
	return "", nil, &enc
}

func decodeWithSecondaryError(
	cause error, _ string, _ []string, payload protoutil.SimpleMessage,
) error {
	enc := payload.(*errbase.EncodedError)
	return &withSecondaryError{cause: cause, secondaryError: errbase.DecodeError(*enc)}
}

func init() {
	tn := errbase.FullTypeName(&withSecondaryError{})
	errbase.RegisterWrapperDecoder(tn, decodeWithSecondaryError)
	errbase.RegisterWrapperEncoder(tn, encodeWithSecondaryError)
}
