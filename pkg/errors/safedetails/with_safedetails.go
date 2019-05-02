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

package safedetails

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withSafeDetails struct {
	cause error

	safeDetails []string
}

func (e *withSafeDetails) SafeDetails() []string {
	return e.safeDetails
}

// Printing a withSecondary reveals the details.
func (e *withSafeDetails) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", e.cause)
			if len(e.safeDetails) > 0 {
				fmt.Fprintf(s, "\n-- safe details:")
				for _, d := range e.safeDetails {
					fmt.Fprintf(s, "\n%s", d)
				}
			}
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, e.cause)
	}
}

func (e *withSafeDetails) Error() string { return e.cause.Error() }
func (e *withSafeDetails) Cause() error  { return e.cause }
func (e *withSafeDetails) Unwrap() error { return e.cause }

func decodeWithSafeDetails(
	cause error, _ string, safeDetails []string, _ protoutil.SimpleMessage,
) error {
	return &withSafeDetails{cause: cause, safeDetails: safeDetails}
}

func init() {
	tn := errbase.FullTypeName(&withSafeDetails{})
	errbase.RegisterWrapperDecoder(tn, decodeWithSafeDetails)
	// Note: no encoder needed, the default implementation is suitable.
}
