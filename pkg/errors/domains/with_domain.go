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

package domains

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// withDomain is a wrapper type that adds a domain annotation to an
// error.
type withDomain struct {
	// Mandatory: error cause
	cause error
	// Mandatory: domain. This also must be free of PII
	// as it will be reported in "safe details".
	domain Domain
}

var _ error = &withDomain{}
var _ errbase.SafeDetailer = &withDomain{}
var _ errbase.TypeNameMarker = &withDomain{}

// withDomain is an error. The original error message is preserved.
func (e *withDomain) Error() string { return e.cause.Error() }

// the cause is reachable.
func (e *withDomain) Cause() error  { return e.cause }
func (e *withDomain) Unwrap() error { return e.cause }

// FullErrorTypeMarker implements the TypeNameMarker interface.
// The full type name of barriers is extended with the domain as extra marker.
// This ensures that domain-annotated errors appear to be of different types
// for the purpose of Is().
func (e *withDomain) FullErrorTypeMarker() string { return string(e.domain) }

// SafeDetails reports the domain.
func (e *withDomain) SafeDetails() []string {
	return []string{string(e.domain)}
}

// Printing a barrier reveals its domain.
func (e *withDomain) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", e.cause)
			fmt.Fprintf(s, "\n-- %s", e.domain)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, e.cause)
	}
}

// A domain-annotated error is decoded exactly.
func decodeWithDomain(cause error, _ string, details []string, _ protoutil.SimpleMessage) error {
	if len(details) == 0 {
		// decoding failure: expecting at least one detail string
		// (the one that carries the domain string).
		return nil
	}
	return &withDomain{cause: cause, domain: Domain(details[0])}
}

func init() {
	tn := errbase.FullTypeName(&withDomain{})
	errbase.RegisterWrapperDecoder(tn, decodeWithDomain)
}
