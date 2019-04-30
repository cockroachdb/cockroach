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

package barriers

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Handled swallows the provided error and hides is from the
// Cause()/Unwrap() interface, and thus the Is() facility that
// identifies causes. However, it retains it for the purpose of
// printing the error out (e.g. for troubleshooting). The error
// message is preserved in full.
func Handled(err error) error {
	if err == nil {
		return nil
	}
	return HandledWithMessage(err, err.Error())
}

// HandledWithMessage is like Handled except the message is overridden.
// This can be used e.g. to hide message details or to prevent
// downstream code to make assertions on the message's contents.
func HandledWithMessage(err error, msg string) error {
	if err == nil {
		return nil
	}
	return &barrierError{maskedErr: err, msg: msg}
}

// HandledWithMessagef is like HandledWithMessagef except the message
// is formatted.
func HandledWithMessagef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &barrierError{maskedErr: err, msg: fmt.Sprintf(format, args...)}
}

// barrierError is a leaf error type. It encapsulates a chain of
// original causes, but these causes are hidden so that they inhibit
// matching via Is() and the Cause()/Unwrap() recursions.
type barrierError struct {
	// Message for the barrier itself.
	// In the common case, the message from the masked error
	// is used as-is (see Handled() above) however it is
	// useful to cache it here since the masked error may
	// have a long chain of wrappers and its Error() call
	// may be expensive.
	msg string
	// Masked error chain.
	maskedErr error
}

var _ error = &barrierError{}
var _ errbase.SafeDetailer = &barrierError{}

// barrierError is an error.
func (e *barrierError) Error() string { return e.msg }

// SafeDetails reports the PII-free details from the masked error.
func (e *barrierError) SafeDetails() []string {
	var details []string
	for err := e.maskedErr; err != nil; err = errbase.UnwrapOnce(err) {
		sd := errbase.GetSafeDetails(err)
		details = sd.Fill(details)
	}
	return details
}

// Printing a barrier reveals the details.
func (e *barrierError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s", e.msg)
			if e.maskedErr != nil {
				fmt.Fprintf(s, "\n-- original cause:\n")
				errbase.FormatError(s, verb, e.maskedErr)
			}
			return
		}
		fallthrough
	case 's', 'q':
		fmt.Fprintf(s, fmt.Sprintf("%%%c", verb), e.msg)
	}
}

// A barrier error is encoded exactly.
func encodeBarrier(err error) (msg string, details []string, payload protoutil.SimpleMessage) {
	e := err.(*barrierError)
	enc := errbase.EncodeError(e.maskedErr)
	return e.msg, e.SafeDetails(), &enc
}

// A barrier error is decoded exactly.
func decodeBarrier(msg string, _ []string, payload protoutil.SimpleMessage) error {
	enc := payload.(*errbase.EncodedError)
	return &barrierError{msg: msg, maskedErr: errbase.DecodeError(*enc)}
}

func init() {
	tn := errbase.FullTypeName(&barrierError{})
	errbase.RegisterLeafDecoder(tn, decodeBarrier)
	errbase.RegisterLeafEncoder(tn, encodeBarrier)
}
