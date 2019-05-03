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

package hintdetail

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/errorspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withHint struct {
	cause error
	hint  string
}

func (w *withHint) ErrorHint() string { return w.hint }
func (w *withHint) Error() string     { return w.cause.Error() }
func (w *withHint) Cause() error      { return w.cause }
func (w *withHint) Unwrap() error     { return w.cause }

func (w *withHint) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.cause)
			fmt.Fprintf(s, "\n-- hint:\n%s", w.hint)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, w.cause)
	}
}

func encodeWithHint(err error) (string, []string, protoutil.SimpleMessage) {
	w := err.(*withHint)
	return "", nil, &errorspb.StringPayload{Msg: w.hint}
}

func decodeWithHint(cause error, _ string, _ []string, payload protoutil.SimpleMessage) error {
	m := payload.(*errorspb.StringPayload)
	return &withHint{cause: cause, hint: m.Msg}
}

func init() {
	errbase.RegisterWrapperEncoder(errbase.FullTypeName(&withHint{}), encodeWithHint)
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withHint{}), decodeWithHint)
}
