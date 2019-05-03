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

type withDetail struct {
	cause  error
	detail string
}

func (w *withDetail) ErrorDetail() string { return w.detail }
func (w *withDetail) Error() string       { return w.cause.Error() }
func (w *withDetail) Cause() error        { return w.cause }
func (w *withDetail) Unwrap() error       { return w.cause }

func (w *withDetail) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.cause)
			fmt.Fprintf(s, "\n-- detail:\n%s", w.detail)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, w.cause)
	}
}

func encodeWithDetail(err error) (string, []string, protoutil.SimpleMessage) {
	w := err.(*withDetail)
	return "", nil, &errorspb.StringPayload{Msg: w.detail}
}

func decodeWithDetail(cause error, _ string, _ []string, payload protoutil.SimpleMessage) error {
	m := payload.(*errorspb.StringPayload)
	return &withDetail{cause: cause, detail: m.Msg}
}

func init() {
	errbase.RegisterWrapperEncoder(errbase.FullTypeName(&withDetail{}), encodeWithDetail)
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withDetail{}), decodeWithDetail)
}
