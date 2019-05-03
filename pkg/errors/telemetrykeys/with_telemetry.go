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

package telemetrykeys

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withTelemetry struct {
	cause error
	keys  []string
}

func (w *withTelemetry) Error() string         { return w.cause.Error() }
func (w *withTelemetry) Cause() error          { return w.cause }
func (w *withTelemetry) Unwrap() error         { return w.cause }
func (w *withTelemetry) SafeDetails() []string { return w.keys }

func (w *withTelemetry) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.cause)
			fmt.Fprintf(s, "\n-- telemetry keys: %+v", w.keys)
			return
		}
		fallthrough
	case 's', 'q':
		errbase.FormatError(s, verb, w.cause)
	}
}

func decodeWithTelemetry(cause error, _ string, keys []string, _ protoutil.SimpleMessage) error {
	return &withTelemetry{cause: cause, keys: keys}
}

func init() {
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withTelemetry{}), decodeWithTelemetry)
}
