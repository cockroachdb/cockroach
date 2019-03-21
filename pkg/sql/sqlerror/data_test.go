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

package sqlerror

import (
	baseErrs "errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestErrorData verifies that errors survive the encode/decode cycle.
func TestErrorData(t *testing.T) {
	sentinels := []error{
		baseErrs.New("woo"),
		NewAssertionErrorf("woo"),
		&roachpb.TransactionRetryWithProtoRefreshError{},
		&roachpb.AmbiguousResultError{},
		&roachpb.UnhandledRetryableError{},
	}

	for _, sentinel := range sentinels {
		t.Run(fmt.Sprintf("%T", sentinel), func(t *testing.T) {
			testData := []struct {
				err error
			}{
				{sentinel},
				{WithMessagef(sentinel, "hello %s", "world")},
				{WithHintf(sentinel, "hello %s", "world")},
				{WithDetailf(sentinel, "hello %s", "world")},
				{Wrapf(sentinel, "hello %s", "world")},
				{NewAssertionErrorWithWrappedErrf(sentinel, "hello %s", "world")},
				{WithSource(1, sentinel)},
			}

			for _, test := range testData {
				t.Run(fmt.Sprintf("%T", test.err), func(t *testing.T) {
					encoded := EncodeError(test.err)
					decoded := encoded.GetError()

					if !reflect.DeepEqual(test.err, decoded) {
						t.Errorf(" expected:\n%+v\ngot:\n%+v", test.err, decoded)
					}
				})
			}
		})
	}

}
