// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"reflect"
	"testing"
)

func TestClientCmdIDIsEmpty(t *testing.T) {
	if !(ClientCmdID{}).IsEmpty() {
		t.Error("expected cmd to be empty")
	}
	if (ClientCmdID{WallTime: 1}).IsEmpty() {
		t.Error("expected cmd to not be empty")
	}
	if (ClientCmdID{Random: 1}).IsEmpty() {
		t.Error("expected cmd to not be empty")
	}
}

type testError struct{}

func (t *testError) Error() string  { return "test" }
func (t *testError) CanRetry() bool { return true }

// TestResponseHeaderSetGoError verifies that a test error that
// implements retryable is converted properly into a generic error.
func TestResponseHeaderSetGoError(t *testing.T) {
	rh := ResponseHeader{}
	rh.SetGoError(&testError{})
	err := rh.GoError()
	if reflect.TypeOf(err) != reflect.TypeOf(&GenericError{}) {
		t.Errorf("expected set error to be type GenericError; got %s", reflect.TypeOf(err))
	}
	if !err.(*GenericError).Retryable {
		t.Error("expected generic error to be retryable")
	}
}
