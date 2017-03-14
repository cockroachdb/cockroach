// Copyright 2016 The Cockroach Authors.
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
//
// Author: Daniel Harrison (dan@cockroachlabs.com)

package syncutil_test

import (
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestWaitGroupNoError(t *testing.T) {
	var wg syncutil.WaitGroupWithError
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitGroupNilError(t *testing.T) {
	var wg syncutil.WaitGroupWithError
	wg.Add(1)
	wg.Done(nil)
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitGroupOneError(t *testing.T) {
	var wg syncutil.WaitGroupWithError
	wg.Add(1)
	origErr := errors.New("one")
	wg.Done(origErr)
	if err := wg.Wait(); err != origErr {
		t.Fatalf("expected the original error got: %+v", err)
	}
}

func TestWaitGroupTwoErrors(t *testing.T) {
	var wg syncutil.WaitGroupWithError
	wg.Add(2)
	wg.Done(errors.New("one"))
	wg.Done(errors.New("two"))
	if err := wg.Wait(); !testutils.IsError(err, "one") {
		t.Fatalf("expected 'one' error got: %+v", err)
	}
}
