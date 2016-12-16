// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package utilccl

import (
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestWaitGroupNoError(t *testing.T) {
	var wg WaitGroupWithError
	wg.Wait()
	if err := wg.FirstError(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitGroupNilError(t *testing.T) {
	var wg WaitGroupWithError
	wg.Add(1)
	wg.Done(nil)
	wg.Wait()
	if err := wg.FirstError(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitGroupOneError(t *testing.T) {
	var wg WaitGroupWithError
	wg.Add(1)
	wg.Done(errors.New("one"))
	wg.Wait()
	if err := wg.FirstError(); !testutils.IsError(err, "one") {
		t.Fatalf("expected 'one' error got: %+v", err)
	}
}

func TestWaitGroupTwoErrors(t *testing.T) {
	var wg WaitGroupWithError
	wg.Add(2)
	wg.Done(errors.New("one"))
	wg.Done(errors.New("two"))
	wg.Wait()
	if err := wg.FirstError(); !testutils.IsError(err, "one") {
		t.Fatalf("expected 'one' error got: %+v", err)
	}
}
