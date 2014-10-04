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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"fmt"
	"testing"
	"time"
)

func TestRetry(t *testing.T) {
	opts := RetryOptions{"test", time.Microsecond * 10, time.Second, 2, 10, false}
	var retries int
	err := RetryWithBackoff(opts, func() (RetryStatus, error) {
		retries++
		if retries >= 3 {
			return RetryBreak, nil
		}
		return RetryContinue, nil
	})
	if err != nil || retries != 3 {
		t.Error("expected 3 retries, got", retries, ":", err)
	}
}

func TestRetryExceedsMaxBackoff(t *testing.T) {
	timer := time.AfterFunc(time.Second, func() {
		t.Error("max backoff not respected")
	})
	opts := RetryOptions{"test", time.Microsecond * 10, time.Microsecond * 10, 1000, 3, false}
	err := RetryWithBackoff(opts, func() (RetryStatus, error) {
		return RetryContinue, nil
	})
	if _, ok := err.(*RetryMaxAttemptsError); !ok {
		t.Errorf("should receive max attempts error on retry: %s", err)
	}
	timer.Stop()
}

func TestRetryExceedsMaxAttempts(t *testing.T) {
	var retries int
	opts := RetryOptions{"test", time.Microsecond * 10, time.Second, 2, 3, false}
	err := RetryWithBackoff(opts, func() (RetryStatus, error) {
		retries++
		return RetryContinue, nil
	})
	if _, ok := err.(*RetryMaxAttemptsError); !ok {
		t.Errorf("should receive max attempts error on retry: %s", err)
	}
	if retries != 3 {
		t.Error("expected 3 retries, got", retries)
	}
}

func TestRetryFunctionReturnsError(t *testing.T) {
	opts := RetryOptions{"test", time.Microsecond * 10, time.Second, 2, 0 /* indefinite */, false}
	err := RetryWithBackoff(opts, func() (RetryStatus, error) {
		return RetryBreak, fmt.Errorf("something went wrong")
	})
	if err == nil {
		t.Error("expected an error")
	}
}

func TestRetryReset(t *testing.T) {
	opts := RetryOptions{"test", time.Microsecond * 10, time.Second, 2, 1, false}
	var count int
	// Backoff loop has 1 allowed retry; we always return RetryReset, so
	// just make sure we get to 2 retries and then break.
	if err := RetryWithBackoff(opts, func() (RetryStatus, error) {
		count++
		if count == 2 {
			return RetryBreak, nil
		}
		return RetryReset, nil
	}); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if count != 2 {
		t.Errorf("expected 2 retries; got %d", count)
	}
}
