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

package retry

import (
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/cockroach/util"
)

func TestRetry(t *testing.T) {
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Second,
			Multiplier:      2,
		},
		Tag:         "test",
		MaxAttempts: 10,
	}

	var retries uint
	err := WithBackoff(opts, func(r *Retry) error {
		retries++
		if retries == 3 {
			return nil
		}
		return fmt.Errorf("try again")
	})
	if err != nil || retries != 3 {
		t.Error("expected 3 retries, got", retries, ":", err)
	}
}

func TestRetryExceedsMaxBackoff(t *testing.T) {
	timer := time.AfterFunc(time.Second, func() {
		t.Error("max backoff not respected")
	})
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Microsecond * 10,
			Multiplier:      1000,
		},
		Tag:         "test",
		MaxAttempts: 3,
	}
	err := WithBackoff(opts, func(r *Retry) error {
		return fmt.Errorf("try again")
	})
	if _, ok := err.(*MaxAttemptsError); !ok {
		t.Errorf("should receive max attempts error on retry: %s", err)
	}
	timer.Stop()
}

func TestRetryExceedsMaxAttempts(t *testing.T) {
	var retries uint
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Second,
			Multiplier:      2,
		},
		Tag:         "test",
		MaxAttempts: 3,
	}
	err := WithBackoff(opts, func(r *Retry) error {
		retries++
		return fmt.Errorf("try again")
	})
	if _, ok := err.(*MaxAttemptsError); !ok {
		t.Errorf("should receive max attempts error on retry: %s", err)
	}
	if retries != 3 {
		t.Error("expected 3 retries, got", retries)
	}
}

func TestRetryFunctionReturnsError(t *testing.T) {
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Second,
			Multiplier:      2,
		},
		Tag: "test",
	}
	err := WithBackoff(opts, func(r *Retry) error {
		r.Stop()
		return fmt.Errorf("something went wrong")
	})
	if err == nil {
		t.Error("expected an error")
	}
}

func TestRetryReset(t *testing.T) {
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Second,
			Multiplier:      2,
		},
		MaxAttempts: 1,
		Tag:         "test",
	}
	var count uint
	// Backoff loop has 1 allowed retry; we always return Reset, so
	// just make sure we get to 2 retries and then break.
	if err := WithBackoff(opts, func(r *Retry) error {
		count++
		if count == 2 {
			return nil
		}
		r.Reset()
		return fmt.Errorf("try again immediately")
	}); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if count != 2 {
		t.Errorf("expected 2 retries; got %d", count)
	}
}

func TestRetryStop(t *testing.T) {
	stopper := util.NewStopper()
	// Create a retry loop which will never stop without stopper.
	opts := Options{
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: time.Microsecond * 10,
			MaxInterval:     time.Second,
			Multiplier:      2,
		},
		Tag:     "test",
		Stopper: stopper,
	}
	if err := WithBackoff(opts, func(r *Retry) error {
		go stopper.Stop()
		return fmt.Errorf("try again")
	}); err == nil {
		t.Errorf("expected retry loop to exit from being stopped")
	}
}
