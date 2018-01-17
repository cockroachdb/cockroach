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
// permissions and limitations under the License.

package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryExceedsMaxBackoff(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Microsecond * 100,
		Multiplier:     2,
		MaxRetries:     10,
	}

	r := Start(opts)
	r.opts.RandomizationFactor = 0
	for i := 0; i < 10; i++ {
		d := r.retryIn()
		if d > opts.MaxBackoff {
			t.Fatalf("expected backoff less than max-backoff: %s vs %s", d, opts.MaxBackoff)
		}
		r.currentAttempt++
	}
}

func TestRetryExceedsMaxAttempts(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		MaxRetries:     1,
	}

	attempts := 0
	for r := Start(opts); r.Next(); attempts++ {
	}

	if expAttempts := opts.MaxRetries + 1; attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d attempts", expAttempts, attempts)
	}
}

func TestRetryReset(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		MaxRetries:     1,
	}

	expAttempts := opts.MaxRetries + 1

	attempts := 0
	// Backoff loop has 1 allowed retry; we always call Reset, so
	// just make sure we get to 2 attempts and then break.
	for r := Start(opts); r.Next(); attempts++ {
		if attempts == expAttempts {
			break
		}
		r.Reset()
	}
	if attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d", expAttempts, attempts)
	}
}

func TestRetryStop(t *testing.T) {
	closer := make(chan struct{})

	opts := Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		Closer:         closer,
	}

	var attempts int

	// Create a retry loop which will never stop without stopper.
	for r := Start(opts); r.Next(); attempts++ {
		go close(closer)
		// Don't race the stopper, just wait for it to do its thing.
		<-opts.Closer
	}

	if expAttempts := 1; attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d", expAttempts, attempts)
	}
}

func TestRetryNextCh(t *testing.T) {
	var attempts int

	opts := Options{
		InitialBackoff: time.Millisecond,
		Multiplier:     2,
		MaxRetries:     1,
	}
	for r := Start(opts); attempts < 3; attempts++ {
		c := r.NextCh()
		if r.currentAttempt != attempts {
			t.Errorf("expected attempt=%d; got %d", attempts, r.currentAttempt)
		}
		switch attempts {
		case 0:
			if c == nil {
				t.Errorf("expected non-nil NextCh() on first attempt")
			}
			if _, ok := <-c; ok {
				t.Errorf("expected closed (immediate) NextCh() on first attempt")
			}
		case 1:
			if c == nil {
				t.Errorf("expected non-nil NextCh() on second attempt")
			}
			if _, ok := <-c; !ok {
				t.Errorf("expected open (delayed) NextCh() on first attempt")
			}
		case 2:
			if c != nil {
				t.Errorf("expected nil NextCh() on third attempt")
			}
		default:
			t.Fatalf("unexpected attempt %d", attempts)
		}
	}
}

func TestRetryWithMaxAttempts(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		MaxRetries:     1,
	}

	attempts := 0
	const maxAttempts = 3
	expectedErr := errors.New("placeholder")
	errFn := func() error {
		attempts++
		return expectedErr
	}

	actualErr := WithMaxAttempts(context.TODO(), opts, maxAttempts, errFn)
	if actualErr != expectedErr {
		t.Fatalf("expected err %v, got %v", expectedErr, actualErr)
	}
	if attempts != maxAttempts {
		t.Errorf("expected %d attempts, got %d attempts", maxAttempts, attempts)
	}
}
