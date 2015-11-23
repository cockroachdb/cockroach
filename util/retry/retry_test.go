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
	"testing"
	"time"
)

func TestRetryExceedsMaxBackoff(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Microsecond * 10,
		Multiplier:     1e5,
		MaxRetries:     1,
	}

	// tolerate a duration two orders of magnitude lower than if MaxBackoff is
	// not respected.
	fudgeFactor := opts.Multiplier / 100

	attempts := 0
	start := time.Now()
	for r := Start(opts); r.Next(); attempts++ {
	}

	duration := time.Since(start)

	if expDuration := time.Duration(fudgeFactor) * time.Duration(opts.MaxRetries+1) * opts.MaxBackoff; duration > expDuration {
		t.Errorf("expected retry loop to run for %s, actually ran for %s", expDuration, duration)
	}

	if expAttempts := opts.MaxRetries + 1; attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d attempts", expAttempts, attempts)
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
