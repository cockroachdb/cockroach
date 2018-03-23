// Copyright 2018 The Cockroach Authors.
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

package contextutil

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCancels(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxFailures = 10
	const okChecks = 5

	counter := 0
	// Use a Checker that becomes false after a few iterations.
	ctx, cancel := WithCheck(context.Background(),
		OptionChecker(
			func() (interface{}, bool) {
				counter++
				return counter, counter <= okChecks
			}),
		OptionMaxFailures(maxFailures),
		OptionPeriod(time.Nanosecond),
	)

	<-ctx.Done()

	// Verify call count.
	if counter != maxFailures+okChecks {
		t.Fatalf("expected counter == %d, got %d", maxFailures+okChecks, counter)
	}

	switch err := ctx.Err().(type) {
	case *TooManyChecksFailedError:
		if maxFailures != len(err.Samples) {
			t.Fatalf("expecting %d samples, saw %d", maxFailures, len(err.Samples))
		}
		for idx, sample := range err.Samples {
			a, ok := sample.(int)
			if !ok {
				t.Fatalf("sample %d could not be cast to int", a)
			}
			e := idx + okChecks + 1
			if a != e {
				t.Fatalf("expecting sample %d == %d at index %d", a, e, idx)
			}
		}
	default:
		t.Fatal("unexpected error returned:", err)
	}

	// Ensure no-op behavior is ok.
	cancel()
}

func TestExplicitCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	child, childCancel := WithCheck(
		context.Background(), OptionPeriod(time.Hour))

	// Ensure that this doesn't blow up on no-op cancellation.
	childCancel()
	<-child.Done()
	if child.Err() != context.Canceled {
		t.Fatal("unexpected error from child context:", child.Err())
	}
}

func TestParentCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parent, parentCancel := context.WithCancel(context.Background())
	child, childCancel := WithCheck(parent, OptionPeriod(time.Hour))

	parentCancel()

	<-child.Done()
	if child.Err() != context.Canceled {
		t.Fatal("unexpected error from child context:", child.Err())
	}

	// Ensure that this doesn't blow up on no-op cancellation.
	childCancel()
	<-child.Done()
	if child.Err() != context.Canceled {
		t.Fatal("unexpected error from child context:", child.Err())
	}
}

// This creates a context that will execute the checker once a minute
// and allow up to five consecutive failures before canceling
// the context.
func ExampleWithCheck_eager() {
	ctx, cancel := WithCheck(context.Background(),
		OptionChecker(
			func() (interface{}, bool) {
				return "arbitrary sample value", true // If sample is OK
			}),
		OptionMaxFailures(5),
		OptionPeriod(1*time.Minute),
	)
	// You'll want to handle explicit cancellation.
	defer cancel()

	// Then do something with the Context.
	<-ctx.Done()
}

// This shows how the Checker can be set after the Context has been
// created.  This is useful for cases like starting an IO operation,
// where you might not be able to determine the rate until after
// the context must be created.
func ExampleWithCheck_lazy() {
	ctx, cancel := WithCheck(context.Background(),
		OptionMaxFailures(5),
		OptionPeriod(1*time.Minute),
	)
	// You'll want to handle explicit cancellation.
	defer cancel()

	server := startServer(ctx)

	ctx.SetChecker(func() (interface{}, bool) {
		return nil, server.isHealthy()
	})

	// The wait for things to complete.
	<-ctx.Done()
}

func startServer(_ context.Context) interface {
	isHealthy() bool
} {
	return nil
}
