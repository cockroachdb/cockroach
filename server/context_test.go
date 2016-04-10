// Copyright 2015 The Cockroach Authors.
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
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package server

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestParseInitNodeAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := NewContext()
	ctx.Attrs = "attr1=val1::attr2=val2"
	ctx.Stores = StoreSpecList{Specs: []StoreSpec{{InMemory: true, SizeInBytes: minimumStoreSize * 100}}}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	if err := ctx.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}

	if a, e := ctx.NodeAttributes.Attrs, []string{"attr1=val1", "attr2=val2"}; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected attributes: %v, found: %v", e, a)
	}
}

// TestParseJoinUsingAddrs verifies that JoinUsing is parsed
// correctly.
func TestParseJoinUsingAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := NewContext()
	ctx.JoinUsing = "localhost:12345,,localhost:23456"
	ctx.Stores = StoreSpecList{Specs: []StoreSpec{{InMemory: true, SizeInBytes: minimumStoreSize * 100}}}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	if err := ctx.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}
	r1, err := resolver.NewResolver(&ctx.Context, "localhost:12345")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := resolver.NewResolver(&ctx.Context, "localhost:23456")
	if err != nil {
		t.Fatal(err)
	}
	expected := []resolver.Resolver{r1, r2}
	if !reflect.DeepEqual(ctx.GossipBootstrapResolvers, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", ctx.GossipBootstrapResolvers, expected)
	}
}

// TestReadEnvironmentVariables verifies that all environment variables are
// correctly parsed.
func TestReadEnvironmentVariables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	resetEnvVar := func() {
		// Reset all environment variables in case any were already set.
		if err := os.Unsetenv("COCKROACH_LINEARIZABLE"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_MAX_OFFSET"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_METRICS_SAMPLE_INTERVAL"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_SCAN_INTERVAL"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_SCAN_MAX_IDLE_TIME"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_TIME_UNTIL_STORE_DEAD"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL"); err != nil {
			t.Fatal(err)
		}
	}
	defer resetEnvVar()

	// Makes sure no values are set when no environment variables are set.
	ctx := NewContext()
	ctxExpected := NewContext()

	resetEnvVar()
	ctx.readEnvironmentVariables()
	if !reflect.DeepEqual(ctx, ctxExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", ctx, ctxExpected)
	}

	// Set all the environment variables to valid values and ensure they are set
	// correctly.
	if err := os.Setenv("COCKROACH_LINEARIZABLE", "true"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.Linearizable = true
	if err := os.Setenv("COCKROACH_MAX_OFFSET", "1s"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.MaxOffset = time.Second
	if err := os.Setenv("COCKROACH_METRICS_SAMPLE_INTERVAL", "1h10m"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.MetricsSampleInterval = time.Hour + time.Minute*10
	if err := os.Setenv("COCKROACH_SCAN_INTERVAL", "48h"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.ScanInterval = time.Hour * 48
	if err := os.Setenv("COCKROACH_SCAN_MAX_IDLE_TIME", "100ns"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.ScanMaxIdleTime = time.Nanosecond * 100
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL", "48h"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.ConsistencyCheckInterval = time.Hour * 48
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE", "true"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.ConsistencyCheckPanicOnFailure = true
	if err := os.Setenv("COCKROACH_TIME_UNTIL_STORE_DEAD", "10ms"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.TimeUntilStoreDead = time.Millisecond * 10
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL", "10ms"); err != nil {
		t.Fatal(err)
	}
	ctxExpected.ConsistencyCheckInterval = time.Millisecond * 10

	envutil.ClearEnvCache()
	ctx.readEnvironmentVariables()
	if !reflect.DeepEqual(ctx, ctxExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", ctx, ctxExpected)
	}

	// Set all the environment variables to invalid values and test that the
	// defaults are still set.
	ctx = NewContext()
	ctxExpected = NewContext()

	if err := os.Setenv("COCKROACH_LINEARIZABLE", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_MAX_OFFSET", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_METRICS_SAMPLE_INTERVAL", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_SCAN_INTERVAL", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_SCAN_MAX_IDLE_TIME", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_TIME_UNTIL_STORE_DEAD", "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL", "abcd"); err != nil {
		t.Fatal(err)
	}

	envutil.ClearEnvCache()
	ctx.readEnvironmentVariables()
	if !reflect.DeepEqual(ctx, ctxExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", ctx, ctxExpected)
	}
}
