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
	r1, err := resolver.NewResolver(&ctx.Context, "tcp=localhost:12345")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := resolver.NewResolver(&ctx.Context, "tcp=localhost:23456")
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
		os.Unsetenv("cockroach-linearizable")
		os.Unsetenv("cockroach-max-offset")
		os.Unsetenv("cockroach-metrics-frequency")
		os.Unsetenv("cockroach-scan-interval")
		os.Unsetenv("cockroach-scan-max-idle-time")
		os.Unsetenv("cockroach-time-until-store-dead")
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
	os.Setenv("cockroach-linearizable", "true")
	ctxExpected.Linearizable = true
	os.Setenv("cockroach-max-offset", "1s")
	ctxExpected.MaxOffset = time.Second
	os.Setenv("cockroach-metrics-frequency", "1h10m")
	ctxExpected.MetricsFrequency = time.Hour + time.Minute*10
	os.Setenv("cockroach-scan-interval", "48h")
	ctxExpected.ScanInterval = time.Hour * 48
	os.Setenv("cockroach-scan-max-idle-time", "100ns")
	ctxExpected.ScanMaxIdleTime = time.Nanosecond * 100
	os.Setenv("cockroach-time-until-store-dead", "10ms")
	ctxExpected.TimeUntilStoreDead = time.Millisecond * 10

	ctx.readEnvironmentVariables()
	if !reflect.DeepEqual(ctx, ctxExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", ctx, ctxExpected)
	}

	// Set all the environment variables to invalid values and test that the
	// defaults are still set.
	ctx = NewContext()
	ctxExpected = NewContext()

	os.Setenv("cockroach-linearizable", "abcd")
	os.Setenv("cockroach-max-offset", "abcd")
	os.Setenv("cockroach-metrics-frequency", "abcd")
	os.Setenv("cockroach-scan-interval", "abcd")
	os.Setenv("cockroach-scan-max-idle-time", "abcd")
	os.Setenv("cockroach-time-until-store-dead", "abcd")

	ctx.readEnvironmentVariables()
	if !reflect.DeepEqual(ctx, ctxExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", ctx, ctxExpected)
	}
}
