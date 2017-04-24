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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestParseInitNodeAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := MakeConfig()
	cfg.Attrs = "attr1=val1::attr2=val2"
	cfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{{InMemory: true, SizeInBytes: base.MinimumStoreSize * 100}}}
	engines, err := cfg.CreateEngines()
	if err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	defer engines.Close()
	if err := cfg.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}

	if a, e := cfg.NodeAttributes.Attrs, []string{"attr1=val1", "attr2=val2"}; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected attributes: %v, found: %v", e, a)
	}
}

// TestParseJoinUsingAddrs verifies that JoinList is parsed
// correctly.
func TestParseJoinUsingAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := MakeConfig()
	cfg.JoinList = []string{"localhost:12345,,localhost:23456", "localhost:34567"}
	cfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{{InMemory: true, SizeInBytes: base.MinimumStoreSize * 100}}}
	engines, err := cfg.CreateEngines()
	if err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	defer engines.Close()
	if err := cfg.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}
	r1, err := resolver.NewResolver("localhost:12345")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := resolver.NewResolver("localhost:23456")
	if err != nil {
		t.Fatal(err)
	}
	r3, err := resolver.NewResolver("localhost:34567")
	if err != nil {
		t.Fatal(err)
	}
	expected := []resolver.Resolver{r1, r2, r3}
	if !reflect.DeepEqual(cfg.GossipBootstrapResolvers, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", cfg.GossipBootstrapResolvers, expected)
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
		envutil.ClearEnvCache()
	}
	defer resetEnvVar()

	// Makes sure no values are set when no environment variables are set.
	cfg := MakeConfig()
	cfgExpected := MakeConfig()

	resetEnvVar()
	cfg.readEnvironmentVariables()
	if !reflect.DeepEqual(cfg, cfgExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", cfg, cfgExpected)
	}

	// Set all the environment variables to valid values and ensure they are set
	// correctly.
	if err := os.Setenv("COCKROACH_LINEARIZABLE", "true"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.Linearizable = true
	if err := os.Setenv("COCKROACH_MAX_OFFSET", "1s"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.MaxOffset = time.Second
	if err := os.Setenv("COCKROACH_METRICS_SAMPLE_INTERVAL", "1h10m"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.MetricsSampleInterval = time.Hour + time.Minute*10
	if err := os.Setenv("COCKROACH_SCAN_INTERVAL", "48h"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ScanInterval = time.Hour * 48
	if err := os.Setenv("COCKROACH_SCAN_MAX_IDLE_TIME", "100ns"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ScanMaxIdleTime = time.Nanosecond * 100
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL", "48h"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ConsistencyCheckInterval = time.Hour * 48
	if err := os.Setenv("COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE", "true"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ConsistencyCheckPanicOnFailure = true
	if err := os.Setenv("COCKROACH_TIME_UNTIL_STORE_DEAD", "10ms"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.TimeUntilStoreDead = time.Millisecond * 10

	envutil.ClearEnvCache()
	cfg.readEnvironmentVariables()
	if !reflect.DeepEqual(cfg, cfgExpected) {
		t.Fatalf("actual context does not match expected:\nactual:%+v\nexpected:%+v", cfg, cfgExpected)
	}

	for _, envVar := range []string{
		"COCKROACH_LINEARIZABLE",
		"COCKROACH_MAX_OFFSET",
		"COCKROACH_METRICS_SAMPLE_INTERVAL",
		"COCKROACH_SCAN_INTERVAL",
		"COCKROACH_SCAN_MAX_IDLE_TIME",
		"COCKROACH_CONSISTENCY_CHECK_INTERVAL",
		"COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE",
		"COCKROACH_TIME_UNTIL_STORE_DEAD",
	} {
		t.Run("invalid", func(t *testing.T) {
			if err := os.Setenv(envVar, "abcd"); err != nil {
				t.Fatal(err)
			}
			envutil.ClearEnvCache()

			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()

			cfg.readEnvironmentVariables()
		})
	}
}

func TestFilterGossipBootstrapResolvers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	resolverSpecs := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"localhost:9004",
	}

	resolvers := []resolver.Resolver{}
	for _, rs := range resolverSpecs {
		resolver, err := resolver.NewResolver(rs)
		if err == nil {
			resolvers = append(resolvers, resolver)
		}
	}
	cfg := MakeConfig()
	cfg.GossipBootstrapResolvers = resolvers

	listenAddr := util.MakeUnresolvedAddr("tcp", resolverSpecs[0])
	advertAddr := util.MakeUnresolvedAddr("tcp", resolverSpecs[2])
	filtered := cfg.FilterGossipBootstrapResolvers(context.Background(), &listenAddr, &advertAddr)
	if len(filtered) != 1 {
		t.Fatalf("expected one resolver; got %+v", filtered)
	} else if filtered[0].Addr() != resolverSpecs[1] {
		t.Fatalf("expected resolver to be %q; got %q", resolverSpecs[1], filtered[0].Addr())
	}
}
