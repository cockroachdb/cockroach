// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestParseInitNodeAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	cfg.Attrs = "attr1=val1::attr2=val2"
	cfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{{InMemory: true, Size: base.SizeSpec{InBytes: base.MinimumStoreSize * 100}}}}
	engines, err := cfg.CreateEngines(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	defer engines.Close()
	if err := cfg.InitNode(context.Background()); err != nil {
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
	defer log.Scope(t).Close(t)
	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	cfg.JoinList = []string{"localhost:12345", "localhost:23456", "localhost:34567", "localhost"}
	cfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{{InMemory: true, Size: base.SizeSpec{InBytes: base.MinimumStoreSize * 100}}}}
	engines, err := cfg.CreateEngines(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	defer engines.Close()
	if err := cfg.InitNode(context.Background()); err != nil {
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
	r4, err := resolver.NewResolver("localhost:26257")
	if err != nil {
		t.Fatal(err)
	}
	expected := []resolver.Resolver{r1, r2, r3, r4}
	if !reflect.DeepEqual(cfg.GossipBootstrapResolvers, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", cfg.GossipBootstrapResolvers, expected)
	}
}

// TestReadEnvironmentVariables verifies that all environment variables are
// correctly parsed.
func TestReadEnvironmentVariables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	resetEnvVar := func() {
		// Reset all environment variables in case any were already set.
		if err := os.Unsetenv("COCKROACH_EXPERIMENTAL_LINEARIZABLE"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_SCAN_INTERVAL"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_SCAN_MIN_IDLE_TIME"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_SCAN_MAX_IDLE_TIME"); err != nil {
			t.Fatal(err)
		}
		if err := os.Unsetenv("COCKROACH_CONSISTENCY_CHECK_INTERVAL"); err != nil {
			t.Fatal(err)
		}
		envutil.ClearEnvCache()
	}
	defer resetEnvVar()

	st := cluster.MakeTestingClusterSettings()
	// Makes sure no values are set when no environment variables are set.
	cfg := MakeConfig(context.Background(), st)
	cfgExpected := MakeConfig(context.Background(), st)

	resetEnvVar()
	cfg.readEnvironmentVariables()
	require.Equal(t, cfgExpected, cfg)

	// Set all the environment variables to valid values and ensure they are set
	// correctly.
	if err := os.Setenv("COCKROACH_EXPERIMENTAL_LINEARIZABLE", "true"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.Linearizable = true
	if err := os.Setenv("COCKROACH_SCAN_INTERVAL", "48h"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ScanInterval = time.Hour * 48
	if err := os.Setenv("COCKROACH_SCAN_MIN_IDLE_TIME", "1h"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ScanMinIdleTime = time.Hour
	if err := os.Setenv("COCKROACH_SCAN_MAX_IDLE_TIME", "100ns"); err != nil {
		t.Fatal(err)
	}
	cfgExpected.ScanMaxIdleTime = time.Nanosecond * 100

	envutil.ClearEnvCache()
	cfg.readEnvironmentVariables()
	if !reflect.DeepEqual(cfg, cfgExpected) {
		t.Fatalf("actual context does not match expected: diff(actual,expected) = %s", pretty.Diff(cfgExpected, cfg))
	}

	for _, envVar := range []string{
		"COCKROACH_EXPERIMENTAL_LINEARIZABLE",
		"COCKROACH_SCAN_INTERVAL",
		"COCKROACH_SCAN_MIN_IDLE_TIME",
		"COCKROACH_SCAN_MAX_IDLE_TIME",
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
	defer log.Scope(t).Close(t)

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
	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	cfg.GossipBootstrapResolvers = resolvers
	cfg.Addr = resolverSpecs[0]
	cfg.AdvertiseAddr = resolverSpecs[2]

	filtered := cfg.FilterGossipBootstrapResolvers(context.Background())
	if len(filtered) != 1 {
		t.Fatalf("expected one resolver; got %+v", filtered)
	} else if filtered[0].Addr() != resolverSpecs[1] {
		t.Fatalf("expected resolver to be %q; got %q", resolverSpecs[1], filtered[0].Addr())
	}
}

func TestParseBootstrapResolvers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	const expectedName = "hello"

	t.Run("nosrv", func(t *testing.T) {
		// Ensure that a name in the join list becomes a resolver for that name,
		// when SRV lookups are disabled.
		cfg.JoinPreferSRVRecords = false
		cfg.JoinList = append(base.JoinListType(nil), expectedName)

		resolvers, err := cfg.parseGossipBootstrapResolvers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(resolvers) != 1 {
			t.Fatalf("expected 1 resolver, got %# v", pretty.Formatter(resolvers))
		}
		host, port, err := netutil.SplitHostPort(resolvers[0].Addr(), "UNKNOWN")
		if err != nil {
			t.Fatal(err)
		}
		if port == "UNKNOWN" {
			t.Fatalf("expected port defined in resover: %# v", pretty.Formatter(resolvers))
		}
		if host != expectedName {
			t.Errorf("expected name %q, got %q", expectedName, host)
		}
	})

	t.Run("srv", func(t *testing.T) {
		cfg.JoinPreferSRVRecords = true
		cfg.JoinList = append(base.JoinListType(nil), "othername")

		defer resolver.TestingOverrideSRVLookupFn(func(service, proto, name string) (string, []*net.SRV, error) {
			return "cluster", []*net.SRV{{Target: expectedName, Port: 111}}, nil
		})()

		resolvers, err := cfg.parseGossipBootstrapResolvers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(resolvers) != 1 {
			t.Fatalf("expected 1 resolver, got %# v", pretty.Formatter(resolvers))
		}
		host, port, err := netutil.SplitHostPort(resolvers[0].Addr(), "UNKNOWN")
		if err != nil {
			t.Fatal(err)
		}
		if port == "UNKNOWN" {
			t.Fatalf("expected port defined in resover: %# v", pretty.Formatter(resolvers))
		}
		if port != "111" {
			t.Fatalf("expected port 111 from SRV, got %q", port)
		}
		if host != expectedName {
			t.Errorf("expected name %q, got %q", expectedName, host)
		}
	})
}
