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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
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

	hostname, err := os.Hostname()
	require.NoError(t, err)

	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	cfg.JoinList = []string{"localhost:12345", "[::1]:23456", "f00f::1234", ":34567", ":0", ":", "", "localhost"}
	cfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{{InMemory: true, Size: base.SizeSpec{InBytes: base.MinimumStoreSize * 100}}}}
	engines, err := cfg.CreateEngines(context.Background())
	if err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	defer engines.Close()
	if err := cfg.InitNode(context.Background()); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}
	expected := []util.UnresolvedAddr{
		util.MakeUnresolvedAddr("tcp", "localhost:12345"),
		util.MakeUnresolvedAddr("tcp", "[::1]:23456"),
		util.MakeUnresolvedAddr("tcp", "[f00f::1234]:26257"),
		util.MakeUnresolvedAddr("tcp", hostname+":34567"),
		util.MakeUnresolvedAddr("tcp", hostname+":0"),
		util.MakeUnresolvedAddr("tcp", hostname+":26257"),
		util.MakeUnresolvedAddr("tcp", "localhost:26257"),
	}
	if !reflect.DeepEqual(cfg.GossipBootstrapAddresses, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", cfg.GossipBootstrapAddresses, expected)
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

	// Tracers store their stack trace in NewTracer, and this wouldn't match.
	cfg.Tracer = nil
	cfg.AmbientCtx.Tracer = nil
	cfgExpected.Tracer = nil
	cfgExpected.AmbientCtx.Tracer = nil
	cfg.ExternalStorageAccessor = nil
	cfgExpected.ExternalStorageAccessor = nil
	// Temp storage disk monitors will have slightly different names, so we
	// override them to point to the same one.
	cfgExpected.TempStorageConfig.Mon = cfg.TempStorageConfig.Mon
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

func TestFilterGossipBootstrapAddresses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	addresses := []util.UnresolvedAddr{
		util.MakeUnresolvedAddr("tcp", "127.0.0.1:9000"),
		util.MakeUnresolvedAddr("tcp", "127.0.0.1:9001"),
		util.MakeUnresolvedAddr("tcp", "localhost:9004"),
	}
	cfg := MakeConfig(context.Background(), cluster.MakeTestingClusterSettings())
	cfg.GossipBootstrapAddresses = addresses
	cfg.Addr = addresses[0].String()
	cfg.AdvertiseAddr = addresses[2].String()

	filtered := cfg.FilterGossipBootstrapAddresses(context.Background())
	if len(filtered) != 1 {
		t.Fatalf("expected one address; got %+v", filtered)
	} else if filtered[0].String() != addresses[1].String() {
		t.Fatalf("expected address to be %q; got %q", addresses[1], filtered[0])
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

		addresses, err := cfg.parseGossipBootstrapAddresses(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(addresses) != 1 {
			t.Fatalf("expected 1 address, got %# v", pretty.Formatter(addresses))
		}
		host, port, err := addr.SplitHostPort(addresses[0].String(), "UNKNOWN")
		if err != nil {
			t.Fatal(err)
		}
		if port == "UNKNOWN" {
			t.Fatalf("expected port defined in resover: %# v", pretty.Formatter(addresses))
		}
		if host != expectedName {
			t.Errorf("expected name %q, got %q", expectedName, host)
		}
	})

	t.Run("srv", func(t *testing.T) {
		cfg.JoinPreferSRVRecords = true
		cfg.JoinList = append(base.JoinListType(nil), "othername")

		defer netutil.TestingOverrideSRVLookupFn(func(service, proto, name string) (string, []*net.SRV, error) {
			return "cluster", []*net.SRV{{Target: expectedName, Port: 111}}, nil
		})()

		addresses, err := cfg.parseGossipBootstrapAddresses(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(addresses) != 1 {
			t.Fatalf("expected 1 address, got %# v", pretty.Formatter(addresses))
		}
		host, port, err := addr.SplitHostPort(addresses[0].String(), "UNKNOWN")
		if err != nil {
			t.Fatal(err)
		}
		if port == "UNKNOWN" {
			t.Fatalf("expected port defined in resover: %# v", pretty.Formatter(addresses))
		}
		if port != "111" {
			t.Fatalf("expected port 111 from SRV, got %q", port)
		}
		if host != expectedName {
			t.Errorf("expected name %q, got %q", expectedName, host)
		}
	})
}

func TestIdProviderServerIdentityString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type fields struct {
		clusterID  *base.ClusterIDContainer
		clusterStr atomic.Value
		tenantID   roachpb.TenantID
		tenantStr  atomic.Value
		serverID   *base.NodeIDContainer
		serverStr  atomic.Value
	}
	type args struct {
		key serverident.ServerIdentificationKey
	}

	nodeID := &base.NodeIDContainer{}
	nodeID.Set(context.Background(), roachpb.NodeID(123))

	tenID, err := roachpb.MakeTenantID(2)
	require.NoError(t, err)

	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			"system tenant shows nodeID",
			fields{tenantID: roachpb.SystemTenantID, serverID: nodeID},
			args{key: serverident.IdentifyKVNodeID},
			"123",
		},
		{
			"system tenant shows tenID",
			fields{tenantID: roachpb.SystemTenantID, serverID: nodeID},
			args{key: serverident.IdentifyTenantID},
			"1",
		},
		{
			"application tenant hides nodeID",
			fields{tenantID: tenID, serverID: nodeID},
			args{key: serverident.IdentifyKVNodeID},
			"",
		},
		{
			"application tenant shows tenID",
			fields{tenantID: tenID, serverID: nodeID},
			args{key: serverident.IdentifyTenantID},
			"2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idProvider{
				clusterID:  tt.fields.clusterID,
				clusterStr: tt.fields.clusterStr,
				tenantID:   tt.fields.tenantID,
				tenantStr:  tt.fields.tenantStr,
				serverID:   tt.fields.serverID,
				serverStr:  tt.fields.serverStr,
			}
			assert.Equalf(t, tt.want, s.ServerIdentityString(tt.args.key), "ServerIdentityString(%v)", tt.args.key)
		})
	}
}
