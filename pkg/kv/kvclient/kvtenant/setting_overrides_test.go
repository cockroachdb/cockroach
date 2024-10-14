// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func newTestConnector(
	t *testing.T, ctx context.Context,
) (*connector, func(), <-chan error, chan<- *kvpb.TenantSettingsEvent) {
	stopper := stop.NewStopper()
	cleanup := func() { stopper.Stop(ctx) }
	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)

	tenantID := roachpb.MustMakeTenantID(5)
	gossipSubFn := func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error {
		return stream.Send(gossipEventForClusterID(rpcContext.StorageClusterID.Get()))
	}
	eventCh := make(chan *kvpb.TenantSettingsEvent, 2)
	prevCleanup := cleanup
	cleanup = func() { close(eventCh); prevCleanup() }
	settingsFn := func(req *kvpb.TenantSettingsRequest, stream kvpb.Internal_TenantSettingsServer) error {
		if req.TenantID != tenantID {
			t.Errorf("invalid tenantID %s (expected %s)", req.TenantID, tenantID)
		}
		for event := range eventCh {
			if err := stream.Send(event); err != nil {
				return err
			}
		}
		return nil
	}
	kvpb.RegisterInternalServer(s, &mockServer{
		gossipSubFn:      gossipSubFn,
		tenantSettingsFn: settingsFn,
	})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	cfg := ConnectorConfig{
		TenantID:        tenantID,
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := newConnector(cfg, addrs)

	// Start should block until the first TenantSettings response.
	startedC := make(chan error)
	go func() {
		startedC <- c.Start(ctx)
	}()
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	return c, cleanup, startedC, eventCh
}

// TestConnectorSettingOverrides tests connector's role as a
// settingswatcher.OverridesMonitor.
func TestConnectorSettingOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	c, cleanup, startedC, eventCh := newTestConnector(t, ctx)
	defer cleanup()

	ev := &kvpb.TenantSettingsEvent{
		EventType:   kvpb.TenantSettingsEvent_SETTING_EVENT,
		Precedence:  kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES,
		Incremental: false,
		Overrides:   nil,
	}
	eventCh <- ev

	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	ev = &kvpb.TenantSettingsEvent{
		EventType:   kvpb.TenantSettingsEvent_SETTING_EVENT,
		Precedence:  kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
		Incremental: false,
		Overrides:   nil,
	}
	eventCh <- ev
	select {
	case err := <-startedC:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to see start complete")
	}

	ch := expectSettings(t, c, "foo=default bar=default baz=default")

	st := func(key settings.InternalKey, val string) kvpb.TenantSetting {
		return kvpb.TenantSetting{
			InternalKey: key,
			Value:       settings.EncodedValue{Value: val},
		}
	}

	// Set some all-tenant overrides.
	ev = &kvpb.TenantSettingsEvent{
		Precedence:  kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
		Incremental: true,
		Overrides:   []kvpb.TenantSetting{st("foo", "all"), st("bar", "all")},
	}
	eventCh <- ev
	waitForNotify(t, ch)
	ch = expectSettings(t, c, "foo=all bar=all baz=default")

	// Set some tenant-specific overrides, with all-tenant overlap.
	ev = &kvpb.TenantSettingsEvent{
		Precedence:  kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES,
		Incremental: true,
		Overrides:   []kvpb.TenantSetting{st("foo", "specific"), st("baz", "specific")},
	}
	eventCh <- ev
	waitForNotify(t, ch)
	ch = expectSettings(t, c, "foo=specific bar=all baz=specific")

	// Remove an all-tenant override that has a specific override.
	ev = &kvpb.TenantSettingsEvent{
		Precedence:  kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
		Incremental: true,
		Overrides:   []kvpb.TenantSetting{st("foo", "")},
	}
	eventCh <- ev
	waitForNotify(t, ch)
	ch = expectSettings(t, c, "foo=specific bar=all baz=specific")

	// Remove a specific override.
	ev = &kvpb.TenantSettingsEvent{
		Precedence:  kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES,
		Incremental: true,
		Overrides:   []kvpb.TenantSetting{st("foo", "")},
	}
	eventCh <- ev
	waitForNotify(t, ch)
	ch = expectSettings(t, c, "foo=default bar=all baz=specific")

	// Non-incremental change to all-tenants override.
	ev = &kvpb.TenantSettingsEvent{
		Precedence:  kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
		Incremental: true,
		Overrides:   []kvpb.TenantSetting{st("bar", "all")},
	}
	eventCh <- ev
	waitForNotify(t, ch)
	_ = expectSettings(t, c, "foo=default bar=all baz=specific")
}

func waitForNotify(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(10 * time.Second):
		t.Fatalf("waitForNotify timed out")
	}
}

func expectSettings(t *testing.T, c *connector, exp string) <-chan struct{} {
	t.Helper()
	vars := []settings.InternalKey{"foo", "bar", "baz"}
	values := make(map[settings.InternalKey]string)
	for i := range vars {
		values[vars[i]] = "default"
	}
	overrides, updateCh := c.Overrides()
	for _, v := range vars {
		if val, ok := overrides[v]; ok {
			values[v] = val.Value
		}
	}
	var strs []string
	for _, v := range vars {
		strs = append(strs, fmt.Sprintf("%s=%s", v, values[v]))
	}
	str := strings.Join(strs, " ")
	if str != exp {
		t.Errorf("expected:  %s  got:  %s", exp, str)
	}

	return updateCh
}

// TestConnectorTenantMetadata tests the connector's role as a
// tenant metadata provider.
func TestConnectorTenantMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	c, cleanup, startedC, eventCh := newTestConnector(t, ctx)
	defer cleanup()

	// First message - required by startup protocol.
	const firstPrecedence = kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES
	ev := &kvpb.TenantSettingsEvent{
		EventType:  kvpb.TenantSettingsEvent_SETTING_EVENT,
		Precedence: firstPrecedence,
	}
	eventCh <- ev
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Initial tenant metadata.
	ev = &kvpb.TenantSettingsEvent{
		EventType:   kvpb.TenantSettingsEvent_METADATA_EVENT,
		Precedence:  firstPrecedence,
		Incremental: true,
		Name:        "initial",
		// TODO(knz): remove cast after the dep cycle has been resolved.
		DataState:    uint32(mtinfopb.DataStateReady),
		ServiceMode:  uint32(mtinfopb.ServiceModeExternal),
		Capabilities: &tenantcapabilitiespb.TenantCapabilities{CanViewNodeInfo: true},
	}
	eventCh <- ev
	select {
	case err := <-startedC:
		t.Fatalf("Start unexpectedly completed with err=%v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Finish startup.
	ev = &kvpb.TenantSettingsEvent{
		EventType:  kvpb.TenantSettingsEvent_SETTING_EVENT,
		Precedence: kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
	}
	eventCh <- ev
	select {
	case err := <-startedC:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to see start complete")
	}

	ch := expectMetadata(t, c, `tid=5 name="initial" data=ready service=external caps=can_view_node_info:true `)

	// Change some metadata fields.
	const anyPrecedence = kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES
	ev = &kvpb.TenantSettingsEvent{
		EventType:   kvpb.TenantSettingsEvent_METADATA_EVENT,
		Precedence:  anyPrecedence,
		Incremental: true,
		Name:        "initial",
		// TODO(knz): remove cast after the dep cycle has been resolved.
		DataState:    uint32(mtinfopb.DataStateDrop),
		ServiceMode:  uint32(mtinfopb.ServiceModeShared),
		Capabilities: &tenantcapabilitiespb.TenantCapabilities{ExemptFromRateLimiting: true},
	}
	eventCh <- ev
	waitForNotify(t, ch)

	_ = expectMetadata(t, c, `tid=5 name="initial" data=dropping service=shared caps=exempt_from_rate_limiting:true `)
}

func expectMetadata(t *testing.T, c *connector, exp string) <-chan struct{} {
	t.Helper()
	info, updateCh := c.TenantInfo()
	str := fmt.Sprintf("tid=%v name=%q data=%v service=%v caps=%+v",
		info.TenantID, info.Name, info.DataState, info.ServiceMode, info.TenantCapabilities)
	if str != exp {
		t.Errorf("\nexpected: %q\ngot     : %q", exp, str)
	}

	return updateCh
}

// TestCrossVersionMetadataSupport tests that and old-version
// connector can talk to a new-version server and a new-version server
// can talk to an old-version connector.
func TestCrossVersionMetadataSupport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, b := range []bool{false, true} {
		oldVersionClient := b
		oldVersionServer := !b
		strs := map[bool]string{false: "new", true: "old"}
		t.Run(fmt.Sprintf("client=%s/server=%s", strs[oldVersionClient], strs[oldVersionServer]), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			clock := hlc.NewClockForTesting(nil)
			rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
			s, err := rpc.NewServer(ctx, rpcContext)
			require.NoError(t, err)

			gossipSubFn := func(req *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer) error {
				return stream.Send(gossipEventForClusterID(rpcContext.StorageClusterID.Get()))
			}
			server := &mockServer{
				gossipSubFn:                    gossipSubFn,
				emulateOldVersionSettingServer: oldVersionServer,
			}
			kvpb.RegisterInternalServer(s, server)
			ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
			require.NoError(t, err)

			cfg := ConnectorConfig{
				TenantID:        roachpb.MustMakeTenantID(5),
				AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
				RPCContext:      rpcContext,
				RPCRetryOptions: rpcRetryOpts,
			}
			addrs := []string{ln.Addr().String()}
			c := newConnector(cfg, addrs)
			c.testingEmulateOldVersionSettingsClient = oldVersionClient

			// Start the connector.
			startedC := make(chan error)
			go func() {
				startedC <- c.Start(ctx)
			}()
			select {
			case err := <-startedC:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatalf("failed to see start complete")
			}

			// In any case check that the overrides are available.
			func() {
				t.Helper()
				c.settingsMu.Lock()
				defer c.settingsMu.Unlock()

				require.True(t, c.settingsMu.receivedFirstAllTenantOverrides)
				require.True(t, c.settingsMu.receivedFirstSpecificOverrides)
			}()

			// If either the client or the server is new, the metadata is
			// not communicated or not processed.
			receivedFirstMetadata := func() bool {
				c.metadataMu.Lock()
				defer c.metadataMu.Unlock()
				return c.metadataMu.receivedFirstMetadata
			}()
			require.False(t, receivedFirstMetadata)
			expectMetadata(t, c, `tid=5 name="" data=add service=none caps=<nil>`)
		})
	}
}
