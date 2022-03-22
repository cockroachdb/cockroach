// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
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

// TestConnectorSettingOverrides tests Connector's role as a
// settingswatcher.OverridesMonitor.
func TestConnectorSettingOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	s := rpc.NewServer(rpcContext)

	tenantID := roachpb.MakeTenantID(5)
	gossipSubFn := func(req *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer) error {
		return stream.Send(gossipEventForClusterID(rpcContext.ClusterID.Get()))
	}
	eventCh := make(chan *roachpb.TenantSettingsEvent)
	defer close(eventCh)
	settingsFn := func(req *roachpb.TenantSettingsRequest, stream roachpb.Internal_TenantSettingsServer) error {
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
	roachpb.RegisterInternalServer(s, &mockServer{
		gossipSubFn:      gossipSubFn,
		tenantSettingsFn: settingsFn,
	})
	ln, err := netutil.ListenAndServeGRPC(stopper, s, util.TestAddr)
	require.NoError(t, err)

	cfg := kvtenant.ConnectorConfig{
		TenantID:        tenantID,
		AmbientCtx:      log.MakeTestingAmbientContext(stopper.Tracer()),
		RPCContext:      rpcContext,
		RPCRetryOptions: rpcRetryOpts,
	}
	addrs := []string{ln.Addr().String()}
	c := NewConnector(cfg, addrs)

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

	ch := c.RegisterOverridesChannel()
	// We should always get an initial notification.
	waitForSettings(t, ch)

	ev := &roachpb.TenantSettingsEvent{
		Precedence:  1,
		Incremental: false,
		Overrides:   nil,
	}
	eventCh <- ev
	require.NoError(t, <-startedC)

	waitForSettings(t, ch)
	expectSettings(t, c, "foo=default bar=default baz=default")

	st := func(name, val string) roachpb.TenantSetting {
		return roachpb.TenantSetting{
			Name:  name,
			Value: settings.EncodedValue{Value: val},
		}
	}

	// Set some all-tenant overrides.
	ev = &roachpb.TenantSettingsEvent{
		Precedence:  roachpb.AllTenantsOverrides,
		Incremental: true,
		Overrides:   []roachpb.TenantSetting{st("foo", "all"), st("bar", "all")},
	}
	eventCh <- ev
	waitForSettings(t, ch)
	expectSettings(t, c, "foo=all bar=all baz=default")

	// Set some tenant-specific overrides, with all-tenant overlap.
	ev = &roachpb.TenantSettingsEvent{
		Precedence:  roachpb.SpecificTenantOverrides,
		Incremental: true,
		Overrides:   []roachpb.TenantSetting{st("foo", "specific"), st("baz", "specific")},
	}
	eventCh <- ev
	waitForSettings(t, ch)
	expectSettings(t, c, "foo=specific bar=all baz=specific")

	// Remove an all-tenant override that has a specific override.
	ev = &roachpb.TenantSettingsEvent{
		Precedence:  roachpb.AllTenantsOverrides,
		Incremental: true,
		Overrides:   []roachpb.TenantSetting{st("foo", "")},
	}
	eventCh <- ev
	waitForSettings(t, ch)
	expectSettings(t, c, "foo=specific bar=all baz=specific")

	// Remove a specific override.
	ev = &roachpb.TenantSettingsEvent{
		Precedence:  roachpb.SpecificTenantOverrides,
		Incremental: true,
		Overrides:   []roachpb.TenantSetting{st("foo", "")},
	}
	eventCh <- ev
	waitForSettings(t, ch)
	expectSettings(t, c, "foo=default bar=all baz=specific")

	// Non-incremental change to all-tenants override.
	ev = &roachpb.TenantSettingsEvent{
		Precedence:  roachpb.AllTenantsOverrides,
		Incremental: true,
		Overrides:   []roachpb.TenantSetting{st("bar", "all")},
	}
	eventCh <- ev
	waitForSettings(t, ch)
	expectSettings(t, c, "foo=default bar=all baz=specific")
}

func waitForSettings(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(10 * time.Second):
		t.Fatalf("waitForSettings timed out")
	}
}
func expectSettings(t *testing.T, c *Connector, exp string) {
	t.Helper()
	vars := []string{"foo", "bar", "baz"}
	values := make(map[string]string)
	for i := range vars {
		values[vars[i]] = "default"
	}
	overrides := c.Overrides()
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
}
