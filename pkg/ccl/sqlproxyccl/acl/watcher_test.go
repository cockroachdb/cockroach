// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acl

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func noError(t *testing.T) func(error) {
	return func(e error) {
		t.Fatalf("unexpected callback: %v", e)
	}
}

func denyList(entity DenyEntity, reason string) *Denylist {
	entry := DenyEntry{
		Entity: entity,
		Reason: reason,
	}
	list := &Denylist{
		entries: make(map[DenyEntity]*DenyEntry),
	}
	list.entries[entry.Entity] = &entry
	return list
}

func allowList(cluster string, ips ...string) *Allowlist {
	list := &Allowlist{
		entries: make(map[string]AllowEntry),
	}
	entry := AllowEntry{
		ips: make([]*net.IPNet, 0),
	}
	for _, ip := range ips {
		_, ipNet, _ := net.ParseCIDR(ip)
		entry.ips = append(entry.ips, ipNet)
	}
	list.entries[cluster] = entry
	return list
}

func privateEndpoints(fn lookupTenantFunc) *PrivateEndpoints {
	return &PrivateEndpoints{LookupTenantFn: fn}
}

func cidrRanges(fn lookupTenantFunc) *CIDRRanges {
	return &CIDRRanges{LookupTenantFn: fn}
}

func TestACLWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	dir, tds := tenantdirsvr.SetupTestDirectory(t, ctx, stopper, nil /* timeSource */)
	tenantID := roachpb.MustMakeTenantID(10)

	// Wait until the tenant watcher has been established.
	testutils.SucceedsSoon(t, func() error {
		if tds.WatchTenantsListenersCount() == 0 {
			return errors.New("watchers have not been established yet")
		}
		return nil
	})

	tds.CreateTenant(tenantID, &tenant.Tenant{
		Version:                 "001",
		TenantID:                tenantID.ToUint64(),
		ClusterName:             "my-tenant",
		AllowedCIDRRanges:       []string{"1.1.0.0/16"},
		AllowedPrivateEndpoints: []string{"foo-bar-baz", "cockroachdb"},
	})

	t.Run("connection is allowed", func(t *testing.T) {
		deny := denyList(DenyEntity{Item: "1.1.1.1", Type: IPAddrType}, "should match nothing")
		allow := allowList("10", "1.1.0.0/16")
		pe := privateEndpoints(dir.LookupTenant)
		pcr := cidrRanges(dir.LookupTenant)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, allow, nil)
		watcher.addAccessController(ctx, deny, nil)
		watcher.addAccessController(ctx, pe, nil)
		watcher.addAccessController(ctx, pcr, nil)

		connection := ConnectionTags{
			IP:         "1.1.2.2",
			TenantID:   tenantID,
			EndpointID: "cockroachdb",
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		remove()
	})

	t.Run("connection is already denied for ip by allowlist", func(t *testing.T) {
		allow := allowList("10", "1.1.1.0/24")
		watcher, _ := NewWatcher(ctx)
		watcher.controllers = append(watcher.controllers, allow)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: ip address not allowed")
		require.Nil(t, remove)
	})

	t.Run("connection is already denied for ip by denylist", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "rejected for ip")
		watcher, _ := NewWatcher(ctx)
		watcher.controllers = append(watcher.controllers, list)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: rejected for ip")
		require.Nil(t, remove)
	})

	t.Run("connection is already denied by private endpoints", func(t *testing.T) {
		pe := privateEndpoints(dir.LookupTenant)
		watcher, _ := NewWatcher(ctx)
		watcher.controllers = append(watcher.controllers, pe)

		connection := ConnectionTags{
			IP:         "1.1.2.2",
			TenantID:   tenantID,
			EndpointID: "random-connection",
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "cluster does not allow private connections from endpoint 'random-connection'")
		require.Nil(t, remove)
	})

	t.Run("connection is already denied by public cidr ranges", func(t *testing.T) {
		pcr := cidrRanges(dir.LookupTenant)
		watcher, _ := NewWatcher(ctx)
		watcher.controllers = append(watcher.controllers, pcr)

		connection := ConnectionTags{
			IP:       "127.0.0.1",
			TenantID: tenantID,
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "cluster does not allow public connections from IP 127.0.0.1")
		require.Nil(t, remove)
	})

	t.Run("connection is denied by update to the allow list", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, &Allowlist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- allowList("10", "1.1.1.0/24")

		require.EqualError(t, <-errorChan, "connection ip '1.1.2.2' denied: ip address not allowed")
	})

	t.Run("connection is denied by update to the deny list", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, &Denylist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- denyList(DenyEntity{Item: "10", Type: ClusterType}, "denied due to cluster")

		require.EqualError(t, <-errorChan, "connection cluster '10' denied: denied due to cluster")
	})

	t.Run("connection is denied by update to private endpoints", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		pe := privateEndpoints(dir.LookupTenant)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, pe, c)

		connection := ConnectionTags{
			IP:         "1.1.2.2",
			TenantID:   tenantID,
			EndpointID: "cockroachdb",
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		// Update the tenant.
		tds.UpdateTenant(tenantID, &tenant.Tenant{
			Version:                 "002",
			TenantID:                tenantID.ToUint64(),
			ClusterName:             "my-tenant",
			AllowedCIDRRanges:       []string{"1.1.0.0/16"},
			AllowedPrivateEndpoints: []string{"foo-bar-baz"},
		})

		// Wait until watcher has received the updated event.
		testutils.SucceedsSoon(t, func() error {
			ten, err := dir.LookupTenant(ctx, tenantID)
			if err != nil {
				return err
			}
			if ten.Version != "002" {
				return errors.Newf("tenant is not up-to-date, found version=%s", ten.Version)
			}
			return nil
		})

		// Emit the same item.
		c <- pe

		require.EqualError(t, <-errorChan, "cluster does not allow private connections from endpoint 'cockroachdb'")
	})

	t.Run("connection is denied by update to public cidr ranges", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		pcr := cidrRanges(dir.LookupTenant)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, pcr, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		// Update the tenant.
		tds.UpdateTenant(tenantID, &tenant.Tenant{
			Version:           "003",
			TenantID:          tenantID.ToUint64(),
			ClusterName:       "my-tenant",
			AllowedCIDRRanges: []string{"127.0.0.1/32"},
		})

		// Wait until watcher has received the updated event.
		testutils.SucceedsSoon(t, func() error {
			ten, err := dir.LookupTenant(ctx, tenantID)
			if err != nil {
				return err
			}
			if ten.Version != "003" {
				return errors.Newf("tenant is not up-to-date, found version=%s", ten.Version)
			}
			return nil
		})

		// Emit the same item.
		c <- pcr

		require.EqualError(t, <-errorChan, "cluster does not allow public connections from IP 1.1.2.2")
	})

	t.Run("unregister removes listeners", func(t *testing.T) {
		watcher, _ := NewWatcher(ctx)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("new watcher allows connections", func(t *testing.T) {
		watcher, _ := NewWatcher(ctx)
		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}
		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("callback only fires once", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(ctx)
		watcher.addAccessController(ctx, &Denylist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: tenantID,
		}

		runCount := 0
		remove, err := watcher.ListenForDenied(ctx, connection, func(err error) {
			require.EqualError(t, err, "connection ip '1.1.2.2' denied: list v1")
			runCount++
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer remove()

		c <- denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "list v1")
		c <- denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "list v2")

		time.Sleep(time.Second)
		require.Equal(t, runCount, 1)
	})

	t.Run("remove sets the callback to nil", func(t *testing.T) {
		watcher, _ := NewWatcher(ctx)
		remove, err := watcher.ListenForDenied(ctx, ConnectionTags{}, noError(t))
		require.Nil(t, err)

		var l *listener
		watcher.listeners.Ascend(func(i btree.Item) bool {
			l = i.(*listener)
			return false
		})
		require.NotNil(t, l)
		require.NotNil(t, l.mu.denied)

		remove()

		require.Nil(t, l.mu.denied)
	})
}
