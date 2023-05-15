// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func noError(t *testing.T) func(error) {
	return func(e error) {
		t.Fatalf("Unexpected callback: %v", e)
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

func TestACLWatcher(t *testing.T) {
	ctx := context.Background()
	t.Run("Connection is allowed", func(t *testing.T) {
		deny := denyList(DenyEntity{Item: "1.1.1.1", Type: IPAddrType}, "should match nothing")
		allow := allowList("10", "1.1.0.0/16")
		watcher, _ := NewWatcher(context.Background())
		watcher.addAccessController(ctx, allow, nil)
		watcher.addAccessController(ctx, deny, nil)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		remove()
	})

	t.Run("Connection is already denied for ip by allowlist", func(t *testing.T) {
		allow := allowList("10", "1.1.1.0/24")
		watcher, _ := NewWatcher(context.Background())
		watcher.controllers = append(watcher.controllers, allow)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: ip address not allowed")
		require.Nil(t, remove)
	})

	t.Run("Connection is already denied for ip by denylist", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "rejected for ip")
		watcher, _ := NewWatcher(context.Background())
		watcher.controllers = append(watcher.controllers, list)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: rejected for ip")
		require.Nil(t, remove)
	})

	t.Run("Connection is denied by update to the allow list", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(context.Background())
		watcher.addAccessController(ctx, &Allowlist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "Did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- allowList("10", "1.1.1.0/24")

		require.EqualError(t, <-errorChan, "connection ip '1.1.2.2' denied: ip address not allowed")
	})

	t.Run("Connection is denied by update to the deny list", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(context.Background())
		watcher.addAccessController(ctx, &Denylist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(ctx, connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "Did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- denyList(DenyEntity{Item: "10", Type: ClusterType}, "denied due to cluster")

		require.EqualError(t, <-errorChan, "connection cluster '10' denied: denied due to cluster")
	})

	t.Run("Unregister removes listeners", func(t *testing.T) {
		watcher, _ := NewWatcher(context.Background())

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}

		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("New watcher allows connections", func(t *testing.T) {
		watcher, _ := NewWatcher(context.Background())
		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
		}
		remove, err := watcher.ListenForDenied(ctx, connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("Callback only fires once", func(t *testing.T) {
		c := make(chan AccessController)
		defer close(c)
		watcher, _ := NewWatcher(context.Background())
		watcher.addAccessController(ctx, &Denylist{}, c)

		connection := ConnectionTags{
			IP:       "1.1.2.2",
			TenantID: roachpb.MustMakeTenantID(10),
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

	t.Run("Remove sets the callback to nil", func(t *testing.T) {
		watcher, _ := NewWatcher(context.Background())
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
