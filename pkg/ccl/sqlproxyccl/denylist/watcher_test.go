// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func denyList(entity DenyEntity, reason string) *Denylist {
	entry := DenyEntry{
		Entity: entity,
		Reason: reason,
	}
	list := emptyList()
	list.entries[entry.Entity] = &entry
	return list
}

func TestDenyListWatcher(t *testing.T) {
	t.Run("Connection is allowed", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.1.1", Type: IPAddrType}, "should match nothing")
		c := make(chan *Denylist)
		defer close(c)
		watcher := NewWatcher(list, c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo",
		}

		listener, err := watcher.ListenForDenied(connection)
		require.Nil(t, err)
		require.NotNil(t, listener.Denied)
	})

	t.Run("Connection is already denied for ip", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "rejected for ip")
		c := make(chan *Denylist)
		defer close(c)
		watcher := NewWatcher(list, c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo",
		}

		listener, err := watcher.ListenForDenied(connection)
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: rejected for ip")
		require.Nil(t, listener)
	})

	t.Run("Connection is denied by update to the deny list", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := NewWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}

		listener, err := watcher.ListenForDenied(connection)
		require.Nil(t, err)
		require.NotNil(t, listener)
		require.NotNil(t, listener.Denied)

		select {
		case err := <-listener.Denied:
			require.Fail(t, "Did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- denyList(DenyEntity{Item: "foo-cluster", Type: ClusterType}, "denied due to cluster")

		require.EqualError(t, <-listener.Denied, "connection cluster 'foo-cluster' denied: denied due to cluster")
	})

	t.Run("Unregister removes listeners", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := NewWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}

		listener, err := watcher.ListenForDenied(connection)
		require.Nil(t, err)
		require.NotNil(t, listener)
		require.Equal(t, len(watcher.listeners), 1)

		watcher.RemoveListener(listener)
		require.Equal(t, len(watcher.listeners), 0)
	})

	t.Run("Nil watcher allows connections", func(t *testing.T) {
		watcher := NilWatcher()
		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}
		listener, err := watcher.ListenForDenied(connection)
		require.Nil(t, err)
		require.NotNil(t, listener)
		require.Equal(t, len(watcher.listeners), 1)

		watcher.RemoveListener(listener)
		require.Equal(t, len(watcher.listeners), 0)
	})

	t.Run("Ignoring the listener's channel does not deadlock the watcher", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := NewWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}
		listener, err := watcher.ListenForDenied(connection)
		require.NotNil(t, listener)
		require.Nil(t, err)

		c <- denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "list v2")
		c <- denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "list v3")

		watcher.RemoveListener(listener)
		select {
		case err := <-listener.Denied:
			require.EqualError(t, err, "connection ip '1.1.2.2' denied: list v2")
		default:
			require.Fail(t, "The connection was added to the deny list on the second update")
		}
	})
}
