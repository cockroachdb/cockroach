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
	"time"

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
	list := emptyList()
	list.entries[entry.Entity] = &entry
	return list
}

func TestDenyListWatcher(t *testing.T) {
	t.Run("Connection is allowed", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.1.1", Type: IPAddrType}, "should match nothing")
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(list, c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo",
		}

		remove, err := watcher.ListenForDenied(connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		remove()
	})

	t.Run("Connection is already denied for ip", func(t *testing.T) {
		list := denyList(DenyEntity{Item: "1.1.2.2", Type: IPAddrType}, "rejected for ip")
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(list, c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo",
		}

		remove, err := watcher.ListenForDenied(connection, noError(t))
		require.EqualError(t, err, "connection ip '1.1.2.2' denied: rejected for ip")
		require.Nil(t, remove)
	})

	t.Run("Connection is denied by update to the deny list", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}

		errorChan := make(chan error, 1)

		remove, err := watcher.ListenForDenied(connection, func(e error) { errorChan <- e })
		require.Nil(t, err)
		require.NotNil(t, remove)

		select {
		case err := <-errorChan:
			require.Fail(t, "Did not expect to find an error in the initial config: %v", err)
		default:
			// continue
		}

		c <- denyList(DenyEntity{Item: "foo-cluster", Type: ClusterType}, "denied due to cluster")

		require.EqualError(t, <-errorChan, "connection cluster 'foo-cluster' denied: denied due to cluster")
	})

	t.Run("Unregister removes listeners", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}

		remove, err := watcher.ListenForDenied(connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("Nil watcher allows connections", func(t *testing.T) {
		watcher := NilWatcher()
		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}
		remove, err := watcher.ListenForDenied(connection, noError(t))
		require.Nil(t, err)
		require.NotNil(t, remove)
		require.Equal(t, watcher.listeners.Len(), 1)

		remove()
		require.Equal(t, watcher.listeners.Len(), 0)
	})

	t.Run("Callback only fires once", func(t *testing.T) {
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(emptyList(), c)

		connection := ConnectionTags{
			IP:      "1.1.2.2",
			Cluster: "foo-cluster",
		}

		runCount := 0
		remove, err := watcher.ListenForDenied(connection, func(err error) {
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
		c := make(chan *Denylist)
		defer close(c)
		watcher := newWatcher(emptyList(), c)
		remove, err := watcher.ListenForDenied(ConnectionTags{}, noError(t))
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
