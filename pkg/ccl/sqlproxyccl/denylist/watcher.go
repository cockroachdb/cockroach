// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// Watcher maintains a list of connections waiting for changes to the
// denylist. If a connection is added to the denylist, the watcher notifies
// the connection via the Listener.Denied channel.
//
// All of Watcher's methods are thread safe.
type Watcher struct {
	// locking mu is required to access any fields within Watcher.
	mu syncutil.Mutex

	// Each listener is given a unique id. The id is required to disambiguate
	// connections with identical tags.
	nextID int64

	// All of the listeners waiting for changes to the denylist.
	listeners *btree.BTree

	// The current value of the denylist.
	list *Denylist
}

// Listener contains the channel notified when a connection is denied
// and contains state needed to remove the listener from the watcher.
type listener struct {
	// Lock Ordering: Only lock one listener at a time. Lock the listener before
	// locking the watcher.
	mu struct {
		syncutil.Mutex

		// The denied callback is notified iff the connection was added to
		// the denylist. After the callback is notified once, denied is set
		// to nil to prevent duplicate calls.
		denied func(error)
	}

	// Unique id. The id is required to disambiguate connections with identical
	// connection tags.
	id int64

	// Used to identify if the connection matches the deny list.
	connection ConnectionTags
}

// ConnectionTags contains connection properties to match against the denylist.
type ConnectionTags struct {
	IP      string
	Cluster string
}

// NilWatcher produces a watcher that allows every connection. It is intended
// for use when the denylist is disabled.
func NilWatcher() *Watcher {
	c := make(chan *Denylist)
	close(c)
	return newWatcher(emptyList(), c)
}

// WatcherFromFile produces a watcher that reads the denylist from a file and
// periodically polls the file for changes. If the file is unreadable or contains
// invalid content, the watcher fails open and allows all connections.
func WatcherFromFile(ctx context.Context, filename string, opts ...Option) *Watcher {
	dl, dlc := newDenylistWithFile(ctx, filename, opts...)
	return newWatcher(dl, dlc)
}

// newWatcher create a Watcher for watching changes to the Denylist.
// Internally newWatcher spins up a goroutine that notifies listeners
// when they are added to the Denylist. The goroutine exits when the
// input channel is closed.
func newWatcher(list *Denylist, next chan *Denylist) *Watcher {
	w := &Watcher{
		listeners: btree.New(8),
		list:      list,
	}

	go func() {
		for n := range next {
			w.updateDenyList(n)
		}
	}()

	return w
}

// ListenForDenied Adds a listener to the watcher for the given connection. If the
// connection is already on the denylist a nil remove function is returned and an error
// is returned immediately.
//
// Example Usage:
// remove, err := w.ListenForDenied(connection, func(err error) {
//   /* connection was added to the deny list */
// })
// if err != nil { /*connection already on the denylist*/ }
// defer remove()
//
// Warning:
// Do not call remove() within the error callback. It would deadlock.
func (w *Watcher) ListenForDenied(connection ConnectionTags, callback func(error)) (func(), error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := checkConnection(connection, w.list); err != nil {
		return nil, err
	}

	id := w.nextID
	w.nextID++

	l := &listener{
		id:         id,
		connection: connection,
	}
	l.mu.denied = callback

	w.listeners.ReplaceOrInsert(l)
	return func() { w.removeListener(l) }, nil
}

func (w *Watcher) updateDenyList(list *Denylist) {
	w.mu.Lock()
	copy := w.listeners.Clone()
	w.list = list
	w.mu.Unlock()

	copy.Ascend(func(i btree.Item) bool {
		lst := i.(*listener)
		if err := checkConnection(lst.connection, list); err != nil {
			lst.mu.Lock()
			defer lst.mu.Unlock()
			if lst.mu.denied != nil {
				lst.mu.denied(err)
				lst.mu.denied = nil
			}
		}
		return true
	})
}

func (w *Watcher) removeListener(l *listener) {
	l.mu.Lock()
	defer l.mu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	// remove the callback to prevent it from firing after removeListener returns.
	l.mu.denied = nil

	// remove the connection from the listeners tree to reclaim memory.
	w.listeners.Delete(l)
}

// Less implements the btree.Item interface for listener.
func (l *listener) Less(than btree.Item) bool {
	return l.id < than.(*listener).id
}

func checkConnection(connection ConnectionTags, list *Denylist) error {
	ip := DenyEntity{Item: connection.IP, Type: IPAddrType}
	if err := list.Denied(ip); err != nil {
		return errors.Errorf("connection ip '%v' denied: %v", connection.IP, err)
	}
	cluster := DenyEntity{Item: connection.Cluster, Type: ClusterType}
	if err := list.Denied(cluster); err != nil {
		return errors.Errorf("connection cluster '%v' denied: %v", connection.Cluster, err)
	}
	return nil
}
