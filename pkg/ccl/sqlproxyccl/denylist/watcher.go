// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Watcher maintains a list of connections waiting for changes to the
// denylist. If a connection is added to the denylist, the watcher notifies
// the connection via the Listener.Denied channel.
type Watcher struct {
	// locking mu is required to access any fields within Watcher.
	mu syncutil.Mutex

	// Each listener is given a unique id. The id is required to disambiguate
	// connections with identical tags.
	nextID int64

	// All of the listeners waiting for changes to the denylist.
	listeners map[int64]*Listener

	// The current value of the denylist.
	list *Denylist
}

// Listener contains the channel notified when a connection is denied
// and contains state needed to remove the listener from the watcher.
type Listener struct {
	// The Denied channel is notified iff the connection was added to
	// the denylist.
	Denied chan error

	// id is used internally by Watcher.
	id int64

	// Used to identify if the connection matches the deny list.
	connection ConnectionTags
}

// ConnectionTags contains connection properties to match against the denylist.
type ConnectionTags struct {
	IP      string
	Cluster string
}

// NewWatcher create a Watcher for watching changes to the Denylist.
// Internally NewWatcher spins up a goroutine that notifies listeners
// when they are added to the Denylist. The goroutine exits when the
// input channel is closed.
func NewWatcher(list *Denylist, next chan *Denylist) *Watcher {
	w := &Watcher{
		listeners: make(map[int64]*Listener),
		list:      list,
	}

	go func() {
		for n := range next {
			w.updateDenyList(n)
		}
	}()

	return w
}

// NilWatcher produces a watcher that allows every connection. It is intended
// for use when the denylist is disabled.
func NilWatcher() *Watcher {
	c := make(chan *Denylist)
	close(c)
	return NewWatcher(emptyList(), c)
}

// ListenForDenied Adds a listener to the watcher for the given connection. If the
// connection is already on the denylist a nil listener is returned and an error
// is returned immediately.
//
// The listener must be cleaned up via a call to watcher.RemoveListener();
func (w *Watcher) ListenForDenied(connection ConnectionTags) (*Listener, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := checkConnection(connection, w.list); err != nil {
		return nil, err
	}

	id := w.nextID
	w.nextID++

	listener := &Listener{
		id: id,
		// The channel is buffered so that the watcher does not need to block when inserting
		// the error.
		Denied:     make(chan error, 1),
		connection: connection,
	}
	w.listeners[id] = listener

	return listener, nil
}

// RemoveListener removes a listener from the watcher. If RemoveListener is not called,
// the listener instance will leak.
func (w *Watcher) RemoveListener(listener *Listener) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.listeners, listener.id)
}

func (w *Watcher) updateDenyList(list *Denylist) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.list = list

	for _, listener := range w.listeners {
		if err := checkConnection(listener.connection, w.list); err != nil {
			select {
			case listener.Denied <- err:
				// error inserted into channel.
			default:
				// there is already a denied error in the channel.
			}
		}
	}
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
