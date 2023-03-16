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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/btree"
)

// Watcher maintains a list of connections waiting for changes to the
// access control list. If a connection becomes blocked because of changes
// to the access control list, the watcher notifies the connection via the
// Listener.Denied channel.
//
// All of Watcher's methods are thread safe.
type Watcher struct {
	// locking mu is required to access any fields within Watcher.
	mu syncutil.Mutex

	// Each listener is given a unique id. The id is required to disambiguate
	// connections with identical tags.
	nextID int64

	options *aclOptions

	// All of the listeners waiting for changes to the access control list.
	listeners *btree.BTree

	// These control whether or not a connection is allowd based on it's ConnectionTags.
	controllers []AccessController
}

// Listener contains the channel notified when a connection is denied
// and contains state needed to remove the listener from the watcher.
type listener struct {
	// Lock Ordering: Only lock one listener at a time. Lock the listener before
	// locking the watcher.
	mu struct {
		syncutil.Mutex

		// The denied callback is notified iff the connection was blocked.
		// After the callback is notified once, denied is set
		// to nil to prevent duplicate calls.
		denied func(error)
	}

	// Unique id. The id is required to disambiguate connections with identical
	// connection tags.
	id int64

	// Used to identify if the connection matches the access control list.
	connection ConnectionTags
}

// Option allows configuration of an access control list service.
type Option func(*aclOptions)

type aclOptions struct {
	pollingInterval time.Duration
	timeSource      timeutil.TimeSource
	errorCount      *metric.Gauge
	allowlistFile   string
	denylistFile    string
}

// WithPollingInterval specifies interval between polling for config file
// changes.
func WithPollingInterval(d time.Duration) Option {
	return func(op *aclOptions) {
		op.pollingInterval = d
	}
}

// WithTimeSource overrides the time source used to check expiration times.
func WithTimeSource(t timeutil.TimeSource) Option {
	return func(op *aclOptions) {
		op.timeSource = t
	}
}

func WithErrorCount(errorCount *metric.Gauge) Option {
	return func(op *aclOptions) {
		op.errorCount = errorCount
	}
}

func WithAllowListFile(allowlistFile string) Option {
	return func(op *aclOptions) {
		op.allowlistFile = allowlistFile
	}
}

func WithDenyListFile(denylistFile string) Option {
	return func(op *aclOptions) {
		op.denylistFile = denylistFile
	}
}

func NewWatcher(ctx context.Context, opts ...Option) (*Watcher, error) {
	options := &aclOptions{
		pollingInterval: defaultPollingInterval,
		timeSource:      timeutil.DefaultTimeSource{},
	}
	for _, opt := range opts {
		opt(options)
	}
	w := &Watcher{
		listeners:   btree.New(8),
		options:     options,
		controllers: make([]AccessController, 0),
	}

	if options.allowlistFile != "" {
		c, next, err := newAccessControllerFromFile[*Allowlist](ctx, options.allowlistFile, w.options.pollingInterval, w.options.errorCount)
		if err != nil {
			return nil, err
		}
		w.addAccessController(c, next)
	}
	if options.denylistFile != "" {
		c, next, err := newAccessControllerFromFile[*Denylist](ctx, options.denylistFile, w.options.pollingInterval, w.options.errorCount)
		if err != nil {
			return nil, err
		}
		w.addAccessController(c, next)
	}
	return w, nil
}

// addAccessController adds a new access controller to the watcher, and spawns a goroutine that watches for updates and
// replaces the controller as needed, using it's index in the slice.
func (w *Watcher) addAccessController(controller AccessController, next chan AccessController) {
	w.mu.Lock()
	index := len(w.controllers)
	w.controllers = append(w.controllers, controller)
	w.mu.Unlock()

	if next != nil {
		go func() {
			for n := range next {
				w.updateAccessController(index, n)
			}
		}()
	}
}

// updateAccessController replaces an old instance of a controller at a particular index with a new one. Once the new controller is added,
// all connections are re-checked to see if they're still valid. This is primarily used by the goroutine spawned in addAccessController.
func (w *Watcher) updateAccessController(index int, controller AccessController) {
	w.mu.Lock()
	copy := w.listeners.Clone()
	w.controllers[index] = controller
	controllers := append([]AccessController(nil), w.controllers...)
	w.mu.Unlock()

	checkListeners(copy, w.options.timeSource, controllers)
}

// ListenForDenied Adds a listener to the watcher for the given connection. If the
// connection is already blocked a nil remove function is returned and an error
// is returned immediately.
//
// Example Usage:
//
//	remove, err := w.ListenForDenied(connection, func(err error) {
//	  /* connection was blocked by change */
//	})
//
// if err != nil { /*connection already blocked*/ }
// defer remove()
//
// Warning:
// Do not call remove() within the error callback. It would deadlock.
func (w *Watcher) ListenForDenied(connection ConnectionTags, callback func(error)) (func(), error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := checkConnection(connection, w.options.timeSource, w.controllers); err != nil {
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

func checkListeners(
	listeners *btree.BTree, timesource timeutil.TimeSource, controllers []AccessController,
) {
	listeners.Ascend(func(i btree.Item) bool {
		lst := i.(*listener)
		if err := checkConnection(lst.connection, timesource, controllers); err != nil {
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

func checkConnection(
	connection ConnectionTags, timesource timeutil.TimeSource, controllers []AccessController,
) error {
	for _, c := range controllers {
		if err := c.CheckConnection(connection, timesource); err != nil {
			return err
		}
	}
	return nil
}
