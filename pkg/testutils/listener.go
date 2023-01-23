// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// ListenerRegistry is a registry for listener sockets that allows TestServers
// to reuse listener sockets and keep them on the same ports throughout server
// restarts.
// Tests rely on net.Listen on port 0 to open first available port and uses its
// details to let TestServers connect to each other. Tests can't rely on fixed
// ports because it must be possible to run lots of tests in parallel. When
// TestServer is restarted, it would close its listener and reopen a new one
// on a different port as its own port might be reused by that time.
// This registry provides listener wrappers that could be associated with server
// ids and injected into TestServers normal way. Listeners will not close
// actual network sockets when closed, but will pause accepting connections.
// Test could then specifically resume listeners prior to restarting servers.
type ListenerRegistry struct {
	listeners map[int]*reusableListener
}

func NewListenerRegistry() ListenerRegistry {
	return ListenerRegistry{listeners: make(map[int]*reusableListener)}
}

func (r *ListenerRegistry) Get(t *testing.T, idx int) net.Listener {
	t.Helper()
	if l, ok := r.listeners[idx]; ok {
		return l
	}
	nl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to create network listener")
	l := &reusableListener{
		id:      idx,
		wrapped: nl,
		acceptC: make(chan net.Conn),
		stopC:   make(chan interface{}),
	}
	l.resume()
	r.listeners[idx] = l
	go l.run()
	return l
}

func (r *ListenerRegistry) Reopen(idx int) error {
	l, ok := r.listeners[idx]
	if !ok {
		return errors.Newf("socket for id %d is not open", idx)
	}
	l.resume()
	return nil
}

func (r *ListenerRegistry) Close() {
	for k, v := range r.listeners {
		_ = v.wrapped.Close()
		close(v.stopC)
		delete(r.listeners, k)
	}
}

type reusableListener struct {
	id      int
	wrapped net.Listener
	acceptC chan net.Conn
	pauseMu struct {
		syncutil.RWMutex
		pauseC chan interface{}
	}
	stopC chan interface{}
}

func (l *reusableListener) run() {
	defer func() {
		close(l.acceptC)
	}()
	for {
		c, err := l.wrapped.Accept()
		if err != nil {
			return
		}
		select {
		case l.acceptC <- c:
		case <-l.pauseC():
			_ = c.Close()
		case <-l.stopC:
			_ = c.Close()
			return
		}
	}
}

func (l *reusableListener) pauseC() <-chan interface{} {
	l.pauseMu.RLock()
	defer l.pauseMu.RUnlock()
	return l.pauseMu.pauseC
}

func (l *reusableListener) resume() {
	l.pauseMu.Lock()
	defer l.pauseMu.Unlock()
	l.pauseMu.pauseC = make(chan interface{})
}

func (l *reusableListener) Accept() (net.Conn, error) {
	select {
	case c, ok := <-l.acceptC:
		if !ok {
			return nil, net.ErrClosed
		}
		return c, nil
	case <-l.pauseC():
		return nil, net.ErrClosed
	}
}

func (l *reusableListener) Close() error {
	l.pauseMu.Lock()
	defer l.pauseMu.Unlock()
	select {
	case <-l.pauseMu.pauseC:
		// Already paused, nothing to do.
	default:
		close(l.pauseMu.pauseC)
	}
	return nil
}

func (l *reusableListener) Addr() net.Addr {
	return l.wrapped.Addr()
}
