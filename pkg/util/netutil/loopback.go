// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package netutil

import (
	"context"
	"net"
	"sync"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// LoopbackListener implements a local listener
// that delivers net.Conns via its Connect() method
// based on the other side calls to its Accept() method.
type LoopbackListener struct {
	stopper *stop.Stopper

	closeOnce sync.Once
	active    chan struct{}

	// requests are tokens from the Connect() method to the
	// Accept() method.
	requests chan struct{}
	// conns are responses from the Accept() method
	// to the Connect() method.
	conns chan net.Conn
}

var _ net.Listener = (*LoopbackListener)(nil)

// ErrLocalListenerClosed is returned when the listener
// is shutting down.
// note that we need to use cmux.ErrListenerClosed as base (leaf)
// error so that it is recognized as special case in
// netutil.IsClosedConnection.
var ErrLocalListenerClosed = errors.Wrap(cmux.ErrListenerClosed, "loopback listener")

// Accept waits for and returns the next connection to the listener.
func (l *LoopbackListener) Accept() (conn net.Conn, err error) {
	select {
	case <-l.stopper.ShouldQuiesce():
		return nil, ErrLocalListenerClosed
	case <-l.active:
		return nil, ErrLocalListenerClosed
	case <-l.requests:
	}
	c1, c2 := net.Pipe()
	select {
	case l.conns <- c1:
		return c2, nil
	case <-l.stopper.ShouldQuiesce():
	case <-l.active:
	}
	err = ErrLocalListenerClosed
	err = errors.CombineErrors(err, c1.Close())
	err = errors.CombineErrors(err, c2.Close())
	return nil, err
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (l *LoopbackListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.active)
	})
	return nil
}

// Addr returns the listener's network address.
func (l *LoopbackListener) Addr() net.Addr {
	return loopbackAddr{}
}

// Connect signals the Accept method that a conn is needed.
func (l *LoopbackListener) Connect(ctx context.Context) (net.Conn, error) {
	// Send request to acceptor.
	select {
	case <-l.stopper.ShouldQuiesce():
		return nil, ErrLocalListenerClosed
	case <-l.active:
		return nil, ErrLocalListenerClosed
	case l.requests <- struct{}{}:
	}
	// Get conn from acceptor.
	select {
	case <-l.stopper.ShouldQuiesce():
		return nil, ErrLocalListenerClosed
	case <-l.active:
		return nil, ErrLocalListenerClosed
	case conn := <-l.conns:
		return conn, nil
	}
}

// NewLoopbackListener constructs a new LoopbackListener.
func NewLoopbackListener(ctx context.Context, stopper *stop.Stopper) *LoopbackListener {
	return &LoopbackListener{
		stopper:  stopper,
		active:   make(chan struct{}),
		requests: make(chan struct{}),
		conns:    make(chan net.Conn),
	}
}

type loopbackAddr struct{}

var _ net.Addr = loopbackAddr{}

func (loopbackAddr) Network() string { return "pipe" }
func (loopbackAddr) String() string  { return "loopback" }
