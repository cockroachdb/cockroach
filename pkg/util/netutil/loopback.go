// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package netutil

import (
	"context"
	"net"
	"sync"
	"time"

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
	a := bidi{c1, c2}
	b := bidi{c2, c1}
	select {
	case l.conns <- a:
		return b, nil
	case <-l.stopper.ShouldQuiesce():
	case <-l.active:
	}
	err = ErrLocalListenerClosed
	err = errors.CombineErrors(err, a.Close())
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

type bidi struct {
	thisSide, otherSide net.Conn
}

var _ net.Conn = bidi{}

func (b bidi) Read(p []byte) (n int, err error)  { return b.thisSide.Read(p) }
func (b bidi) Write(p []byte) (n int, err error) { return b.thisSide.Write(p) }
func (b bidi) Close() error {
	err := b.thisSide.Close()
	err = errors.CombineErrors(err, b.otherSide.Close())
	return err
}
func (b bidi) LocalAddr() net.Addr                { return loopbackAddr{} }
func (b bidi) RemoteAddr() net.Addr               { return loopbackAddr{} }
func (b bidi) SetDeadline(t time.Time) error      { return b.thisSide.SetDeadline(t) }
func (b bidi) SetReadDeadline(t time.Time) error  { return b.thisSide.SetReadDeadline(t) }
func (b bidi) SetWriteDeadline(t time.Time) error { return b.thisSide.SetWriteDeadline(t) }
