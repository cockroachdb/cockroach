// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"storj.io/drpc/drpcpool"
)

// Connection is a wrapper around grpc.ClientConn. It prevents the underlying
// connection from being used until it has been validated via heartbeat.
type Connection struct {
	// Fields in this struct are only ever mutated from the circuit breaker probe,
	// but they may be read widely (many callers hold a *Connection).

	// The following fields are populated on instantiation.
	k peerKey
	// breakerSignalFn is (*Breaker).Signal of the surrounding `*peer`. We consult
	// this in Connect() to abort dial attempts when the breaker is tripped.
	breakerSignalFn func() circuit.Signal
	// connFuture is signaled with success (revealing the clientConn) once the
	// initial heartbeat succeeds. If we fail to create a ClientConn or the
	// ClientConn fails its first heartbeat, it's signaled with an error.
	//
	// connFuture can be signaled (like any mutation, from the probe only) without
	// holding the surrounding mutex.
	//
	// It always has to be signaled eventually, regardless of the stopper
	// draining, etc, since callers might be blocking on it.
	connFuture connFuture
	// batchStreamPool holds a pool of BatchStreamClient streams established on
	// the connection. The pool can be used to avoid the overhead of unary Batch
	// RPCs.
	//
	// The pool is only initialized once the ClientConn is resolved.
	batchStreamPool     BatchStreamPool
	drpcBatchStreamPool DRPCBatchStreamPool
}

// newConnectionToNodeID makes a Connection for the given node, class, and nontrivial Signal
// that should be queried in Connect().
func newConnectionToNodeID(
	opts *ContextOptions, k peerKey, breakerSignal func() circuit.Signal,
) *Connection {
	c := &Connection{
		breakerSignalFn: breakerSignal,
		k:               k,
		connFuture: connFuture{
			ready: make(chan struct{}),
		},
		batchStreamPool:     makeStreamPool(opts.Stopper, newBatchStream),
		drpcBatchStreamPool: makeStreamPool(opts.Stopper, newDRPCBatchStream),
	}
	return c
}

// waitOrDefault blocks on initialHeartbeatDone and returns either an error or
// the unwrapped grpc connection. If the provided context or signal fire, they
// will short-circuit the waiting process. The signal may be nil in which case
// it is ignored. If a non-nil defErr is provided, waitOrDefault will never
// block but fall back to defErr in this case.
func (c *Connection) waitOrDefault(
	ctx context.Context, defErr error, sig circuit.Signal,
) (*grpc.ClientConn, drpcpool.Conn, error) {
	// Check the circuit breaker first. If it is already tripped now, we
	// want it to take precedence over connFuture below (which is closed in
	// the common case of a connection going bad after having been healthy
	// for a while).
	select {
	case <-sig.C():
		return nil, nil, sig.Err()
	default:
	}

	// Wait for either the breaker to trip, the caller to give up, or the waitCh
	// to fire. Because we support both a default and no default in this method,
	// there are two largely identical branches that should be kept in sync.
	if defErr == nil {
		select {
		case <-c.connFuture.C():
		case <-sig.C():
			return nil, nil, sig.Err()
		case <-ctx.Done():
			return nil, nil, errors.Wrapf(ctx.Err(), "while connecting to n%d at %s", c.k.NodeID, c.k.TargetAddr)
		}
	} else {
		select {
		case <-c.connFuture.C():
		case <-sig.C():
			return nil, nil, sig.Err()
		case <-ctx.Done():
			return nil, nil, errors.Wrapf(ctx.Err(), "while connecting to n%d at %s", c.k.NodeID, c.k.TargetAddr)
		default:
			return nil, nil, defErr
		}
	}

	// Done waiting, c.connFuture has resolved, return the result. Note that this
	// conn could be unhealthy (or there may not even be a conn, i.e. Err() !=
	// nil), if that's what the caller wanted (ConnectNoBreaker).
	return c.connFuture.Conn(), c.connFuture.DRPCConn(), c.connFuture.Err()
}

// Connect returns the underlying grpc.ClientConn after it has been validated,
// or an error if dialing or validation fails. Connect implements circuit
// breaking, i.e. there is a circuit breaker for each peer and if the breaker is
// tripped (which happens when a heartbeat fails), Connect will fail-fast with
// an error. In rare cases, this behavior is undesired and ConnectNoBreaker may
// be used instead.
func (c *Connection) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	cc, _, err := c.waitOrDefault(ctx, nil /* defErr */, c.breakerSignalFn())
	return cc, err
}

// ConnectEx is similar to Connect but it addition to gRPC connection, it also
// returns underlying drpc connection after it has been validated.
func (c *Connection) ConnectEx(ctx context.Context) (*grpc.ClientConn, drpcpool.Conn, error) {
	return c.waitOrDefault(ctx, nil /* defErr */, c.breakerSignalFn())
}

type neverTripSignal struct{}

func (s *neverTripSignal) Err() error {
	return nil
}

func (s *neverTripSignal) C() <-chan struct{} {
	return nil
}

func (s *neverTripSignal) IsTripped() bool {
	return false
}

// ConnectNoBreaker is like Connect but bypasses the circuit breaker, meaning
// that it will latch onto (or start) an existing connection attempt even if
// previous attempts have not succeeded. This may be preferable to Connect
// if the caller is already certain that a peer is available.
func (c *Connection) ConnectNoBreaker(
	ctx context.Context,
) (*grpc.ClientConn, drpcpool.Conn, error) {
	// For ConnectNoBreaker we don't use the default Signal but pass a dummy one
	// that never trips. (The probe tears down the Conn on quiesce so we don't rely
	// on the Signal for that).
	//
	// Because peer probe attempts can become on-demand (when the peer suspects
	// that it is stale and waiting to be deleted) we touch the real Signal to
	// make sure a probe is attempted (async). This doesn't matter for this call -
	// after all we're already tied to a *Connection - but it matters for future
	// calls. We could hypothetically end up in a situation in which the
	// *Connection is unhealthy and the breaker is tripped, but the *peer has
	// deleteAfter set and is thus not currently running a probe. If there are no
	// calls to Connect (which queries the Signal, starting a probe), the defunct
	// *Connection will remain around forever. By simply reading the Signal here
	// we make sure that calls to ConnectNoBreaker tip the probe off as well,
	// avoiding this problem.
	_ = c.Signal().Err()
	return c.waitOrDefault(ctx, nil /* defErr */, &neverTripSignal{})
}

// Health returns an error indicating the success or failure of the connection's
// latest heartbeat. Returns ErrNotHeartbeated if the peer was just contacted for
// the first time and the first heartbeat has not occurred yet.
func (c *Connection) Health() error {
	_, _, err := c.waitOrDefault(context.Background(), ErrNotHeartbeated, c.breakerSignalFn())
	return err
}

func (c *Connection) Signal() circuit.Signal {
	return c.breakerSignalFn()
}

func (c *Connection) BatchStreamPool() *BatchStreamPool {
	if !c.connFuture.Resolved() {
		panic("BatchStreamPool called on unresolved connection")
	}
	return &c.batchStreamPool
}

func (c *Connection) DRPCBatchStreamPool() *DRPCBatchStreamPool {
	if !c.connFuture.Resolved() {
		panic("DRPCBatchStreamPool called on unresolved connection")
	}
	return &c.drpcBatchStreamPool
}

type connFuture struct {
	ready chan struct{}
	cc    *grpc.ClientConn
	dc    drpcpool.Conn
	err   error
}

var _ circuit.Signal = (*connFuture)(nil)

func (s *connFuture) C() <-chan struct{} {
	return s.ready
}

// Err must only be called after C() has been closed.
func (s *connFuture) Err() error {
	return s.err
}

func (s *connFuture) IsTripped() bool {
	return s.Resolved()
}

// Conn must only be called after C() has been closed.
func (s *connFuture) Conn() *grpc.ClientConn {
	if s.err != nil {
		return nil
	}
	return s.cc
}

// DRPCConn must only be called after C() has been closed.
func (s *connFuture) DRPCConn() drpcpool.Conn {
	if s.err != nil {
		return nil
	}
	return s.dc
}

func (s *connFuture) Resolved() bool {
	select {
	case <-s.ready:
		return true
	default:
		return false
	}
}

// Resolve is idempotent. Only the first call has any effect.
// Not thread safe.
func (s *connFuture) Resolve(cc *grpc.ClientConn, dc drpcpool.Conn, err error) {
	select {
	case <-s.ready:
		// Already resolved, noop.
	default:
		s.cc, s.dc, s.err = cc, dc, err
		close(s.ready)
	}
}
