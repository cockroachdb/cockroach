// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

const (
	defaultHeartbeatInterval = 3 * time.Second

	// Affects maximum error in reading the clock of the remote. 1.5 seconds is
	// the longest NTP allows for a remote clock reading. After 1.5 seconds, we
	// assume that the offset from the clock is infinite.
	maximumClockReadingDelay = 1500 * time.Millisecond
)

var (
	clientMu          sync.Mutex         // Protects access to the client cache.
	clients           map[string]*Client // Cache of RPC clients.
	heartbeatInterval = defaultHeartbeatInterval
	// TODO(tschottdorf) err{Closed,Unstarted} are candidates for NodeUnavailableError.
	errClosed    = errors.New("client is closed")
	errUnstarted = errors.New("not started yet")
)

// clientRetryOptions specifies exponential backoff starting
// at 1s and ending at 30s with indefinite retries.
// Clients currently never give up.
// TODO(tamird): Add `MaxRetries` here or otherwise address this.
var clientRetryOptions = retry.Options{
	InitialBackoff: 1 * time.Second,  // first backoff at 1s
	MaxBackoff:     30 * time.Second, // max backoff is 30s
	Multiplier:     2,                // doubles
}

// init creates a new client RPC cache.
func init() {
	clients = map[string]*Client{}
}

type internalConn struct {
	conn   net.Conn
	client *rpc.Client
}

// Client is a Cockroach-specific RPC client.
type Client struct {
	key  string // cache key for later removal from cache
	addr util.UnresolvedAddr
	// `closer` is `close()`d when `Close` is called on the client.
	// It signals the end of the heartbeat run loop. When the run loop
	// exits, it `close()`es `Closed`, which signals to the outside
	// that the client has indeed stopped.
	closer, Closed    chan struct{}
	conn              unsafe.Pointer // holds a `internalConn`
	healthy           atomic.Value   // holds a `chan struct{}` exposed in `Healthy`
	healthWaitTime    time.Time
	healthReceived    chan struct{}
	tlsConfig         *tls.Config
	disableReconnects bool
	clock             *hlc.Clock
	remoteClocks      *RemoteClockMonitor
	remoteOffset      RemoteOffset
}

// NewClient returns a client RPC stub for the specified address
// (usually a TCP host:port, but for testing may be a unix domain
// socket). The process-wide client RPC cache is consulted first; if
// the requested client is not present, it's created and the cache is
// updated. Specify opts to fine tune client connection behavior or
// nil to use defaults (i.e. indefinite retries with exponential
// backoff).
//
// The Closed channel is closed if the client's Close() method is
// invoked.
func NewClient(addr net.Addr, context *Context) *Client {
	clientMu.Lock()
	defer clientMu.Unlock()

	unresolvedAddr := util.MakeUnresolvedAddr(addr.Network(), addr.String())

	key := fmt.Sprintf("%s@%s", context.User, unresolvedAddr)

	if !context.DisableCache {
		if c, ok := clients[key]; ok {
			return c
		}
	}

	tlsConfig, err := context.GetClientTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	c := &Client{
		closer:            make(chan struct{}),
		Closed:            make(chan struct{}),
		key:               key,
		addr:              unresolvedAddr,
		tlsConfig:         tlsConfig,
		disableReconnects: context.DisableReconnects,
		clock:             context.localClock,
		remoteClocks:      context.RemoteClocks,
	}

	c.healthy.Store(make(chan struct{}))
	c.healthWaitTime = time.Now().Add(context.HealthWait)
	c.healthReceived = make(chan struct{})

	if !context.DisableCache {
		clients[key] = c
	}

	retryOpts := clientRetryOptions
	retryOpts.Closer = context.Stopper.ShouldStop()

	context.Stopper.RunWorker(func() {
		c.runHeartbeat(retryOpts)

		close(c.Closed)

		if conn := c.internalConn(); conn != nil {
			conn.client.Close()
		}
	})

	return c
}

// Go delegates to net/rpc.Client.Go.
func (c *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) *rpc.Call {
	return c.internalConn().client.Go(serviceMethod, args, reply, done)
}

// Call delegates to net/rpc.Client.Call.
func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return c.internalConn().client.Call(serviceMethod, args, reply)
}

func (c *Client) internalConn() *internalConn {
	return (*internalConn)(atomic.LoadPointer(&c.conn))
}

// connect attempts a single connection attempt. On success, updates `c.conn`.
func (c *Client) connect() error {
	conn, err := codec.TLSDialHTTP(
		c.addr.NetworkField, c.addr.AddressField, base.NetworkTimeout, c.tlsConfig)
	if err != nil {
		return err
	}
	if oldConn := (*internalConn)(atomic.SwapPointer(&c.conn, unsafe.Pointer(&internalConn{
		conn:   conn,
		client: rpc.NewClientWithCodec(codec.NewClientCodec(conn)),
	}))); oldConn != nil {
		oldConn.conn.Close()
	}

	return nil
}

// Healthy returns a channel that is closed when the client becomes healthy.
// In the event of the client becoming unhealthy, future calls to Healthy()
// return a new channel.
func (c *Client) Healthy() <-chan struct{} {
	return c.healthy.Load().(chan struct{})
}

func (c *Client) isHealthy() bool {
	select {
	case <-c.Healthy():
		return true
	default:
	}
	return false
}

// WaitHealthy returns the health of the Client. On the first connection of a
// newly-created Client, WaitHealthy will block for up to Context.HealthWait if
// its health has not yet been determined.
func (c *Client) WaitHealthy() bool {
	// If we shouldn't wait for healthy, return immediately. The healthReceived
	// channel is closed after we receive the first health indication for the
	// client (i.e. (un)successful heartbeat or failure to open the connection).
	select {
	case <-c.healthReceived:
		return c.isHealthy()
	default:
	}

	if delta := c.healthWaitTime.Sub(time.Now()); delta > 0 {
		select {
		case <-time.After(delta):
		case <-c.healthReceived:
			return c.isHealthy()
		case <-c.closer:
		}
	}
	return false
}

// Close closes the client, removing it from the clients cache and returning
// when the heartbeat goroutine has exited.
func (c *Client) Close() {
	c.close()
	<-c.Closed
}

func (c *Client) close() {
	clientMu.Lock()
	defer clientMu.Unlock()

	select {
	case <-c.closer:
	case <-c.Closed:
	default:
		delete(clients, c.key)
		close(c.closer)
	}
}

// runHeartbeat sends periodic heartbeats to client, marking the client healthy
// or unhealthy and reconnecting appropriately until either the Client or the
// supplied channel is closed.
func (c *Client) runHeartbeat(retryOpts retry.Options) {
	healthReceived := c.healthReceived
	setHealthReceived := func() {
		if healthReceived != nil {
			close(healthReceived)
			healthReceived = nil
		}
	}

	isHealthy := false
	setHealthy := func() {
		if isHealthy {
			return
		}
		isHealthy = true
		close(c.healthy.Load().(chan struct{}))
		setHealthReceived()
	}
	setUnhealthy := func() {
		if isHealthy {
			isHealthy = false
			c.healthy.Store(make(chan struct{}))
		}
		setHealthReceived()
	}

	var err = errUnstarted // initial condition
	for {
		for r := retry.Start(retryOpts); r.Next(); {
			select {
			case <-c.closer:
				return
			case <-retryOpts.Closer:
				c.close()
				return
			default:
			}

			// Reconnect on failure.
			if err != nil {
				// If reconnects are disabled, and we already have a failed connection, return now.
				if c.disableReconnects && c.internalConn() != nil {
					return
				}
				if err = c.connect(); err != nil {
					setUnhealthy()
					log.Warning(err)
					continue
				}
			}

			// Heartbeat regardless of failure.
			if err = c.heartbeat(retryOpts.Closer); err != nil {
				setUnhealthy()
				log.Warning(err)
				continue
			}

			setHealthy()
			break
		}

		// Wait after the heartbeat so that the first iteration gets a wait-free
		// heartbeat attempt.
		select {
		case <-c.closer:
			return
		case <-retryOpts.Closer:
			c.close()
			return
		case <-time.After(heartbeatInterval):
			// TODO(tamird): Perhaps retry more aggressively when the client is unhealthy.
		}
	}
}

// LocalAddr returns the local address of the client.
func (c *Client) LocalAddr() net.Addr {
	return c.internalConn().conn.LocalAddr()
}

// RemoteAddr returns remote address of the client.
func (c *Client) RemoteAddr() net.Addr {
	return c.addr
}

// heartbeat sends a single heartbeat RPC. As part of the heartbeat protocol,
// it measures the clock of the remote to determine the node's clock offset
// from the remote.
func (c *Client) heartbeat(closer <-chan struct{}) error {
	request := &PingRequest{Offset: c.remoteOffset, Addr: c.LocalAddr().String()}
	response := &PingResponse{}
	sendTime := c.clock.PhysicalNow()

	call := c.Go("Heartbeat.Ping", request, response, nil)

	select {
	case <-closer:
		return errClosed
	case <-c.closer:
		return errClosed
	case <-call.Done:
		if err := call.Error; err != nil {
			return err
		}
	case <-time.After(2 * heartbeatInterval):
		return util.Errorf("heartbeat timed out after %s", 2*heartbeatInterval)
	}

	receiveTime := c.clock.PhysicalNow()

	// Only update the clock offset measurement if we actually got a
	// successful response from the server.
	if receiveTime > sendTime+maximumClockReadingDelay.Nanoseconds() {
		c.remoteOffset.Reset()
	} else {
		// Offset and error are measured using the remote clock reading
		// technique described in
		// http://se.inf.tu-dresden.de/pubs/papers/SRDS1994.pdf, page 6.
		// However, we assume that drift and min message delay are 0, for
		// now.
		c.remoteOffset.MeasuredAt = receiveTime
		c.remoteOffset.Uncertainty = (receiveTime - sendTime) / 2
		remoteTimeNow := response.ServerTime + c.remoteOffset.Uncertainty
		c.remoteOffset.Offset = remoteTimeNow - receiveTime
		c.remoteClocks.UpdateOffset(c.RemoteAddr().String(), c.remoteOffset)
	}

	return nil
}
