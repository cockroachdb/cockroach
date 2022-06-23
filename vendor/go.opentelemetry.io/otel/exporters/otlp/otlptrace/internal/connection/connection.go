// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connection

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc/encoding/gzip"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/otlpconfig"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Connection struct {
	// Ensure pointer is 64-bit aligned for atomic operations on both 32 and 64 bit machines.
	lastConnectErrPtr unsafe.Pointer

	// mu protects the Connection as it is accessed by the
	// exporter goroutines and background Connection goroutine
	mu sync.Mutex
	cc *grpc.ClientConn

	// these fields are read-only after constructor is finished
	cfg                  otlpconfig.Config
	SCfg                 otlpconfig.SignalConfig
	requestFunc          retry.RequestFunc
	metadata             metadata.MD
	newConnectionHandler func(cc *grpc.ClientConn)

	// these channels are created once
	disconnectedCh             chan bool
	backgroundConnectionDoneCh chan struct{}
	stopCh                     chan struct{}

	// this is for tests, so they can replace the closing
	// routine without a worry of modifying some global variable
	// or changing it back to original after the test is done
	closeBackgroundConnectionDoneCh func(ch chan struct{})
}

func NewConnection(cfg otlpconfig.Config, sCfg otlpconfig.SignalConfig, handler func(cc *grpc.ClientConn)) *Connection {
	c := new(Connection)
	c.newConnectionHandler = handler
	c.cfg = cfg
	c.requestFunc = cfg.RetryConfig.RequestFunc(evaluate)
	c.SCfg = sCfg
	if len(c.SCfg.Headers) > 0 {
		c.metadata = metadata.New(c.SCfg.Headers)
	}
	c.closeBackgroundConnectionDoneCh = func(ch chan struct{}) {
		close(ch)
	}
	return c
}

func (c *Connection) StartConnection(ctx context.Context) error {
	c.stopCh = make(chan struct{})
	c.disconnectedCh = make(chan bool, 1)
	c.backgroundConnectionDoneCh = make(chan struct{})

	if err := c.connect(ctx); err == nil {
		c.setStateConnected()
	} else {
		c.SetStateDisconnected(err)
	}
	go c.indefiniteBackgroundConnection()

	// TODO: proper error handling when initializing connections.
	// We can report permanent errors, e.g., invalid settings.
	return nil
}

func (c *Connection) LastConnectError() error {
	errPtr := (*error)(atomic.LoadPointer(&c.lastConnectErrPtr))
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

func (c *Connection) saveLastConnectError(err error) {
	var errPtr *error
	if err != nil {
		errPtr = &err
	}
	atomic.StorePointer(&c.lastConnectErrPtr, unsafe.Pointer(errPtr))
}

func (c *Connection) SetStateDisconnected(err error) {
	c.saveLastConnectError(err)
	select {
	case c.disconnectedCh <- true:
	default:
	}
	c.newConnectionHandler(nil)
}

func (c *Connection) setStateConnected() {
	c.saveLastConnectError(nil)
}

func (c *Connection) Connected() bool {
	return c.LastConnectError() == nil
}

const defaultConnReattemptPeriod = 10 * time.Second

func (c *Connection) indefiniteBackgroundConnection() {
	defer func() {
		c.closeBackgroundConnectionDoneCh(c.backgroundConnectionDoneCh)
	}()

	connReattemptPeriod := c.cfg.ReconnectionPeriod
	if connReattemptPeriod <= 0 {
		connReattemptPeriod = defaultConnReattemptPeriod
	}

	// No strong seeding required, nano time can
	// already help with pseudo uniqueness.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63n(1024)))

	// maxJitterNanos: 70% of the connectionReattemptPeriod
	maxJitterNanos := int64(0.7 * float64(connReattemptPeriod))

	for {
		// Otherwise these will be the normal scenarios to enable
		// reconnection if we trip out.
		// 1. If we've stopped, return entirely
		// 2. Otherwise block until we are disconnected, and
		//    then retry connecting
		select {
		case <-c.stopCh:
			return

		case <-c.disconnectedCh:
			// Quickly check if we haven't stopped at the
			// same time.
			select {
			case <-c.stopCh:
				return

			default:
			}

			// Normal scenario that we'll wait for
		}

		if err := c.connect(context.Background()); err == nil {
			c.setStateConnected()
		} else {
			// this code is unreachable in most cases
			// c.connect does not establish Connection
			c.SetStateDisconnected(err)
		}

		// Apply some jitter to avoid lockstep retrials of other
		// collector-exporters. Lockstep retrials could result in an
		// innocent DDOS, by clogging the machine's resources and network.
		jitter := time.Duration(rng.Int63n(maxJitterNanos))
		select {
		case <-c.stopCh:
			return
		case <-time.After(connReattemptPeriod + jitter):
		}
	}
}

func (c *Connection) connect(ctx context.Context) error {
	cc, err := c.dialToCollector(ctx)
	if err != nil {
		return err
	}
	c.setConnection(cc)
	c.newConnectionHandler(cc)
	return nil
}

// setConnection sets cc as the client Connection and returns true if
// the Connection state changed.
func (c *Connection) setConnection(cc *grpc.ClientConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If previous clientConn is same as the current then just return.
	// This doesn't happen right now as this func is only called with new ClientConn.
	// It is more about future-proofing.
	if c.cc == cc {
		return false
	}

	// If the previous clientConn was non-nil, close it
	if c.cc != nil {
		_ = c.cc.Close()
	}
	c.cc = cc
	return true
}

func (c *Connection) dialToCollector(ctx context.Context) (*grpc.ClientConn, error) {
	dialOpts := []grpc.DialOption{}
	if c.cfg.ServiceConfig != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(c.cfg.ServiceConfig))
	}
	if c.SCfg.GRPCCredentials != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(c.SCfg.GRPCCredentials))
	} else if c.SCfg.Insecure {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}
	if c.SCfg.Compression == otlpconfig.GzipCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	if len(c.cfg.DialOptions) != 0 {
		dialOpts = append(dialOpts, c.cfg.DialOptions...)
	}

	ctx, cancel := c.ContextWithStop(ctx)
	defer cancel()
	ctx = c.ContextWithMetadata(ctx)
	return grpc.DialContext(ctx, c.SCfg.Endpoint, dialOpts...)
}

func (c *Connection) ContextWithMetadata(ctx context.Context) context.Context {
	if c.metadata.Len() > 0 {
		return metadata.NewOutgoingContext(ctx, c.metadata)
	}
	return ctx
}

func (c *Connection) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	// Ensure that the backgroundConnector returns
	select {
	case <-c.backgroundConnectionDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	c.mu.Lock()
	cc := c.cc
	c.cc = nil
	c.mu.Unlock()

	if cc != nil {
		return cc.Close()
	}

	return nil
}

func (c *Connection) ContextWithStop(ctx context.Context) (context.Context, context.CancelFunc) {
	// Unify the parent context Done signal with the Connection's
	// stop channel.
	ctx, cancel := context.WithCancel(ctx)
	go func(ctx context.Context, cancel context.CancelFunc) {
		select {
		case <-ctx.Done():
			// Nothing to do, either cancelled or deadline
			// happened.
		case <-c.stopCh:
			cancel()
		}
	}(ctx, cancel)
	return ctx, cancel
}

func (c *Connection) DoRequest(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := c.ContextWithStop(ctx)
	defer cancel()
	return c.requestFunc(ctx, func(ctx context.Context) error {
		err := fn(ctx)
		// nil is converted to OK.
		if status.Code(err) == codes.OK {
			// Success.
			return nil
		}
		return err
	})
}

// evaluate returns if err is retry-able and a duration to wait for if an
// explicit throttle time is included in err.
func evaluate(err error) (bool, time.Duration) {
	s := status.Convert(err)
	switch s.Code() {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unavailable,
		codes.DataLoss:
		return true, throttleDelay(s)
	}

	// Not a retry-able error.
	return false, 0
}

// throttleDelay returns a duration to wait for if an explicit throttle time
// is included in the response status.
func throttleDelay(status *status.Status) time.Duration {
	for _, detail := range status.Details() {
		if t, ok := detail.(*errdetails.RetryInfo); ok {
			return t.RetryDelay.AsDuration()
		}
	}
	return 0
}
