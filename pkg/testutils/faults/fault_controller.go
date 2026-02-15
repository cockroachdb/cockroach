// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package faults provides runtime-controllable fault injection for unit tests.
//
// This package enables in-process testing of failure scenarios without requiring
// real disk/network operations. The key design principles are:
//
//  1. Zero production code pollution - Faults are injected via interface wrapping,
//     not conditional checks in production paths
//  2. Runtime control - Faults can be started/stopped at any point during test
//     execution without restarting the system
//  3. In-process operation - Works with mock/in-memory implementations,
//     no real I/O required
//
// Example usage:
//
//	fc := faults.NewController()
//
//	// Create a wrapped sender that supports fault injection
//	faultySender := fc.WrapSender(realSender)
//
//	// Later in test - inject delay
//	fc.SetSenderDelay(100 * time.Millisecond)
//	fc.EnableSenderFaults()
//
//	// Operations through faultySender now have 100ms delay
//	resp, err := faultySender.Send(ctx, req)
//
//	// Remove fault
//	fc.DisableSenderFaults()
package faults

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"google.golang.org/grpc"
)

// FaultType categorizes the kind of fault being injected.
type FaultType int

const (
	// FaultTypeNone indicates no fault.
	FaultTypeNone FaultType = iota
	// FaultTypeDelay adds latency to operations.
	FaultTypeDelay
	// FaultTypeBlock blocks operations until explicitly released.
	FaultTypeBlock
	// FaultTypeError returns an error for operations.
	FaultTypeError
	// FaultTypeThrottle limits throughput.
	FaultTypeThrottle
)

// Controller provides centralized, runtime-controllable fault injection.
//
// It manages multiple fault domains (disk, RPC, sender) and allows
// independently enabling/disabling faults in each domain at runtime.
//
// Thread-safety: All methods are safe for concurrent use.
type Controller struct {
	disk   diskFaults
	rpc    rpcFaults
	sender senderFaults
}

// NewController creates a new fault controller with all faults disabled.
func NewController() *Controller {
	c := &Controller{}
	c.disk.blockedCond = sync.NewCond(&c.disk.mu)
	c.sender.blockedCond = sync.NewCond(&c.sender.mu)
	return c
}

// diskFaults controls disk I/O fault injection.
type diskFaults struct {
	enabled     atomic.Bool
	mu          sync.Mutex
	faultType   FaultType
	delay       time.Duration
	blockCh     chan struct{}
	blockedCond *sync.Cond
	blocked     int
	stalledOps  atomic.Int64
}

// rpcFaults controls RPC fault injection.
type rpcFaults struct {
	enabled atomic.Bool
	mu      sync.RWMutex

	// delays maps target address to delay duration.
	delays map[string]time.Duration

	// partitions maps source to set of blocked destinations.
	partitions map[string]map[string]struct{}

	// errors maps target address to error to return.
	errors map[string]error
}

// senderFaults controls kv.Sender fault injection.
type senderFaults struct {
	enabled     atomic.Bool
	mu          sync.Mutex
	faultType   FaultType
	delay       time.Duration
	err         error
	blockCh     chan struct{}
	blockedCond *sync.Cond
	blocked     int
	stalledOps  atomic.Int64

	// keyFilter, if set, only affects operations on matching keys.
	keyFilter func(key []byte) bool
}

// -----------------------------------------------------------------------------
// Disk Faults
// -----------------------------------------------------------------------------

// DiskStallInjector returns a StallableInjector that can be used with errorfs.Wrap.
// The injector's behavior is controlled by this Controller at runtime.
//
// Example:
//
//	fc := faults.NewController()
//	injector := fc.DiskStallInjector(nil)
//	wrappedFS := errorfs.Wrap(memFS, injector)
//
//	// Later:
//	fc.SetDiskDelay(100 * time.Millisecond)
//	fc.EnableDiskFaults()
func (c *Controller) DiskStallInjector(opFilter func(errorfs.Op) bool) *fs.StallableInjector {
	// Create a StallableInjector that delegates to our controller
	return fs.NewStallableInjectorWithController(&diskInjectorAdapter{c: c}, opFilter)
}

// diskInjectorAdapter adapts Controller to the interface expected by StallableInjector.
type diskInjectorAdapter struct {
	c *Controller
}

func (a *diskInjectorAdapter) IsEnabled() bool {
	return a.c.disk.enabled.Load()
}

func (a *diskInjectorAdapter) Stall() {
	a.c.disk.stalledOps.Add(1)

	// Read fault configuration under lock.
	a.c.disk.mu.Lock()
	faultType := a.c.disk.faultType
	delay := a.c.disk.delay
	blockCh := a.c.disk.blockCh
	a.c.disk.mu.Unlock()

	switch faultType {
	case FaultTypeBlock:
		a.c.diskIncrementBlocked()
		<-blockCh
		a.c.diskDecrementBlocked()

	case FaultTypeDelay:
		time.Sleep(delay)
	}
}

func (c *Controller) diskIncrementBlocked() {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	c.disk.blocked++
	c.disk.blockedCond.Broadcast()
}

func (c *Controller) diskDecrementBlocked() {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	c.disk.blocked--
}

// EnableDiskFaults enables disk fault injection.
func (c *Controller) EnableDiskFaults() {
	c.disk.enabled.Store(true)
}

// DisableDiskFaults disables disk fault injection.
func (c *Controller) DisableDiskFaults() {
	c.disk.enabled.Store(false)
}

// SetDiskBlock configures disk faults to block all operations until ReleaseDisk is called.
func (c *Controller) SetDiskBlock() {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	c.disk.faultType = FaultTypeBlock
	c.disk.blockCh = make(chan struct{})
	c.disk.enabled.Store(true)
}

// ReleaseDisk unblocks all blocked disk operations and disables blocking.
func (c *Controller) ReleaseDisk() {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	if c.disk.blockCh != nil {
		close(c.disk.blockCh)
		c.disk.blockCh = nil
	}
	c.disk.faultType = FaultTypeNone
	c.disk.enabled.Store(false)
}

// SetDiskDelay configures disk faults to add the specified delay to all operations.
func (c *Controller) SetDiskDelay(d time.Duration) {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	c.disk.faultType = FaultTypeDelay
	c.disk.delay = d
	c.disk.enabled.Store(true)
}

// WaitForDiskBlocked blocks until at least n operations are blocked on disk I/O.
func (c *Controller) WaitForDiskBlocked(n int) {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	for c.disk.blocked < n {
		c.disk.blockedCond.Wait()
	}
}

// DiskBlockedCount returns the number of operations currently blocked on disk I/O.
func (c *Controller) DiskBlockedCount() int {
	c.disk.mu.Lock()
	defer c.disk.mu.Unlock()
	return c.disk.blocked
}

// DiskStalledOps returns the total number of disk operations that have been stalled.
func (c *Controller) DiskStalledOps() int64 {
	return c.disk.stalledOps.Load()
}

// -----------------------------------------------------------------------------
// RPC Faults
// -----------------------------------------------------------------------------

// EnableRPCFaults enables RPC fault injection.
func (c *Controller) EnableRPCFaults() {
	c.rpc.enabled.Store(true)
}

// DisableRPCFaults disables RPC fault injection.
func (c *Controller) DisableRPCFaults() {
	c.rpc.enabled.Store(false)
}

// SetRPCDelay sets a delay for RPCs to the specified target address.
func (c *Controller) SetRPCDelay(target string, d time.Duration) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	if c.rpc.delays == nil {
		c.rpc.delays = make(map[string]time.Duration)
	}
	c.rpc.delays[target] = d
}

// ClearRPCDelay removes any delay for the specified target address.
func (c *Controller) ClearRPCDelay(target string) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	delete(c.rpc.delays, target)
}

// AddRPCPartition creates a network partition from source to target.
// RPCs from source to target will fail with an error.
func (c *Controller) AddRPCPartition(source, target string) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	if c.rpc.partitions == nil {
		c.rpc.partitions = make(map[string]map[string]struct{})
	}
	if c.rpc.partitions[source] == nil {
		c.rpc.partitions[source] = make(map[string]struct{})
	}
	c.rpc.partitions[source][target] = struct{}{}
}

// RemoveRPCPartition removes a network partition.
func (c *Controller) RemoveRPCPartition(source, target string) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	if c.rpc.partitions != nil && c.rpc.partitions[source] != nil {
		delete(c.rpc.partitions[source], target)
	}
}

// SetRPCError sets an error to return for RPCs to the specified target.
func (c *Controller) SetRPCError(target string, err error) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	if c.rpc.errors == nil {
		c.rpc.errors = make(map[string]error)
	}
	c.rpc.errors[target] = err
}

// ClearRPCError removes any error for the specified target.
func (c *Controller) ClearRPCError(target string) {
	c.rpc.mu.Lock()
	defer c.rpc.mu.Unlock()
	delete(c.rpc.errors, target)
}

// checkRPCFault checks for RPC faults and returns any error, delay to apply.
// This is extracted to handle locking properly.
func (c *Controller) checkRPCFault(source, target string) (error, time.Duration) {
	if !c.rpc.enabled.Load() {
		return nil, 0
	}

	c.rpc.mu.RLock()
	defer c.rpc.mu.RUnlock()

	// Check for partition
	if c.rpc.partitions != nil {
		if targets, ok := c.rpc.partitions[source]; ok {
			if _, partitioned := targets[target]; partitioned {
				return errors.Newf("rpc error: partitioned from %s to %s", source, target), 0
			}
		}
	}

	// Check for error
	if c.rpc.errors != nil {
		if err, ok := c.rpc.errors[target]; ok {
			return err, 0
		}
	}

	// Check for delay
	return nil, c.rpc.delays[target]
}

// UnaryClientInterceptor returns a gRPC unary interceptor that applies RPC faults.
// This should be set via ContextTestingKnobs.UnaryClientInterceptor.
func (c *Controller) UnaryClientInterceptor(source string) func(target string, class rpcbase.ConnectionClass) grpc.UnaryClientInterceptor {
	return func(target string, class rpcbase.ConnectionClass) grpc.UnaryClientInterceptor {
		return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if err, delay := c.checkRPCFault(source, target); err != nil {
				return err
			} else if delay > 0 {
				time.Sleep(delay)
			}

			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}
}

// StreamClientInterceptor returns a gRPC stream interceptor that applies RPC faults.
func (c *Controller) StreamClientInterceptor(source string) func(target string, class rpcbase.ConnectionClass) grpc.StreamClientInterceptor {
	return func(target string, class rpcbase.ConnectionClass) grpc.StreamClientInterceptor {
		return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if err, delay := c.checkRPCFault(source, target); err != nil {
				return nil, err
			} else if delay > 0 {
				time.Sleep(delay)
			}

			cs, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				return nil, err
			}

			// Wrap stream to check partition on each message
			return &faultyClientStream{
				ClientStream: cs,
				checkFault: func() error {
					err, _ := c.checkRPCFault(source, target)
					return err
				},
			}, nil
		}
	}
}

type faultyClientStream struct {
	grpc.ClientStream
	checkFault func() error
}

func (s *faultyClientStream) SendMsg(m interface{}) error {
	if err := s.checkFault(); err != nil {
		return err
	}
	return s.ClientStream.SendMsg(m)
}

func (s *faultyClientStream) RecvMsg(m interface{}) error {
	if err := s.checkFault(); err != nil {
		return err
	}
	return s.ClientStream.RecvMsg(m)
}

// -----------------------------------------------------------------------------
// Sender Faults
// -----------------------------------------------------------------------------

// Sender is the interface for sending KV batch requests.
type Sender interface {
	Send(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// WrapSender wraps a kv.Sender with fault injection capabilities.
// The returned sender checks the Controller's state on every operation.
func (c *Controller) WrapSender(s Sender) Sender {
	return &faultySender{c: c, inner: s}
}

type faultySender struct {
	c     *Controller
	inner Sender
}

// senderFaultResult holds the result of checking sender faults.
type senderFaultResult struct {
	faultType FaultType
	delay     time.Duration
	err       error
	blockCh   chan struct{}
}

// checkSenderFault reads sender fault configuration under lock.
func (c *Controller) checkSenderFault(ba *kvpb.BatchRequest) *senderFaultResult {
	if !c.sender.enabled.Load() {
		return nil
	}

	// Check key filter if set
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()

	if c.sender.keyFilter != nil {
		shouldApply := false
		for _, req := range ba.Requests {
			header := req.GetInner().Header()
			if c.sender.keyFilter(header.Key) {
				shouldApply = true
				break
			}
		}
		if !shouldApply {
			return nil
		}
	}

	return &senderFaultResult{
		faultType: c.sender.faultType,
		delay:     c.sender.delay,
		err:       c.sender.err,
		blockCh:   c.sender.blockCh,
	}
}

func (c *Controller) senderIncrementBlocked() {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.blocked++
	c.sender.blockedCond.Broadcast()
}

func (c *Controller) senderDecrementBlocked() {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.blocked--
}

func (s *faultySender) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if fault := s.c.checkSenderFault(ba); fault != nil {
		s.c.sender.stalledOps.Add(1)

		switch fault.faultType {
		case FaultTypeBlock:
			s.c.senderIncrementBlocked()
			<-fault.blockCh
			s.c.senderDecrementBlocked()

		case FaultTypeDelay:
			time.Sleep(fault.delay)

		case FaultTypeError:
			return nil, kvpb.NewError(fault.err)
		}
	}

	return s.inner.Send(ctx, ba)
}

// EnableSenderFaults enables sender fault injection.
func (c *Controller) EnableSenderFaults() {
	c.sender.enabled.Store(true)
}

// DisableSenderFaults disables sender fault injection.
func (c *Controller) DisableSenderFaults() {
	c.sender.enabled.Store(false)
}

// SetSenderBlock configures sender faults to block all operations until ReleaseSender is called.
func (c *Controller) SetSenderBlock() {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.faultType = FaultTypeBlock
	c.sender.blockCh = make(chan struct{})
	c.sender.enabled.Store(true)
}

// ReleaseSender unblocks all blocked sender operations.
func (c *Controller) ReleaseSender() {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	if c.sender.blockCh != nil {
		close(c.sender.blockCh)
		c.sender.blockCh = nil
	}
	c.sender.faultType = FaultTypeNone
	c.sender.enabled.Store(false)
}

// SetSenderDelay configures sender faults to add the specified delay.
func (c *Controller) SetSenderDelay(d time.Duration) {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.faultType = FaultTypeDelay
	c.sender.delay = d
	c.sender.enabled.Store(true)
}

// SetSenderError configures sender faults to return the specified error.
func (c *Controller) SetSenderError(err error) {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.faultType = FaultTypeError
	c.sender.err = err
	c.sender.enabled.Store(true)
}

// SetSenderKeyFilter sets a filter function that determines which keys are affected.
// If nil, all keys are affected.
func (c *Controller) SetSenderKeyFilter(filter func(key []byte) bool) {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	c.sender.keyFilter = filter
}

// WaitForSenderBlocked blocks until at least n operations are blocked.
func (c *Controller) WaitForSenderBlocked(n int) {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	for c.sender.blocked < n {
		c.sender.blockedCond.Wait()
	}
}

// SenderBlockedCount returns the number of operations currently blocked.
func (c *Controller) SenderBlockedCount() int {
	c.sender.mu.Lock()
	defer c.sender.mu.Unlock()
	return c.sender.blocked
}

// SenderStalledOps returns the total number of sender operations that have been stalled.
func (c *Controller) SenderStalledOps() int64 {
	return c.sender.stalledOps.Load()
}
