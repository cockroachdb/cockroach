// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mon

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// BoundAccount and BytesMonitor together form the mechanism by which
// allocations are tracked and constrained (e.g. memory allocations by the
// server on behalf of db clients). The primary motivation is to avoid common
// cases of memory or disk blow-ups due to user error or unoptimized queries; a
// secondary motivation in the longer term is to offer more detailed metrics to
// users to track and explain memory/disk usage.
//
// The overall mechanism functions as follows:
//
// - components in CockroachDB that wish to have their allocations tracked
//   declare/register their allocations to an instance of BytesMonitor. To do
//   this, each component maintains one or more instances of BoundAccount, one
//   per "category" of allocation, and issue requests to Grow, Resize or Close
//   to their monitor. Grow/Resize requests can be denied (return an error),
//   which indicates the budget has been reached.
//

// - different instances of BoundAccount are associated to different usage
//   categories in components, in principle to track different object
//   lifetimes. Each account tracks the total amount of bytes allocated in
//   that category and enables declaring all the bytes as released at once
//   using Close().
//
// - BytesMonitor checks the total sum of allocations across accounts, but also
//   serves as endpoint for statistics. Therefore each db client connection
//   should use a separate monitor for its allocations, so that statistics can
//   be separated per connection.
//
// - a BytesMonitor can be used standalone, and operate independently from other
//   monitors; however, since we also want to constrain global bytes usage
//   across all connections, multiple instance of BytesMonitors can coordinate
//   with each other by referring to a shared BytesMonitor, also known as
//   "pool". When operating in that mode, each BytesMonitor reports allocations
//   declared to it by component accounts also to the pool; and refusal by the
//   pool is reported back to the component. In addition, allocations are
//   "buffered" to reduce pressure on the mutex of the shared pool.
//
// General use cases:
//
//   component1 -+- account1 ---\
//               |              |
//               \- account2 ---+--- monitor (standalone)
//                              |
//   component2 --- account3 ---/
//
//
// Client connection A:
//   component1 -+- account1 ---\
//               |              |
//               \- account2 ---+--- monitorA --\
//                              |               |
//   component2 --- account3 ---/               |
//                                              +---- pool (shared monitor)
// Client connection B:                         |
//   component1 -+- account1 ---\               |
//               |              |               |
//               \- account2 ---+--- monitorB --/
//                              |
//   component2 --- account3 ---/
//
//
// In CockroachDB this is integrated as follows:
//
// For the internal executor:
//
//   internal executor ------------------------------owns-- session --owns-- monitor (standalone)
//        |                                                   |
//        \--- (sql run by internal executor) -- (accounts) --/
//
// Every use of the internal executor talks to a monitor that is not
// connected to a pool and does not constrain allocations (it just
// performs tracking).
//
// For admin commands:
//
//   admin server ---------------------------------------------------owns-- pool1 (shared monitor)
//     |                                                                       |
//     +-- admin conn1 --owns------------------ session --owns-- monitor --\   |
//     |     |                                     |                       |   |
//     |     \-- (sql for conn) -- (accounts) -----/                       +---/
//     |                                                                   |
//     +-- admin conn2 --owns------------------ session --owns-- monitor --/
//           |                                     |
//           \-- (sql for conn) -- (accounts) -----/
//
// All admin endpoints have a monitor per connection, held by the SQL
// session object, and all admin monitors talk to a single pool in the
// adminServer. This pool is (currently) unconstrained; it merely
// serves to track global memory usage by admin commands.
//
// The reason why the monitor object is held by the session object and tracks
// allocation that may span the entire lifetime of the session is detailed
// in a comment in the Session struct (cf. session.go).
//
// For regular SQL client connections:
//
//   executor --------------------------------------------------------owns-- pool2 (shared monitor)
//                                                                             |
//   pgwire server ---------------------------------------owns-- monitor --\   |
//     |                                                           |       |   |
//     +-- conn1 -- base account-----------------------------------+       +---/
//     |     |                                                     |       |
//     |     |                                                    ```      |
//     |     |                                                             |
//     |     +-----owns------------------------ session --owns-- monitor --+
//     |     |                                     |                       |
//     |     \-- (sql for conn) -- (accounts) -----/              ...      |
//     |                                                           |       |
//     |                                                           |       |
//     +-- conn2 -- base account-----------------------------------/       |
//           |                                                             |
//           |                                                             |
//           +-----owns------------------------ session --owns-- monitor --/
//           |                                     |
//           \-- (sql for conn) -- (accounts) -----/
//
// This is similar to the situation with admin commands with two deviations:
//
// - in this use case the shared pool is constrained; the maximum is
//   configured to be 1/4 of RAM size by default, and can be
//   overridden from the command-line.
//
// - in addition to the per-connection monitors, the pgwire server
//   owns and uses an additional shared monitor. This is an
//   optimization: when a pgwire connection is opened, the server
//   pre-reserves some bytes (`baseSQLMemoryBudget`) using a
//   per-connection "base account" to the shared server monitor. By
//   doing so, the server capitalizes on the fact that a monitor
//   "buffers" allocations from the pool and thus each connection
//   receives a starter bytes budget without needing to hit the
//   shared pool and its mutex.
//
// Finally, a simplified API is provided in session_mem_usage.go
// (WrappedMemoryAccount) to simplify the interface offered to SQL components
// using accounts linked to the session-bound monitor.

// BytesMonitor defines an object that can track and limit memory/disk usage by
// other CockroachDB components. The monitor must be set up via Start/Stop
// before and after use.
// The various counters express sizes in bytes.
type BytesMonitor struct {
	mu struct {
		syncutil.Mutex

		// curAllocated tracks the current amount of bytes allocated at this
		// monitor by its client components.
		curAllocated int64

		// maxAllocated tracks the high water mark of allocations. Used for
		// monitoring.
		maxAllocated int64

		// curBudget represents the budget allocated at the pool on behalf of
		// this monitor.
		curBudget BoundAccount

		//  Both fields below are protected by the mutex because they might be
		//  updated after the monitor has been instantiated.

		// curBytesCount is the metric object used to track number of bytes
		// reserved by the monitor during its lifetime.
		curBytesCount *metric.Gauge

		// maxBytesHist is the metric object used to track the high watermark of bytes
		// allocated by the monitor during its lifetime.
		maxBytesHist metric.IHistogram
	}

	// name identifies this monitor in logging messages.
	name redact.RedactableString

	// resource specifies what kind of resource the monitor is tracking
	// allocations for. Specific behavior is delegated to this resource (e.g.
	// budget exceeded errors).
	resource Resource

	// reserved indicates how many bytes were already reserved for this
	// monitor before it was instantiated. Allocations registered to
	// this monitor are first deducted from this budget. If there is no
	// pool, reserved determines the maximum allocation capacity of this
	// monitor. The reserved bytes are released to their owner monitor
	// upon Stop.
	reserved *BoundAccount

	// limit specifies a hard limit on the number of bytes a monitor allows to
	// be allocated. Note that this limit will not be observed if allocations
	// hit constraints on the owner monitor. This is useful to limit allocations
	// when an owner monitor has a larger capacity than wanted but should still
	// keep track of allocations made through this monitor. Note that child
	// monitors are affected by this limit.
	limit int64

	// poolAllocationSize specifies the allocation unit for requests to the
	// pool.
	poolAllocationSize int64

	// relinquishAllOnReleaseBytes, if true, indicates that the monitor should
	// relinquish all bytes on releaseBytes() call.
	relinquishAllOnReleaseBytes bool

	// noteworthyUsageBytes is the size beyond which total allocations start to
	// become reported in the logs.
	noteworthyUsageBytes int64

	settings *cluster.Settings
}

// maxAllocatedButUnusedBlocks determines the maximum difference between the
// amount of bytes used by a monitor and the amount of bytes reserved at the
// upstream pool before the monitor relinquishes the bytes back to the pool.
// This is useful so that a monitor currently at the boundary of a block does
// not cause contention when accounts cause its allocation counter to grow and
// shrink slightly beyond and beneath an allocation block boundary. The
// difference is expressed as a number of blocks of size `poolAllocationSize`.
var maxAllocatedButUnusedBlocks = envutil.EnvOrDefaultInt("COCKROACH_MAX_ALLOCATED_UNUSED_BLOCKS", 10)

// DefaultPoolAllocationSize specifies the unit of allocation used by a monitor
// to reserve and release bytes to a pool.
var DefaultPoolAllocationSize = envutil.EnvOrDefaultInt64("COCKROACH_ALLOCATION_CHUNK_SIZE", 10*1024)

// NewMonitor creates a new monitor.
// Arguments:
//
//   - name is used to annotate log messages, can be used to distinguish
//     monitors.
//
//   - resource specifies what kind of resource the monitor is tracking
//     allocations for (e.g. memory or disk).
//
//   - curCount and maxHist are the metric objects to update with usage
//     statistics. Can be nil.
//
//   - increment is the block size used for upstream allocations from
//     the pool. Note: if set to 0 or lower, the default pool allocation
//     size is used.
//
//   - noteworthy determines the minimum total allocated size beyond
//     which the monitor starts to log increases. Use 0 to always log
//     or math.MaxInt64 to never log.
func NewMonitor(
	name redact.RedactableString,
	res Resource,
	curCount *metric.Gauge,
	maxHist metric.IHistogram,
	increment int64,
	noteworthy int64,
	settings *cluster.Settings,
) *BytesMonitor {
	return NewMonitorWithLimit(
		name, res, math.MaxInt64, curCount, maxHist, increment, noteworthy, settings)
}

// NewMonitorWithLimit creates a new monitor with a limit local to this
// monitor.
func NewMonitorWithLimit(
	name redact.RedactableString,
	res Resource,
	limit int64,
	curCount *metric.Gauge,
	maxHist metric.IHistogram,
	increment int64,
	noteworthy int64,
	settings *cluster.Settings,
) *BytesMonitor {
	if increment <= 0 {
		increment = DefaultPoolAllocationSize
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	m := &BytesMonitor{
		name:                 name,
		resource:             res,
		limit:                limit,
		noteworthyUsageBytes: noteworthy,
		poolAllocationSize:   increment,
		settings:             settings,
	}
	m.mu.curBytesCount = curCount
	m.mu.maxBytesHist = maxHist
	return m
}

// NewMonitorInheritWithLimit creates a new monitor with a limit local to this
// monitor with all other attributes inherited from the passed in monitor.
// Note on metrics and inherited monitors.
// When using pool to share resource, downstream monitors must not use the
// same metric objects as pool monitor to avoid reporting the same allocation
// multiple times. Downstream monitors should use their own metrics as needed
// by using BytesMonitor.SetMetrics function.
// Also note that because monitors pre-allocate resources from pool in chunks,
// those chunks would be reported as used by pool while downstream monitors will
// not.
func NewMonitorInheritWithLimit(
	name redact.RedactableString, limit int64, m *BytesMonitor,
) *BytesMonitor {
	m.mu.Lock()
	defer m.mu.Unlock()
	return NewMonitorWithLimit(
		name,
		m.resource,
		limit,
		nil, // curCount is not inherited as we don't want to double count allocations
		nil, // maxHist is not inherited as we don't want to double count allocations
		m.poolAllocationSize,
		m.noteworthyUsageBytes,
		m.settings,
	)
}

// noReserved is safe to be used by multiple monitors as the "reserved" account
// since only its 'used' field will ever be read.
var noReserved = BoundAccount{}

// StartNoReserved is the same as Start when there is no pre-reserved budget.
func (mm *BytesMonitor) StartNoReserved(ctx context.Context, pool *BytesMonitor) {
	mm.Start(ctx, pool, &noReserved)
}

// Start begins a monitoring region.
// Arguments:
//   - pool is the upstream monitor that provision allocations exceeding the
//     pre-reserved budget. If pool is nil, no upstream allocations are possible
//     and the pre-reserved budget determines the entire capacity of this monitor.
//
// - reserved is the pre-reserved budget (see above).
func (mm *BytesMonitor) Start(ctx context.Context, pool *BytesMonitor, reserved *BoundAccount) {
	if mm.mu.curAllocated != 0 {
		panic(fmt.Sprintf("%s: started with %d bytes left over", mm.name, mm.mu.curAllocated))
	}
	if mm.mu.curBudget.mon != nil {
		panic(fmt.Sprintf("%s: already started with pool %s", mm.name, mm.mu.curBudget.mon.name))
	}
	mm.mu.curAllocated = 0
	mm.mu.maxAllocated = 0
	mm.mu.curBudget = pool.MakeBoundAccount()
	mm.reserved = reserved
	if log.V(2) {
		poolname := redact.RedactableString("(none)")
		if pool != nil {
			poolname = pool.name
		}
		log.InfofDepth(ctx, 1, "%s: starting monitor, reserved %s, pool %s",
			mm.name,
			humanizeutil.IBytes(mm.reserved.used),
			poolname)
	}
}

// NewUnlimitedMonitor creates a new monitor and starts the monitor in
// "detached" mode without a pool and without a maximum budget.
func NewUnlimitedMonitor(
	ctx context.Context,
	name redact.RedactableString,
	res Resource,
	curCount *metric.Gauge,
	maxHist metric.IHistogram,
	noteworthy int64,
	settings *cluster.Settings,
) *BytesMonitor {
	if log.V(2) {
		log.InfofDepth(ctx, 1, "%s: starting unlimited monitor", name)

	}
	m := &BytesMonitor{
		name:                 name,
		resource:             res,
		limit:                math.MaxInt64,
		noteworthyUsageBytes: noteworthy,
		poolAllocationSize:   DefaultPoolAllocationSize,
		reserved:             NewStandaloneBudget(math.MaxInt64),
		settings:             settings,
	}
	m.mu.curBytesCount = curCount
	m.mu.maxBytesHist = maxHist
	return m
}

// EmergencyStop completes a monitoring region, and disables checking that all
// accounts have been closed. This is useful when recovering from panics so that
// we don't panic again.
func (mm *BytesMonitor) EmergencyStop(ctx context.Context) {
	mm.doStop(ctx, false)
}

// Stop completes a monitoring region.
func (mm *BytesMonitor) Stop(ctx context.Context) {
	mm.doStop(ctx, true)
}

// Name returns the name of the monitor.
func (mm *BytesMonitor) Name() string {
	return string(mm.name)
}

// Limit returns the memory limit of the monitor.
func (mm *BytesMonitor) Limit() int64 {
	return mm.limit
}

const bytesMaxUsageLoggingThreshold = 100 * 1024

func (mm *BytesMonitor) doStop(ctx context.Context, check bool) {
	// NB: No need to lock mm.mu here, when StopMonitor() is called the
	// monitor is not shared any more.
	if log.V(1) && mm.mu.maxAllocated >= bytesMaxUsageLoggingThreshold {
		log.InfofDepth(ctx, 1, "%s, bytes usage max %s",
			mm.name,
			humanizeutil.IBytes(mm.mu.maxAllocated))
	}

	if check && mm.mu.curAllocated != 0 {
		logcrash.ReportOrPanic(
			ctx, &mm.settings.SV,
			"%s: unexpected %d leftover bytes",
			mm.name, mm.mu.curAllocated)
		mm.releaseBytes(ctx, mm.mu.curAllocated)
	}

	mm.releaseBudget(ctx)

	if mm.mu.maxBytesHist != nil && mm.mu.maxAllocated > 0 {
		// TODO(knz): We record the logarithm because the UI doesn't know
		// how to do logarithmic y-axes yet. See the explanatory comments
		// in sql/mem_metrics.go.
		val := int64(1000 * math.Log(float64(mm.mu.maxAllocated)) / math.Ln10)
		mm.mu.maxBytesHist.RecordValue(val)
	}

	// Disable the pool for further allocations, so that further
	// uses outside of monitor control get errors.
	mm.mu.curBudget.mon = nil

	// Release the reserved budget to its original pool, if any.
	if mm.reserved != &noReserved {
		mm.reserved.Clear(ctx)
	}
}

// MaximumBytes returns the maximum number of bytes that were allocated by this
// monitor at one time since it was started.
func (mm *BytesMonitor) MaximumBytes() int64 {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return mm.mu.maxAllocated
}

// AllocBytes returns the current number of allocated bytes in this monitor.
func (mm *BytesMonitor) AllocBytes() int64 {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return mm.mu.curAllocated
}

// SetMetrics sets the metric objects for the monitor.
func (mm *BytesMonitor) SetMetrics(curCount *metric.Gauge, maxHist metric.IHistogram) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.mu.curBytesCount = curCount
	mm.mu.maxBytesHist = maxHist
}

// Resource returns the type of the resource the monitor is tracking.
func (mm *BytesMonitor) Resource() Resource {
	return mm.resource
}

// BoundAccount tracks the cumulated allocations for one client of a pool or
// monitor. BytesMonitor has an account to its pool; BytesMonitor clients have
// an account to the monitor. This allows each client to release all the bytes
// at once when it completes its work. Internally, BoundAccount amortizes
// allocations from whichever BoundAccount it is associated with by allocating
// additional memory and parceling it out (see BoundAccount.reserved). A nil
// BoundAccount acts as an unlimited account for which growing and shrinking are
// noops.
//
// See the comments in bytes_usage.go for a fuller picture of how these accounts
// are used in CockroachDB.
//
// A normal BoundAccount is not safe for concurrent use by multiple goroutines,
// however if the Mu field is set to a non-nil mutex, some methods such as Grow,
// Shrink, and Resize calls will lock and unlock that mutex making them safe;
// such methods are identified in their comments.
type BoundAccount struct {
	used int64
	// reserved is a small buffer to amortize the cost of growing an account. It
	// decreases as used increases (and vice-versa).
	reserved int64
	mon      *BytesMonitor

	earmark int64

	// Mu, if non-nil, is used in some methods such as Grow and Shrink.
	Mu *syncutil.Mutex
}

// NewStandaloneBudget creates a BoundAccount suitable for root monitors.
func NewStandaloneBudget(capacity int64) *BoundAccount {
	return &BoundAccount{used: capacity}
}

// Used returns the number of bytes currently allocated through this account.
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) Used() int64 {
	if b == nil {
		return 0
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	return b.used
}

// Monitor returns the BytesMonitor to which this account is bound. The return
// value can be nil.
func (b *BoundAccount) Monitor() *BytesMonitor {
	if b == nil {
		return nil
	}
	return b.mon
}

func (b *BoundAccount) allocated() int64 {
	if b == nil {
		return 0
	}
	return b.used + b.reserved
}

// MakeBoundAccount creates a BoundAccount connected to the given monitor.
func (mm *BytesMonitor) MakeBoundAccount() BoundAccount {
	return BoundAccount{mon: mm}
}

// TransferAccount creates a new account with the budget
// allocated in the given origAccount.
// The new account is owned by this monitor.
//
// If the operation succeeds, origAccount is released.
// If an error occurs, origAccount remains open and the caller
// remains responsible for closing / shrinking it.
func (mm *BytesMonitor) TransferAccount(
	ctx context.Context, origAccount *BoundAccount,
) (newAccount BoundAccount, err error) {
	b := mm.MakeBoundAccount()
	if err = b.Grow(ctx, origAccount.used); err != nil {
		return newAccount, err
	}
	origAccount.Close(ctx)
	return b, nil
}

// Init initializes a BoundAccount, connecting it to the given monitor. It is
// similar to MakeBoundAccount, but allows the caller to save a BoundAccount
// allocation.
func (b *BoundAccount) Init(ctx context.Context, mon *BytesMonitor) {
	if *b != (BoundAccount{}) {
		log.Fatalf(ctx, "trying to re-initialize non-empty account")
	}
	b.mon = mon
}

// Reserve requests an allocation of some amount from the monitor just like Grow
// but does not mark it as used immediately, instead keeping it in the local
// reservation by use by future Grow() calls, and configuring the account to
// consider that amount "earmarked" for this account, meaning that that Shrink()
// calls will not release it back to the parent monitor.
//
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) Reserve(ctx context.Context, x int64) error {
	if b == nil {
		return nil
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	minExtra := b.mon.roundSize(x)
	if err := b.mon.reserveBytes(ctx, minExtra); err != nil {
		return err
	}
	b.reserved += minExtra
	b.earmark += x
	return nil
}

// Empty shrinks the account to use 0 bytes. Previously used memory is returned
// to the reserved buffer, which is subsequently released such that at most
// poolAllocationSize is reserved.
func (b *BoundAccount) Empty(ctx context.Context) {
	if b == nil {
		return
	}
	b.reserved += b.used
	b.used = 0
	if b.reserved > b.mon.poolAllocationSize {
		b.mon.releaseBytes(ctx, b.reserved-b.mon.poolAllocationSize)
		b.reserved = b.mon.poolAllocationSize
	}
}

// Clear releases all the cumulated allocations of an account at once and
// primes it for reuse.
func (b *BoundAccount) Clear(ctx context.Context) {
	if b == nil {
		return
	}
	if b.mon == nil {
		// An account created by NewStandaloneBudget is disconnected from any
		// monitor -- "bytes out of the aether". This needs not be closed.
		return
	}
	b.Close(ctx)
	b.used = 0
	b.reserved = 0
}

// Close releases all the cumulated allocations of an account at once.
func (b *BoundAccount) Close(ctx context.Context) {
	if b == nil {
		return
	}
	if b.mon == nil {
		// An account created by NewStandaloneBudget is disconnected from any
		// monitor -- "bytes out of the aether". This needs not be closed.
		return
	}
	if a := b.allocated(); a > 0 {
		b.mon.releaseBytes(ctx, a)
	}
}

// Resize requests a size change for an object already registered in an
// account. The reservation is not modified if the new allocation is refused,
// so that the caller can keep using the original item without an accounting
// error. This is better than calling ClearAccount then GrowAccount because if
// the Clear succeeds and the Grow fails the original item becomes invisible
// from the perspective of the monitor.
//
// If one is interested in specifying the new size of the account as a whole (as
// opposed to resizing one object among many in the account), ResizeTo() should
// be used.
//
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) Resize(ctx context.Context, oldSz, newSz int64) error {
	if b == nil {
		return nil
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	delta := newSz - oldSz
	switch {
	case delta > 0:
		return b.Grow(ctx, delta)
	case delta < 0:
		b.Shrink(ctx, -delta)
	}
	return nil
}

// ResizeTo resizes (grows or shrinks) the account to a specified size.
//
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) ResizeTo(ctx context.Context, newSz int64) error {
	if b == nil {
		return nil
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	if newSz == b.used {
		// Performance optimization to avoid an unnecessary dispatch.
		return nil
	}
	return b.Resize(ctx, b.used, newSz)
}

// Grow is an accessor for b.mon.GrowAccount.
//
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) Grow(ctx context.Context, x int64) error {
	if b == nil {
		return nil
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	if b.reserved < x {
		minExtra := b.mon.roundSize(x - b.reserved)
		if err := b.mon.reserveBytes(ctx, minExtra); err != nil {
			return err
		}
		b.reserved += minExtra
	}
	b.reserved -= x
	b.used += x
	return nil
}

// Shrink releases part of the cumulated allocations by the specified size.
//
// If Mu is set, it is safe for use by concurrent goroutines.
func (b *BoundAccount) Shrink(ctx context.Context, delta int64) {
	if b == nil || delta == 0 {
		return
	}
	if b.Mu != nil {
		b.Mu.Lock()
		defer b.Mu.Unlock()
	}
	if b.used < delta {
		logcrash.ReportOrPanic(ctx, &b.mon.settings.SV,
			"%s: no bytes in account to release, current %d, free %d",
			b.mon.name, b.used, delta)
		delta = b.used
	}
	b.used -= delta
	b.reserved += delta
	if b.reserved > b.mon.poolAllocationSize && (b.earmark == 0 || b.used+b.mon.poolAllocationSize > b.earmark) {
		b.mon.releaseBytes(ctx, b.reserved-b.mon.poolAllocationSize)
		b.reserved = b.mon.poolAllocationSize
	}
}

// reserveBytes declares an allocation to this monitor. An error is returned if
// the allocation is denied.
// x must be a multiple of `poolAllocationSize`.
func (mm *BytesMonitor) reserveBytes(ctx context.Context, x int64) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	// Check the local limit first. NB: The condition is written in this manner
	// so that it handles overflow correctly. Consider what happens if
	// x==math.MaxInt64. mm.limit-x will be a large negative number.
	//
	// TODO(knz): make the monitor name reportable in telemetry, after checking
	// that the name is never constructed from user data.
	if mm.mu.curAllocated > mm.limit-x {
		return errors.Wrapf(
			mm.resource.NewBudgetExceededError(x, mm.mu.curAllocated, mm.limit), "%s", mm.name,
		)
	}
	// Check whether we need to request an increase of our budget.
	if mm.mu.curAllocated > mm.mu.curBudget.used+mm.reserved.used-x {
		if err := mm.increaseBudget(ctx, x); err != nil {
			return err
		}
	}
	mm.mu.curAllocated += x
	if mm.mu.curBytesCount != nil {
		mm.mu.curBytesCount.Inc(x)
	}
	if mm.mu.maxAllocated < mm.mu.curAllocated {
		mm.mu.maxAllocated = mm.mu.curAllocated
	}

	// Report "large" queries to the log for further investigation.
	if log.V(1) {
		if mm.mu.curAllocated > mm.noteworthyUsageBytes {
			// We only report changes in binary magnitude of the size. This is to
			// limit the amount of log messages when a size blowup is caused by
			// many small allocations.
			if bits.Len64(uint64(mm.mu.curAllocated)) != bits.Len64(uint64(mm.mu.curAllocated-x)) {
				log.Infof(ctx, "%s: bytes usage increases to %s (+%d)",
					mm.name,
					humanizeutil.IBytes(mm.mu.curAllocated), x)
			}
		}
	}

	if log.V(2) {
		// We avoid VEventf here because we want to avoid computing the
		// trace string if there is nothing to log.
		log.Infof(ctx, "%s: now at %d bytes (+%d) - %s",
			mm.name, mm.mu.curAllocated, x, util.GetSmallTrace(3))
	}
	return nil
}

// releaseBytes releases bytes previously successfully registered via
// reserveBytes().
func (mm *BytesMonitor) releaseBytes(ctx context.Context, sz int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.mu.curAllocated < sz {
		logcrash.ReportOrPanic(ctx, &mm.settings.SV,
			"%s: no bytes to release, current %d, free %d",
			mm.name, mm.mu.curAllocated, sz)
		sz = mm.mu.curAllocated
	}
	mm.mu.curAllocated -= sz
	if mm.mu.curBytesCount != nil {
		mm.mu.curBytesCount.Dec(sz)
	}
	mm.adjustBudget(ctx)

	if log.V(2) {
		// We avoid VEventf here because we want to avoid computing the
		// trace string if there is nothing to log.
		log.Infof(ctx, "%s: now at %d bytes (-%d) - %s",
			mm.name, mm.mu.curAllocated, sz, util.GetSmallTrace(5))
	}
}

// increaseBudget requests more bytes from the pool.
// minExtra must be a multiple of `poolAllocationSize`.
func (mm *BytesMonitor) increaseBudget(ctx context.Context, minExtra int64) error {
	// NB: mm.mu Already locked by reserveBytes().
	if mm.mu.curBudget.mon == nil {
		// TODO(knz): make the monitor name reportable in telemetry, after checking
		// that the name is never constructed from user data.
		return errors.Wrapf(mm.resource.NewBudgetExceededError(
			minExtra, mm.mu.curAllocated, mm.reserved.used), "%s", mm.name,
		)
	}
	if log.V(2) {
		log.Infof(ctx, "%s: requesting %d bytes from the pool", mm.name, minExtra)
	}

	return mm.mu.curBudget.Grow(ctx, minExtra)
}

// roundSize rounds its argument to the smallest greater or equal
// multiple of `poolAllocationSize`.
func (mm *BytesMonitor) roundSize(sz int64) int64 {
	const maxRoundSize = 4 << 20 // 4 MB
	if sz >= maxRoundSize {
		// Don't round the size up if the allocation is large. This also avoids
		// edge cases in the math below if sz == math.MaxInt64.
		return sz
	}
	chunks := (sz + mm.poolAllocationSize - 1) / mm.poolAllocationSize
	return chunks * mm.poolAllocationSize
}

// releaseBudget relinquishes all the monitor's allocated bytes back to the
// pool.
func (mm *BytesMonitor) releaseBudget(ctx context.Context) {
	// NB: mm.mu need not be locked here, as this is only called from StopMonitor().
	if log.V(2) {
		log.Infof(ctx, "%s: releasing %d bytes to the pool", mm.name, mm.mu.curBudget.allocated())
	}
	mm.mu.curBudget.Clear(ctx)
}

// RelinquishAllOnReleaseBytes makes it so that the monitor doesn't keep any
// margin bytes when the bytes are released from it.
func (mm *BytesMonitor) RelinquishAllOnReleaseBytes() {
	mm.relinquishAllOnReleaseBytes = true
}

// adjustBudget ensures that the monitor does not keep many more bytes reserved
// from the pool than it currently has allocated. Bytes are relinquished when
// there are at least maxAllocatedButUnusedBlocks*poolAllocationSize bytes
// reserved but unallocated (if relinquishAllOnReleaseBytes is false).
func (mm *BytesMonitor) adjustBudget(ctx context.Context) {
	// NB: mm.mu Already locked by releaseBytes().
	var margin int64
	if !mm.relinquishAllOnReleaseBytes {
		margin = mm.poolAllocationSize * int64(maxAllocatedButUnusedBlocks)
	}

	neededBytes := mm.mu.curAllocated
	if neededBytes <= mm.reserved.used {
		neededBytes = 0
	} else {
		neededBytes = mm.roundSize(neededBytes - mm.reserved.used)
	}
	if neededBytes <= mm.mu.curBudget.used-margin {
		mm.mu.curBudget.Shrink(ctx, mm.mu.curBudget.used-neededBytes)
	}
}

// ReadAll is like ioctx.ReadAll except it additionally asks the BoundAccount acct
// permission, if it is non-nil, it grows its buffer while reading. When the
// caller releases the returned slice it shrink the bound account by its cap.
func ReadAll(ctx context.Context, r ioctx.ReaderCtx, acct *BoundAccount) ([]byte, error) {
	if acct == nil {
		b, err := ioctx.ReadAll(ctx, r)
		return b, err
	}

	const starting, maxIncrease = 1024, 8 << 20
	if err := acct.Grow(ctx, starting); err != nil {
		return nil, err
	}

	b := make([]byte, 0, starting)

	for {
		// If we've filled our buffer, ask the monitor for more, up to its cap again
		// or max, whichever is less (so we double until we hit 8mb then grow by 8mb
		// each time thereafter), then alloc a new buffer that is that much bigger
		// and copy the existing buffer over.
		if len(b) == cap(b) {
			grow := cap(b)
			if grow > maxIncrease {
				// If we're realloc'ing at the max size it's probably worth checking if
				// we've been cancelled too.
				if err := ctx.Err(); err != nil {
					acct.Shrink(ctx, int64(cap(b)))
					return nil, err
				}
				grow = maxIncrease
			}
			if err := acct.Grow(ctx, int64(grow)); err != nil {
				// We were denied so release whatever we had before returning the error.
				acct.Shrink(ctx, int64(cap(b)))
				return nil, err
			}
			realloc := make([]byte, len(b), cap(b)+grow)
			copy(realloc, b)
			b = realloc
		}

		// Read into our buffer until we get an error.
		n, err := r.Read(ctx, b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}
