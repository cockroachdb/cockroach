// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mon

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
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

		// head is the head of the doubly-linked list of children of this
		// monitor.
		// NOTE: a child's mutex **cannot** be acquired while holding this
		// monitor's lock since it could lead to deadlocks. The main allocation
		// code path (reserveBytes() and releaseBytes()) might acquire the
		// parent's lock, so only locking "upwards" is allowed while keeping the
		// current monitor's lock.
		head *BytesMonitor

		// stopped indicates whether this monitor has been stopped.
		stopped bool

		// relinquishAllOnReleaseBytes, if true, indicates that the monitor should
		// relinquish all bytes on releaseBytes() call.
		// NB: this field doesn't need mutex protection but is inside of mu
		// struct in order to reduce the struct size.
		relinquishAllOnReleaseBytes bool

		// tracksDisk indicates whether this monitor is for tracking disk
		// resources.
		// NB: this field doesn't need mutex protection but is inside of mu
		// struct in order to reduce the struct size.
		tracksDisk bool

		// rootSQLMonitor indicates whether this monitor is the root SQL memory
		// monitor (in which case, memory budget exceeded errors should have a
		// hint to increase --max-sql-memory parameter).
		// NB: this field doesn't need mutex protection but is inside of mu
		// struct in order to reduce the struct size.
		rootSQLMonitor bool

		// longLiving indicates whether lifetime of this monitor matches the
		// server's life, and as such, this monitor is exempted from having to
		// be stopped when its ancestor monitor is stopped.
		// NB: this field doesn't need mutex protection but is inside of mu
		// struct in order to reduce the struct size.
		longLiving bool
	}

	// parentMu encompasses the fields that must be accessed while holding the
	// mutex of the parent monitor.
	parentMu struct {
		// prevSibling and nextSibling are references to the previous and the
		// next siblings of this monitor (i.e. the previous and the next nodes
		// in the doubly-linked list of children of the parent monitor).
		prevSibling, nextSibling *BytesMonitor
	}

	// name identifies this monitor in logging messages.
	name MonitorName

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
	//
	// limit is computed from configLimit, the parent monitor and the
	// reserved budget during Start().
	limit int64

	// configLimit is the limit configured when the monitor is created.
	configLimit int64

	// poolAllocationSize specifies the allocation unit for requests to the
	// pool.
	poolAllocationSize int64

	settings *cluster.Settings
}

// MonitorName is used to identify monitors in logging messages. It consists of
// a string name and an optional ID.
type MonitorName struct {
	name redact.SafeString
	id   uuid.Short
}

// MakeMonitorName constructs a MonitorName with the given name.
func MakeMonitorName(name redact.SafeString) MonitorName {
	return MonitorName{name: name}
}

// MakeMonitorNameWithID constructs a MonitorName with the given name and
// ID.
func MakeMonitorNameWithID(name redact.SafeString, id uuid.Short) MonitorName {
	return MonitorName{name: name, id: id}
}

// String returns the monitor name as a string.
func (mn MonitorName) String() string {
	return redact.StringWithoutMarkers(mn)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (mn MonitorName) SafeFormat(w redact.SafePrinter, r rune) {
	w.SafeString(mn.name)
	var nullShort uuid.Short
	if mn.id != nullShort {
		w.SafeString(redact.SafeString(mn.id.String()))
	}
}

const (
	// Consult with SQL Queries before increasing these values.
	expectedMonitorSize = 168
	expectedAccountSize = 24
)

func init() {
	monitorSize := unsafe.Sizeof(BytesMonitor{})
	if !util.RaceEnabled {
		if monitorSize != expectedMonitorSize {
			panic(errors.AssertionFailedf("expected monitor size to be %d, found %d", expectedMonitorSize, monitorSize))
		}
	}
	if accountSize := unsafe.Sizeof(BoundAccount{}); accountSize != expectedAccountSize {
		panic(errors.AssertionFailedf("expected account size to be %d, found %d", expectedAccountSize, accountSize))
	}
}

// enableMonitorTreeTrackingEnvVar indicates whether tracking of all children of
// a BytesMonitor (which is what powers TraverseTree) is enabled.
var enableMonitorTreeTrackingEnvVar = envutil.EnvOrDefaultBool(
	"COCKROACH_ENABLE_MONITOR_TREE", true)

// enableMonitorTreeTrackingSetting indicates whether tracking of all children
// of a BytesMonitor (which is what powers TraverseTree) is enabled.
var enableMonitorTreeTrackingSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"diagnostics.memory_monitor_tree.enabled",
	"enable tracking of memory monitor tree",
	true,
)

// MonitorState describes the current state of a single monitor.
type MonitorState struct {
	// Level tracks how many "generations" away the current monitor is from the
	// root.
	Level int
	// Name is the name of the monitor.
	Name MonitorName
	// ID is the "id" of the monitor (its address converted to int64).
	ID int64
	// ParentID is the "id" of the parent monitor (parent's address converted to
	// int64), or 0 if the monitor doesn't have a parent.
	ParentID int64
	// Used is amount of bytes currently used by the monitor (as reported by
	// curBudget.used). This doesn't include the usage registered with the
	// reserved account.
	Used int64
	// ReservedUsed is amount of bytes currently consumed from the reserved
	// account, or 0 if no reserved account was provided in Start.
	ReservedUsed int64
	// ReservedReserved is amount of bytes reserved in the reserved account, or
	// 0 if no reserved account was provided in Start.
	ReservedReserved int64
	// Stopped indicates whether the monitor has been stopped.
	Stopped bool
	// LongLiving indicates whether the monitor is a long-living one.
	LongLiving bool
}

// TraverseTree traverses the tree of monitors rooted in the BytesMonitor. The
// passed-in callback is called for each non-stopped monitor. If the callback
// returns an error, the traversal stops immediately.
//
// Note that this state can be inconsistent since a parent's state is recorded
// before its children without synchronization with children being stopped.
// Namely, the parent's MonitorState might include the state of the monitors
// that don't get the callback called on their MonitorState.
func (mm *BytesMonitor) TraverseTree(monitorStateCb func(MonitorState) error) error {
	return mm.traverseTree(0 /* level */, monitorStateCb)
}

// traverseTree recursively traverses the tree of monitors rooted in the current
// monitor.
func (mm *BytesMonitor) traverseTree(level int, monitorStateCb func(MonitorState) error) error {
	mm.mu.Lock()
	var reservedUsed, reservedReserved int64
	if mm.reserved != nil {
		reservedUsed = mm.reserved.used
		reservedReserved = mm.reserved.reserved
	}
	id := uintptr(unsafe.Pointer(mm))
	var parentID uintptr
	if parent := mm.mu.curBudget.mon; parent != nil {
		parentID = uintptr(unsafe.Pointer(parent))
	}
	monitorState := MonitorState{
		Level:            level,
		Name:             mm.name,
		ID:               int64(id),
		ParentID:         int64(parentID),
		Used:             mm.mu.curAllocated,
		ReservedUsed:     reservedUsed,
		ReservedReserved: reservedReserved,
		Stopped:          mm.mu.stopped,
		LongLiving:       mm.mu.longLiving,
	}
	// Note that we cannot call traverseTree on the children while holding mm's
	// lock since it could lead to deadlocks. Instead, we store all children as
	// of right now, and then export them after unlocking ourselves.
	//
	//gcassert:noescape
	var childrenAlloc [8]*BytesMonitor
	children := childrenAlloc[:0]
	for c := mm.mu.head; c != nil; c = c.parentMu.nextSibling {
		children = append(children, c)
	}
	mm.mu.Unlock()
	if err := monitorStateCb(monitorState); err != nil {
		return err
	}
	for _, c := range children {
		if err := c.traverseTree(level+1, monitorStateCb); err != nil {
			return err
		}
	}
	return nil
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

// Options encompasses all arguments to the NewMonitor and NewUnlimitedMonitor
// calls. If a particular option is not set, then a reasonable default will be
// used.
type Options struct {
	// Name is used to annotate log messages, can be used to distinguish
	// monitors.
	Name MonitorName
	// Res specifies what kind of resource the monitor is tracking allocations
	// for (e.g. memory or disk). If unset, MemoryResource is assumed.
	Res   Resource
	Limit int64
	// CurCount and MaxHist are the metric objects to update with usage
	// statistics.
	CurCount *metric.Gauge
	MaxHist  metric.IHistogram
	// Increment is the block size used for upstream allocations from the pool.
	Increment  int64
	Settings   *cluster.Settings
	LongLiving bool
}

// NewMonitor creates a new monitor.
func NewMonitor(args Options) *BytesMonitor {
	if args.Limit <= 0 {
		args.Limit = math.MaxInt64
	}
	if args.Increment <= 0 {
		args.Increment = DefaultPoolAllocationSize
	}
	m := &BytesMonitor{
		name:               args.Name,
		configLimit:        args.Limit,
		limit:              args.Limit,
		poolAllocationSize: args.Increment,
		settings:           args.Settings,
	}
	m.mu.curBytesCount = args.CurCount
	m.mu.maxBytesHist = args.MaxHist
	m.mu.tracksDisk = args.Res == DiskResource
	m.mu.longLiving = args.LongLiving
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
	name redact.SafeString, limit int64, m *BytesMonitor, longLiving bool,
) *BytesMonitor {
	res := MemoryResource
	if m.mu.tracksDisk {
		res = DiskResource
	}
	return NewMonitor(Options{
		Name:       MakeMonitorName(name),
		Res:        res,
		Limit:      limit,
		CurCount:   nil, // CurCount is not inherited as we don't want to double count allocations
		MaxHist:    nil, // MaxHist is not inherited as we don't want to double count allocations
		Increment:  m.poolAllocationSize,
		Settings:   m.settings,
		LongLiving: longLiving,
	})
}

func (mm *BytesMonitor) MarkAsRootSQLMonitor() {
	if mm.mu.tracksDisk {
		panic(errors.AssertionFailedf("root SQL memory monitor cannot track disk resources"))
	}
	mm.mu.rootSQLMonitor = true
}

// noReserved is safe to be used by multiple monitors as the "reserved" account
// since only its 'used' and 'reserved' fields will ever be read.
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
		panic(errors.AssertionFailedf("%s: started with %d bytes left over", mm.name, mm.mu.curAllocated))
	}
	if mm.mu.curBudget.mon != nil {
		panic(errors.AssertionFailedf("%s: already started with pool %s", mm.name, mm.mu.curBudget.mon.name))
	}
	mm.mu.curAllocated = 0
	mm.mu.maxAllocated = 0
	mm.mu.curBudget = pool.MakeBoundAccount()
	mm.mu.stopped = false
	mm.reserved = reserved
	if log.V(2) {
		poolname := redact.SafeString("(none)")
		if pool != nil {
			poolname = redact.SafeString(pool.name.String())
		}
		log.InfofDepth(ctx, 1, "%s: starting monitor, reserved %s, pool %s",
			mm.name,
			humanizeutil.IBytes(mm.reserved.used),
			poolname)
	}

	var effectiveLimit int64
	if pool != nil {
		// mm.settings can be nil in tests in which case we use the default
		// value of enableMonitorTreeTrackingSetting cluster setting (true).
		if enableMonitorTreeTrackingEnvVar && (mm.settings == nil || enableMonitorTreeTrackingSetting.Get(&mm.settings.SV)) {
			// If we have a "parent" monitor, then register mm as its child by
			// making it the head of the doubly-linked list.
			func() {
				pool.mu.Lock()
				defer pool.mu.Unlock()
				if s := pool.mu.head; s != nil {
					s.parentMu.prevSibling = mm
					mm.parentMu.nextSibling = s
				}
				pool.mu.head = mm
			}()
		}
		effectiveLimit = pool.limit
	}

	if reserved != nil {
		// In addition to the limit of the parent monitor, we can also
		// allocate from our reserved budget.
		// We do need to take care of overflows though.
		if effectiveLimit < math.MaxInt64-reserved.used {
			effectiveLimit += reserved.used
		} else {
			effectiveLimit = math.MaxInt64
		}
	}

	if effectiveLimit > mm.configLimit {
		effectiveLimit = mm.configLimit
	}
	mm.limit = effectiveLimit
}

// NewUnlimitedMonitor creates a new monitor and starts the monitor in
// "detached" mode without a pool and without a maximum budget.
func NewUnlimitedMonitor(ctx context.Context, args Options) *BytesMonitor {
	if log.V(2) {
		log.InfofDepth(ctx, 1, "%s: starting unlimited monitor", args.Name)
	}
	// The limit is expected to not be set, but let's be conservative.
	args.Limit = math.MaxInt64
	m := NewMonitor(args)
	m.reserved = NewStandaloneBudget(math.MaxInt64)
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
func (mm *BytesMonitor) Name() MonitorName {
	return mm.name
}

// Limit returns the memory limit of the monitor.
func (mm *BytesMonitor) Limit() int64 {
	return mm.limit
}

// MarkLongLiving marks the monitor as a long-living. Such monitors are allowed
// to not be stopped because their lifetime matches the server's lifetime.
func (mm *BytesMonitor) MarkLongLiving() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.mu.longLiving = true
}

func findShortLivingCb(f io.Writer, numShortLiving *int) func(state MonitorState) error {
	return func(s MonitorState) error {
		if s.LongLiving {
			return nil
		}
		*numShortLiving++
		info := fmt.Sprintf("%s%s %s", strings.Repeat(" ", 4*s.Level), s.Name, humanize.IBytes(uint64(s.Used)))
		if s.ReservedUsed != 0 || s.ReservedReserved != 0 {
			info += fmt.Sprintf(" (%s / %s)", humanize.IBytes(uint64(s.ReservedUsed)), humanize.IBytes(uint64(s.ReservedReserved)))
		}
		if _, err := f.Write([]byte(info)); err != nil {
			return err
		}
		_, err := f.Write([]byte{'\n'})
		return err
	}
}

const bytesMaxUsageLoggingThreshold = 100 * 1024

func (mm *BytesMonitor) doStop(ctx context.Context, check bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.mu.stopped = true
	if buildutil.CrdbTestBuild {
		// We expect that all short-living descendants of this monitor have been
		// stopped.
		if mm.mu.head != nil {
			mm.mu.Unlock()
			var sb strings.Builder
			var numShortLiving int
			_ = mm.TraverseTree(findShortLivingCb(&sb, &numShortLiving))
			mm.mu.Lock()
			if !mm.mu.longLiving {
				// Ignore mm itself if it is short-living.
				numShortLiving--
			}
			if numShortLiving > 0 {
				panic(errors.AssertionFailedf(
					"found %d short-living non-stopped monitors in %s\n%s",
					numShortLiving, mm.name, sb.String(),
				))
			}
		}
	}

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
		mm.releaseBytesLocked(ctx, mm.mu.curAllocated)
	}

	mm.releaseBudget(ctx)

	if mm.mu.maxBytesHist != nil && mm.mu.maxAllocated > 0 {
		// TODO(knz): We record the logarithm because the UI doesn't know
		// how to do logarithmic y-axes yet. See the explanatory comments
		// in sql/mem_metrics.go.
		val := int64(1000 * math.Log(float64(mm.mu.maxAllocated)) / math.Ln10)
		mm.mu.maxBytesHist.RecordValue(val)
	}

	if parent := mm.mu.curBudget.mon; parent != nil {
		// If we have a "parent" monitor, then unregister mm from the list of
		// the parent's children.
		func() {
			parent.mu.Lock()
			defer parent.mu.Unlock()
			prev, next := mm.parentMu.prevSibling, mm.parentMu.nextSibling
			if parent.mu.head == mm {
				parent.mu.head = next
			}
			if prev != nil {
				prev.parentMu.nextSibling = next
			}
			if next != nil {
				next.parentMu.prevSibling = prev
			}
			// Lose the references to siblings to aid GC.
			mm.parentMu.prevSibling, mm.parentMu.nextSibling = nil, nil
		}()
	}
	// If this monitor still has children, let's lose the reference to them as
	// well as break the references between them to aid GC.
	if mm.mu.head != nil {
		nextChild := mm.mu.head
		mm.mu.head = nil
		for nextChild != nil {
			next := nextChild.parentMu.nextSibling
			nextChild.parentMu.prevSibling = nil
			nextChild.parentMu.nextSibling = nil
			nextChild = next
		}
	}

	// Disable the pool for further allocations, so that further
	// uses outside of monitor control get errors.
	mm.mu.curBudget.mon = nil

	// Release the reserved budget to its original pool, if any.
	if mm.reserved != &noReserved && mm.reserved != nil {
		mm.reserved.Clear(ctx)
		// Make sure to lose reference to the reserved account because it has a
		// pointer to the parent monitor.
		mm.reserved = &noReserved
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
	if mm.mu.tracksDisk {
		return DiskResource
	}
	return MemoryResource
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
// use ConcurrentBoundAccount if thread safety is needed.
type BoundAccount struct {
	used int64
	// reserved is a small buffer to amortize the cost of growing an account. It
	// decreases as used increases (and vice-versa).
	reserved int64
	mon      *BytesMonitor
}

// EarmarkedBoundAccount extends BoundAccount to pre-reserve large allocations
// up front.
type EarmarkedBoundAccount struct {
	BoundAccount
	earmark int64
}

// ConcurrentBoundAccount is a thread safe wrapper around BoundAccount.
// TODO(yuzefovich): add assertions that ConcurrentBoundAccount is non-nil.
type ConcurrentBoundAccount struct {
	syncutil.Mutex
	wrapped BoundAccount
}

// Used wraps BoundAccount.Used().
func (c *ConcurrentBoundAccount) Used() int64 {
	if c == nil {
		return 0
	}
	c.Lock()
	defer c.Unlock()
	return c.wrapped.Used()
}

// Close wraps BoundAccount.Close().
func (c *ConcurrentBoundAccount) Close(ctx context.Context) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.wrapped.Close(ctx)
}

// Resize wraps BoundAccount.Resize().
func (c *ConcurrentBoundAccount) Resize(ctx context.Context, oldSz, newSz int64) error {
	if c == nil {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	return c.wrapped.Resize(ctx, oldSz, newSz)
}

// ResizeTo wraps BoundAccount.ResizeTo().
func (c *ConcurrentBoundAccount) ResizeTo(ctx context.Context, newSz int64) error {
	if c == nil {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	return c.wrapped.ResizeTo(ctx, newSz)
}

// Grow wraps BoundAccount.Grow().
func (c *ConcurrentBoundAccount) Grow(ctx context.Context, x int64) error {
	if c == nil {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	return c.wrapped.Grow(ctx, x)
}

// Shrink wraps BoundAccount.Shrink().
func (c *ConcurrentBoundAccount) Shrink(ctx context.Context, delta int64) {
	if c == nil || delta == 0 {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.wrapped.Shrink(ctx, delta)
}

// Clear wraps BoundAccount.Clear()
func (c *ConcurrentBoundAccount) Clear(ctx context.Context) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.wrapped.Clear(ctx)
}

// NewStandaloneBudget creates a BoundAccount suitable for root monitors.
func NewStandaloneBudget(capacity int64) *BoundAccount {
	return &BoundAccount{used: capacity}
}

// standaloneUnlimited is a special "marker" BytesMonitor that is used by
// standalone unlimited accounts.
var standaloneUnlimited = &BytesMonitor{}

// NewStandaloneUnlimitedAccount returns a BoundAccount that is actually not
// bound to any BytesMonitor. Use this only when memory allocations shouldn't
// be tracked by the memory accounting system.
func NewStandaloneUnlimitedAccount() *BoundAccount {
	return &BoundAccount{mon: standaloneUnlimited}
}

// standaloneUnlimited returns whether this BoundAccount is actually not bound
// to any BytesMonitor and acts as a "standalone unlimited" one.
func (b *BoundAccount) standaloneUnlimited() bool {
	return b.mon == standaloneUnlimited
}

// Used returns the number of bytes currently allocated through this account.
func (b *BoundAccount) Used() int64 {
	// TODO(yuzefovich): remove nil checks altogether once we've had some baking
	// time with test-only assertions.
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return 0
	}
	return b.used
}

// Monitor returns the BytesMonitor to which this account is bound. The return
// value can be nil.
func (b *BoundAccount) Monitor() *BytesMonitor {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return nil
	}
	if b.standaloneUnlimited() {
		// We don't want to expose access to the standaloneUnlimited monitor.
		return nil
	}
	return b.mon
}

func (b *BoundAccount) Allocated() int64 {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return 0
	}
	return b.used + b.reserved
}

// MakeBoundAccount creates a BoundAccount connected to the given monitor.
func (mm *BytesMonitor) MakeBoundAccount() BoundAccount {
	return BoundAccount{mon: mm}
}

// MakeEarmarkedBoundAccount creates an EarmarkedBoundAccount connected to the
// given monitor.
func (mm *BytesMonitor) MakeEarmarkedBoundAccount() EarmarkedBoundAccount {
	return EarmarkedBoundAccount{BoundAccount: BoundAccount{mon: mm}}
}

// MakeConcurrentBoundAccount creates ConcurrentBoundAccount, which is a thread
// safe wrapper around BoundAccount.
func (mm *BytesMonitor) MakeConcurrentBoundAccount() *ConcurrentBoundAccount {
	return &ConcurrentBoundAccount{wrapped: mm.MakeBoundAccount()}
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
	origAccount.Clear(ctx)
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

// Empty shrinks the account to use 0 bytes. Previously used memory is returned
// to the reserved buffer, which is subsequently released such that at most
// poolAllocationSize is reserved.
func (b *BoundAccount) Empty(ctx context.Context) {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return
	}
	if b.standaloneUnlimited() {
		b.used = 0
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
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return
	}
	// It's ok to call Close even if b.mon is nil or is the standaloneUnlimited
	// one.
	b.Close(ctx)
	b.used = 0
	b.reserved = 0
}

// Close releases all the cumulated allocations of an account at once.
// TODO(yuzefovich): consider removing this method in favor of Clear.
func (b *BoundAccount) Close(ctx context.Context) {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return
	}
	if b.mon == nil || b.standaloneUnlimited() {
		// Either an account created by NewStandaloneBudget or by
		// NewStandaloneUnlimited. In both cases it is disconnected from any
		// monitor -- "bytes out of the aether", so there is nothing to release.
		return
	}
	if a := b.Allocated(); a > 0 {
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
func (b *BoundAccount) Resize(ctx context.Context, oldSz, newSz int64) error {
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
func (b *BoundAccount) ResizeTo(ctx context.Context, newSz int64) error {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return nil
	}
	if newSz == b.used {
		// Performance optimization to avoid an unnecessary dispatch.
		return nil
	}
	return b.Resize(ctx, b.used, newSz)
}

// Grow is an accessor for b.mon.GrowAccount.
func (b *BoundAccount) Grow(ctx context.Context, x int64) error {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return nil
	}
	if b.standaloneUnlimited() {
		b.used += x
		return nil
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
func (b *BoundAccount) Shrink(ctx context.Context, delta int64) {
	if delta == 0 {
		return
	}
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return
	}
	if b.standaloneUnlimited() {
		if b.used < delta {
			logcrash.ReportOrPanic(ctx, nil, /* sv */
				"standalone unlimited: no bytes in account to release, current %d, free %d",
				b.used, delta)
			delta = b.used
		}
		b.used -= delta
		return
	}
	if b.used < delta {
		logcrash.ReportOrPanic(ctx, &b.mon.settings.SV,
			"%s: no bytes in account to release, current %d, free %d",
			b.mon.name, b.used, delta)
		delta = b.used
	}
	b.used -= delta
	b.reserved += delta
	if b.reserved > b.mon.poolAllocationSize {
		b.mon.releaseBytes(ctx, b.reserved-b.mon.poolAllocationSize)
		b.reserved = b.mon.poolAllocationSize
	}
}

// Reserve requests an allocation of some amount from the monitor just like Grow
// but does not mark it as used immediately, instead keeping it in the local
// reservation by use by future Grow() calls, and configuring the account to
// consider that amount "earmarked" for this account, meaning that that Shrink()
// calls will not release it back to the parent monitor.
func (b *EarmarkedBoundAccount) Reserve(ctx context.Context, x int64) error {
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return nil
	}
	minExtra := b.mon.roundSize(x)
	if err := b.mon.reserveBytes(ctx, minExtra); err != nil {
		return err
	}
	b.reserved += minExtra
	b.earmark += x
	return nil
}

// Shrink releases part of the cumulated allocations by the specified size.
func (b *EarmarkedBoundAccount) Shrink(ctx context.Context, delta int64) {
	if delta == 0 {
		return
	}
	if b == nil {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("uninitialized account"))
		}
		return
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

func (mm *BytesMonitor) makeBudgetExceededError(minExtra int64) error {
	errConstructor := NewMemoryBudgetExceededError
	if mm.mu.tracksDisk {
		errConstructor = newDiskBudgetExceededError
	}
	if mm.mu.rootSQLMonitor {
		errConstructor = newRootSQLMemoryMonitorBudgetExceededError
	}
	return errors.Wrapf(errConstructor(
		minExtra, mm.mu.curAllocated, mm.reserved.used), "%s", mm.name,
	)
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
	if mm.mu.curAllocated > mm.limit-x {
		return mm.makeBudgetExceededError(x)
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
	mm.releaseBytesLocked(ctx, sz)
}

// releaseBytesLocked is similar to releaseBytes but requires that mm.mu has
// already been locked.
func (mm *BytesMonitor) releaseBytesLocked(ctx context.Context, sz int64) {
	mm.mu.AssertHeld()
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
		return mm.makeBudgetExceededError(minExtra)
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
		log.Infof(ctx, "%s: releasing %d bytes to the pool", mm.name, mm.mu.curBudget.Allocated())
	}
	mm.mu.curBudget.Clear(ctx)
}

// RelinquishAllOnReleaseBytes makes it so that the monitor doesn't keep any
// margin bytes when the bytes are released from it.
func (mm *BytesMonitor) RelinquishAllOnReleaseBytes() {
	mm.mu.relinquishAllOnReleaseBytes = true
}

// adjustBudget ensures that the monitor does not keep many more bytes reserved
// from the pool than it currently has allocated. Bytes are relinquished when
// there are at least maxAllocatedButUnusedBlocks*poolAllocationSize bytes
// reserved but unallocated (if relinquishAllOnReleaseBytes is false).
func (mm *BytesMonitor) adjustBudget(ctx context.Context) {
	// NB: mm.mu Already locked by releaseBytes().
	var margin int64
	if !mm.mu.relinquishAllOnReleaseBytes {
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

// ReadAll is like ioctx.ReadAll except it additionally asks the BoundAccount
// acct permission if it grows its buffer while reading. When the caller
// releases the returned slice, it shrinks the bound account by its cap (unless
// it provided a standalone unlimited account).
func ReadAll(ctx context.Context, r ioctx.ReaderCtx, acct *BoundAccount) ([]byte, error) {
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
