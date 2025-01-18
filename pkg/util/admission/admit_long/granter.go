// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admit_long

import (
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// If this is set to 0, there is no resource aware granting, and granting is
// capped by AllowedWithoutPermission and the values returned by
// WorkRequester.GetAllowedWithoutPermissionForStore.
var utilizationThreshold = settings.RegisterFloatSetting(settings.SystemOnly,
	"admit_long.granter.utilization_threshold", "", 0.6)

// *AllowedWithoutPermission* controls the number of work items that can be
// run for each category without considering resource usage -- this is the
// node-level constraint corresponding to the store-level
// WorkRequester.GetAllowedWithoutPermissionForStore.

// Multiplier of GOMAXPROCS (and will be rounded up). A value of 0 means
// unlimited.
var compactionsAllowedWithoutPermissionMultiplier = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admit_long.compactions.allowed_without_permission.multiplier", "", 0)

// Multiplier of GOMAXPROCS (and will be rounded up). A value of 0 means
// unlimited.
var receiveSnapshotAllowedWithoutPermissionMultiplier = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admit_long.snapshot_recv.allowed_without_permission.multiplier", "", 0)

type WorkCategory uint8

const (
	PebbleCompaction WorkCategory = iota
	SnapshotRecv
	numWorkCategories
)

type WorkCategoryAndStore struct {
	Category WorkCategory
	Store    roachpb.StoreID
}

// WorkScore is used to order long-lived work.
//
// Ordering is (Optional false < true, Category PebbleCompaction <
// SnapshotRecv, Score >). Earlier in the ordering is more important.
type WorkScore struct {
	Optional bool
	Score    float64
}

// workID is internally used for tracking each work.
type workID uint64

// Locking: WorkRequester mutexes are ordered before WorkGranter mutexes. We
// need to specify some mutex ordering since WorkRequester and WorkGranter
// call into each other. This ordering choice is made to simplify the
// implementation of WorkRequesters, since there are many implementations of
// WorkRequester, and some are quite sophisticated (e.g. Pebble DB, for
// compactions). In contrast, there is only one non-trivial implementation of
// WorkGranter (workGranterImpl). There are three exceptions to this:
// WorkRequester.GetAllowedWithoutPermissionForStore,
// WorkGranter.UnregisterRequester, WorkResourceUsageReporter.WorkDone -- see
// those declarations for details.
//
// Note that this lock ordering choice is exact opposite of what was made in
// admission.{granter,requester}. In that case, there is only one non-trivial
// implementation of requester (WorkQueue), and maintaining high concurrency
// is very important (since the work items can be very tiny). That choice also
// introduced a race condition (see the comment in work_queue.go starting with
// "There is a race here: before q.mu is acquired, the granter could"), which
// is harmless since GrantCoordinator.tryGrantLocked runs periodically at a
// high frequency. Such a race can't be tolerated given the diversity of
// WorkGranter implementations -- specifically, implementations like
// multiqueue.ConcurrencyLimitGranter do not run periodically
// (ConcurrencyLimitGranter implements multiqueue.Granter which is a
// simplified interface akin to WorkGranter).

// WorkRequester is the interface implemented by various entities that want to
// interact the WorkGranter. See the list in the WorkCategory enum. A
// WorkRequester is deemed to be waiting by the WorkGranter, if the last time
// it interacted with WorkRequester was via a WorkRequester.{GetScore,Grant}
// that returned true, or a WorkGranter.TryGet that returned false. Note that
// WorkRequester.{GetScore,Grant} will only be called on a WorkRequester that
// is considered waiting, so the initial transition from waiting=false =>
// waiting=true is done by WorkGranter.TryGet returning false. Once the
// transition happens, the WorkRequester stays in waiting=true state as long
// as WorkRequester.{GetScore,Grant} calls return true. If a
// WorkRequester.{GetScore,Grant} calls returns false, the state transitions
// to waiting=false.
type WorkRequester interface {
	// GetAllowedWithoutPermissionForStore returns what is permitted at the
	// store level for this requester. It does not include node-level
	// constraints that may be known and applied by the WorkGranter. The return
	// value can change over time. For example, for PebbleCompactions, the
	// return value can change based on the compaction backlog. This method must
	// not acquire any mutex in WorkRequester that is covered by the general
	// mutex ordering rule stated earlier.
	//
	// The return value must be > 0.
	GetAllowedWithoutPermissionForStore() int
	// GetScore returns true if there is waiting work, and the WorkScore for
	// that work. This method should typically be efficient, since it will be
	// called for every WorkRequester when there is capacity available, and the
	// WorkScores compared. Note that the WorkGranter does not remember the
	// WorkScores, since if a WorkRequester is not picked, the WorkScore could
	// become stale by the time there is another opportunity to run something.
	// If there is efficiency to be gained by remembering the WorkScore, that
	// should be done by the WorkRequester, since it knows its own internal
	// details and knows when to invalidate this memory.
	GetScore() (bool, WorkScore)
	// Grant is used to grant permission to run one work item. The WorkRequester
	// returns true iff it accepts the grant, in which case it must exercise the
	// reporter.
	Grant(reporter WorkResourceUsageReporter) bool
}

// WorkGranter is used for scheduling long-lived work. WorkGranter may impose
// fixed node-level constraints on concurrency. WorkGranter may also be
// resource aware and permit flexible amount of concurrency based on available
// resources.
type WorkGranter interface {
	RegisterStatsProvider(
		provider ResourceStatsProvider,
		registry *metric.Registry,
		storeRegistries map[roachpb.StoreID]*metric.Registry)
	// RegisterRequester is called to register a requester and to specify the
	// number of goroutines that consume CPU in each work item (see the CPU
	// reporting interface in WorkResourceUsageReporter). Must be called exactly
	// once for a WorkCategoryAndStore.
	RegisterRequester(kind WorkCategoryAndStore, req WorkRequester, numGoroutinesPerWork int)
	// UnregisterRequester is used to unregister a requester.
	// UnregisterRequester waits until all potentially ongoing calls to
	// WorkRequester are finished, so UnregisterRequester must not be called
	// while holding locks mentioned earlier in the general mutex ordering rule.
	UnregisterRequester(kind WorkCategoryAndStore)
	// TryGet is called by a WorkRequester when it wants to run a work item. The
	// bool return value is true iff permission is granted, and in that case the
	// WorkResourceUsageReporter must be exercised the requester. If TryGet
	// returns false, the caller must ensure that in the same critical section
	// it adjusts its state so that a subsequent call to GetScore is guaranteed
	// to return true. See the earlier discussion about "waiting" on why this is
	// important -- violating this can cause waiting=false in the WorkGranter
	// while the WorkRequester believes it is waiting.
	TryGet(kind WorkCategoryAndStore) (bool, WorkResourceUsageReporter)
}

type ResourceStats struct {
	ProvisionedRate int64
	// Cumulative.
	Used int64
}

type StatsForResources struct {
	CPU  ResourceStats
	Disk map[roachpb.StoreID]ResourceStats
}

type ResourceStatsProvider interface {
	GetStats(scratch map[roachpb.StoreID]ResourceStats) StatsForResources
}

// PhysicalResourceValues captures resource usage. The units are not specified
// since depending on the context, the units can be different. For example,
// CPU can be the CPU rate (nanos/s) or the cumulative CPU usage (nanos) or
// the CPU usage over some interval (nanos).
type PhysicalResourceValues struct {
	CPU       int64
	DiskWrite map[roachpb.StoreID]int64
}

// workResourceUsageRate captures the usage rate for a work item. Units are in
// nanos/s and bytes/s respectively.
type workResourceUsageRate [numResources]int64

// workInfo captures information about a work item.
type workInfo struct {
	id            workID
	kind          WorkCategoryAndStore
	usageReporter internalUsageReporter
	// Already observed usage. It is updated periodically at an interval.
	usage resourceUsageByWork
	// rateEstimate is used to immediately subtract from the forecast when the
	// work is done.
	//
	// TODO: this should be called forecast too.
	rateEstimate       workResourceUsageRate
	estimateStartNanos int64
}

type requesterInfo struct {
	// Immutable state.
	kind WorkCategoryAndStore
	WorkRequester
	numGoroutinesPerWork int

	// Mutable state.

	// granted is the count of ongoing work items.
	granted int
	waiting waitingState
}

// waitingState is simply an optimization to avoid calling GetScore, which
// requires releasing and reacquiring workGranterImpl.mu. We think it is
// worthwhile when the number of stores is large.
type waitingState struct {
	value bool
	// tryGetCount is used to ensure transitions to false are not stale. It is
	// harmless to leave value as true, if the transition to false is suspect,
	// since we can always to do it later. But a spurious transition to false is
	// unsafe.
	tryGetCount int
}

func (r *requesterInfo) getWaitingState() waitingState {
	return r.waiting
}

func (r *requesterInfo) trySetWaitingToFalse(cur waitingState) (updated bool) {
	if cur.tryGetCount < r.waiting.tryGetCount {
		return false
	}
	r.waiting.value = false
	return true
}

func (r *requesterInfo) setWaitingToTrue() {
	r.waiting.value = true
	r.waiting.tryGetCount++
}

type cumulativeResourceStats struct {
	observed      int64
	estimated     int64
	workWallNanos int64
}

type statsForCategory struct {
	cpu       cumulativeResourceStats
	diskWrite map[roachpb.StoreID]*cumulativeResourceStats
}

type stats struct {
	category        [numWorkCategories]statsForCategory
	cumCPUForecast  int64
	cumDiskForecast map[roachpb.StoreID]int64
}

var (
	observedMeta = metric.Metadata{
		Name:        "admit_long.%s.%s.observed",
		Help:        "Observed %s %s",
		Measurement: "%s",
	}
	estimatedMeta = metric.Metadata{
		Name:        "admit_long.%s.%s.estimated",
		Help:        "Estimated %s %s",
		Measurement: "%s",
	}
	workWallMeta = metric.Metadata{
		Name:        "admit_long.%s.work_wall_time",
		Help:        "Observed %s work wall time",
		Measurement: "%s",
	}
	forecastMeta = metric.Metadata{
		Name:        "admit_long.%s.forecast",
		Help:        "Forecast %s",
		Measurement: "%s",
	}
)

func makeCategoryMetrics(
	cat string, name string, measurement string, unit metric.Unit,
) *categoryMetrics {
	fixMeta := func(meta metric.Metadata) metric.Metadata {
		meta.Name = fmt.Sprintf(meta.Name, cat, name)
		meta.Help = fmt.Sprintf(meta.Help, cat, name)
		meta.Measurement = fmt.Sprintf(meta.Measurement, measurement)
		meta.Unit = unit
		return meta
	}
	fixWallMeta := func(meta metric.Metadata) metric.Metadata {
		meta.Name = fmt.Sprintf(meta.Name, cat)
		meta.Help = fmt.Sprintf(meta.Help, cat)
		meta.Measurement = fmt.Sprintf(meta.Measurement, measurement)
		meta.Unit = metric.Unit_NANOSECONDS
		return meta
	}
	return &categoryMetrics{
		Observed:      metric.NewCounter(fixMeta(observedMeta)),
		Estimated:     metric.NewCounter(fixMeta(estimatedMeta)),
		WorkWallNanos: metric.NewCounter(fixWallMeta(workWallMeta)),
	}
}

func makeForecastMetric(name string, measurement string, unit metric.Unit) *metric.Counter {
	fixMeta := func(meta metric.Metadata) metric.Metadata {
		meta.Name = fmt.Sprintf(meta.Name, name)
		meta.Help = fmt.Sprintf(meta.Help, name)
		meta.Measurement = fmt.Sprintf(meta.Measurement, measurement)
		meta.Unit = unit
		return meta
	}
	return metric.NewCounter(fixMeta(forecastMeta))
}

type categoryMetrics struct {
	Observed      *metric.Counter
	Estimated     *metric.Counter
	WorkWallNanos *metric.Counter
}

type metrics struct {
	categoryCPU       [numWorkCategories]*categoryMetrics
	categoryDiskWrite [numWorkCategories]map[roachpb.StoreID]*categoryMetrics
	cpuForecast       *metric.Counter
	diskWriteForecast map[roachpb.StoreID]*metric.Counter
}

func makeMetrics(
	registry *metric.Registry, storeRegistries map[roachpb.StoreID]*metric.Registry,
) metrics {
	var categoryCPU [numWorkCategories]*categoryMetrics
	categoryCPU[PebbleCompaction] =
		makeCategoryMetrics("compaction", "cpu", "Nanoseconds", metric.Unit_NANOSECONDS)
	registry.AddMetricStruct(categoryCPU[PebbleCompaction])
	categoryCPU[SnapshotRecv] =
		makeCategoryMetrics("snapshot", "cpu", "Nanoseconds", metric.Unit_NANOSECONDS)
	registry.AddMetricStruct(categoryCPU[SnapshotRecv])

	var categoryDiskWrite [numWorkCategories]map[roachpb.StoreID]*categoryMetrics
	categoryDiskWrite[PebbleCompaction] = map[roachpb.StoreID]*categoryMetrics{}
	for s, registry := range storeRegistries {
		m := makeCategoryMetrics("compaction", "disk_write", "Bytes", metric.Unit_BYTES)
		categoryDiskWrite[PebbleCompaction][s] = m
		registry.AddMetricStruct(m)
	}
	categoryDiskWrite[SnapshotRecv] = map[roachpb.StoreID]*categoryMetrics{}
	for s, registry := range storeRegistries {
		m := makeCategoryMetrics("snapshot", "disk_write", "Bytes", metric.Unit_BYTES)
		categoryDiskWrite[SnapshotRecv][s] = m
		registry.AddMetricStruct(m)
	}

	cpuForecast := makeForecastMetric("cpu", "Nanoseconds", metric.Unit_NANOSECONDS)
	registry.AddMetric(cpuForecast)
	diskWriteForecast := map[roachpb.StoreID]*metric.Counter{}
	for s, registry := range storeRegistries {
		m := makeForecastMetric("disk_write", "Bytes", metric.Unit_BYTES)
		diskWriteForecast[s] = m
		registry.AddMetric(m)
	}
	m := metrics{
		categoryCPU:       categoryCPU,
		categoryDiskWrite: categoryDiskWrite,
		cpuForecast:       cpuForecast,
		diskWriteForecast: diskWriteForecast,
	}
	return m
}

type workGranterImpl struct {
	st          *cluster.Settings
	stop        *stop.Stopper
	timeSource  timeutil.TimeSource
	newReporter newInternalUsageReporter
	goMaxProcs  func() int
	mu          struct {
		syncutil.Mutex
		statsProvider     ResourceStatsProvider
		lastStats         StatsForResources
		lastTickUnixNanos int64
		// statsScratch does not back lastStats.
		statsScratch map[roachpb.StoreID]ResourceStats

		allowedWithoutPermission [numWorkCategories]int
		rateThresholds           PhysicalResourceValues

		intRateForecast  PhysicalResourceValues
		initRateForecast bool

		workRateEstimate map[WorkCategoryAndStore]workResourceUsageRate

		requesters  map[WorkCategoryAndStore]*requesterInfo
		granted     [numWorkCategories]int
		work        map[workID]*workInfo
		workScratch []*workInfo
		idCount     workID

		stats                   stats
		forecastUpdateUnixNanos int64
		metrics                 metrics

		isGranting     bool
		isGrantingCond *sync.Cond
	}
	deterministicForTesting bool
	requestersScratch       []*requesterInfo
	heapScratch             requesterHeap
	logEvery                log.EveryN
}

var _ WorkGranter = &workGranterImpl{}

const tickInterval time.Duration = 100 * time.Millisecond
const ticksPerSecond int64 = int64(time.Second / tickInterval)

func NewLongWorkGranterImpl(st *cluster.Settings, stop *stop.Stopper) *workGranterImpl {
	return newLongWorkGranterImpl(
		st, stop, timeutil.DefaultTimeSource{}, newUsageReporterImpl, defaultGoMaxProcs)
}

func defaultGoMaxProcs() int {
	return runtime.GOMAXPROCS(0)
}

func newLongWorkGranterImpl(
	st *cluster.Settings,
	stop *stop.Stopper,
	timeSource timeutil.TimeSource,
	newReporter newInternalUsageReporter,
	goMaxProcs func() int,
) *workGranterImpl {
	g := &workGranterImpl{
		st:          st,
		stop:        stop,
		timeSource:  timeSource,
		newReporter: newReporter,
		goMaxProcs:  goMaxProcs,
		logEvery:    log.Every(time.Second),
	}
	g.mu.statsScratch = map[roachpb.StoreID]ResourceStats{}
	g.setAllowedWithoutPermission()
	g.mu.rateThresholds = PhysicalResourceValues{DiskWrite: map[roachpb.StoreID]int64{}}
	g.mu.intRateForecast = PhysicalResourceValues{DiskWrite: map[roachpb.StoreID]int64{}}
	g.mu.workRateEstimate = map[WorkCategoryAndStore]workResourceUsageRate{}
	g.mu.requesters = map[WorkCategoryAndStore]*requesterInfo{}
	g.mu.work = map[workID]*workInfo{}
	g.mu.stats = stats{
		cumDiskForecast: make(map[roachpb.StoreID]int64),
	}
	g.mu.stats.category[PebbleCompaction].diskWrite = map[roachpb.StoreID]*cumulativeResourceStats{}
	g.mu.stats.category[SnapshotRecv].diskWrite = map[roachpb.StoreID]*cumulativeResourceStats{}
	g.mu.isGrantingCond = sync.NewCond(&g.mu)
	return g
}

// REQUIRES: g.mu is held
func (g *workGranterImpl) setAllowedWithoutPermission() {
	numProcs := g.goMaxProcs()
	getAllowed := func(mult float64) int {
		if mult == 0 {
			return math.MaxInt
		}
		return max(1, int(math.Ceil(mult*float64(numProcs))))
	}
	g.mu.allowedWithoutPermission[PebbleCompaction] = getAllowed(
		compactionsAllowedWithoutPermissionMultiplier.Get(&g.st.SV))
	g.mu.allowedWithoutPermission[SnapshotRecv] = getAllowed(
		receiveSnapshotAllowedWithoutPermissionMultiplier.Get(&g.st.SV))
}

func (g *workGranterImpl) RegisterStatsProvider(
	provider ResourceStatsProvider,
	registry *metric.Registry,
	storeRegistries map[roachpb.StoreID]*metric.Registry,
) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.metrics = makeMetrics(registry, storeRegistries)
	g.mu.statsProvider = provider
	g.mu.lastStats = provider.GetStats(map[roachpb.StoreID]ResourceStats{})
	g.mu.lastTickUnixNanos = g.timeSource.Now().UnixNano()
	g.mu.forecastUpdateUnixNanos = g.timeSource.Now().UnixNano()
	g.stop.RunAsyncTask(context.Background(), "long-work-granter", func(_ context.Context) {
		ticker := g.timeSource.NewTicker(tickInterval)
		for {
			select {
			case <-ticker.Ch():
				g.tick()
			case <-g.stop.ShouldQuiesce():
				ticker.Stop()
				return
			}
		}
	})
}

func (g *workGranterImpl) RegisterRequester(
	kind WorkCategoryAndStore, requester WorkRequester, numGoroutinesPerWork int,
) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.requesters[kind] = &requesterInfo{
		kind:                 kind,
		WorkRequester:        requester,
		numGoroutinesPerWork: numGoroutinesPerWork,
	}
}

func (g *workGranterImpl) UnregisterRequester(kind WorkCategoryAndStore) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.mu.requesters, kind)
	// Wait until isGranting becomes false. Since the requester has been deleted
	// from the map, once isGranting becomes false, no more granting to this
	// requester will happen.
	for g.mu.isGranting {
		g.mu.isGrantingCond.Wait()
	}
}

func (g *workGranterImpl) TryGet(kind WorkCategoryAndStore) (bool, WorkResourceUsageReporter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	reqInfo := g.mu.requesters[kind]
	if reqInfo.waiting.value {
		reqInfo.waiting.tryGetCount++
		return false, nil
	}
	if g.mu.isGranting {
		// tryGrant is running. It is possible that it will no longer sample
		// whether this WorkRequester has waiting work. But we are ok with this
		// rare race since tryGrant will run again after 100ms. Note that we can't
		// wait until tryGrant stops running since that would cause a deadlock (it could
		// be waiting blocked in a call to GetScore on this requester).
		//
		// We could choose to call tryGetInternalLocked even though tryGrant is
		// running. This would be unfair in allowing this work to grab resources
		// ahead of other waiting work.
		reqInfo.setWaitingToTrue()
		return false, nil
	}
	success, reporter := g.tryGetInternalLocked(kind, reqInfo)
	if !success {
		// NB: tryGetInternalLocked has already set reqInfo.waiting to true.
		return false, nil
	}
	return true, reporter
}

func (g *workGranterImpl) tryGetInternalLocked(
	kind WorkCategoryAndStore, reqInfo *requesterInfo,
) (bool, internalUsageReporter) {
	workEstimate := g.mu.workRateEstimate[kind]
	// If !haveEstimate, we will only permit if it does not need permission.
	// This will only happen early in the lifetime of a node, until the
	// WorkCategoryAndStore gets to run once.
	haveEstimate := workEstimate != workResourceUsageRate{}
	withoutPermissionForStore := reqInfo.WorkRequester.GetAllowedWithoutPermissionForStore()
	haveStorePermission := true
	if reqInfo.granted >= withoutPermissionForStore {
		// Note that we use rate thresholds without considering the workEstimate
		// that will be added. This is to avoid a situation where we block one
		// kind of work that has a higher workEstimate, while allowing another. We
		// think this is the right choice to reduce starvation -- we only want
		// starvation if the score is consistently low.
		if !haveEstimate || !g.mu.initRateForecast ||
			g.mu.intRateForecast.DiskWrite[kind.Store] > g.mu.rateThresholds.DiskWrite[kind.Store] {
			haveStorePermission = false
		}
	}
	log.Infof(context.Background(),
		"workGranterImpl.tryGet tryGet: haveEstimate: %t, haveStorePermission: %t, withoutPermissionForStore: %d, diskthreshold: %s (forecast %s, count %d)",
		haveEstimate, haveStorePermission, withoutPermissionForStore,
		humanize.IBytes(uint64(g.mu.rateThresholds.DiskWrite[kind.Store])),
		humanize.IBytes(uint64(g.mu.intRateForecast.DiskWrite[kind.Store])),
		len(g.mu.work))

	if !haveStorePermission {
		reqInfo.setWaitingToTrue()
		return false, nil
	}
	withoutPermissionForNode := g.mu.allowedWithoutPermission[kind.Category]
	haveNodePermission := true
	if g.mu.granted[kind.Category] >= withoutPermissionForNode {
		if !haveEstimate || !g.mu.initRateForecast ||
			g.mu.intRateForecast.CPU > g.mu.rateThresholds.CPU {
			haveNodePermission = false
		}
	}
	log.Infof(context.Background(),
		"workGranterImpl.tryGet tryGet: haveEstimate: %t, haveNodePermission: %t, withoutPermissionForNode: %d, cputhreshold: %d",
		haveEstimate, haveNodePermission, withoutPermissionForNode, g.mu.rateThresholds.CPU)

	if !haveNodePermission {
		reqInfo.setWaitingToTrue()
		return false, nil
	}
	// Have all permission.
	g.adjustForecastStats(g.timeSource.Now().UnixNano())
	g.mu.intRateForecast.CPU += workEstimate[cpu]
	g.mu.intRateForecast.DiskWrite[kind.Store] += workEstimate[diskWrite]
	reqInfo.granted++
	g.mu.granted[kind.Category]++
	g.mu.idCount++
	usageReporter := g.newReporter(g, kind, g.mu.idCount, reqInfo.numGoroutinesPerWork)
	workInfo := &workInfo{
		id:                 g.mu.idCount,
		kind:               kind,
		usageReporter:      usageReporter,
		usage:              resourceUsageByWork{},
		rateEstimate:       workEstimate,
		estimateStartNanos: g.timeSource.Now().UnixNano(),
	}
	g.mu.work[g.mu.idCount] = workInfo
	return true, usageReporter
}

func (g *workGranterImpl) tick() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.mu.initRateForecast {
		g.mu.initRateForecast = true
	}
	// Update allowedWithoutPermission.
	g.setAllowedWithoutPermission()

	// Use g.mu.statsScratch since it does not back g.mu.lastStats.
	stats := g.mu.statsProvider.GetStats(g.mu.statsScratch)
	// For the next tick, use the scratch that backs g.mu.lastStats, since we
	// will be updating g.mu.lastStats in this tick.
	g.mu.statsScratch = g.mu.lastStats.Disk

	// Update rateThresholds.
	g.mu.rateThresholds.CPU =
		int64(float64(stats.CPU.ProvisionedRate) * utilizationThreshold.Get(&g.st.SV))
	clear(g.mu.rateThresholds.DiskWrite)
	for s, stat := range stats.Disk {
		g.mu.rateThresholds.DiskWrite[s] =
			int64(float64(stat.ProvisionedRate) * utilizationThreshold.Get(&g.st.SV))
	}

	tickUnixNanos := g.timeSource.Now().UnixNano()
	lastTickUnixNanos := g.mu.lastTickUnixNanos
	g.mu.lastTickUnixNanos = tickUnixNanos
	// Use the last forecast before we clobber g.mu.intRateForecast.DiskWrite.
	g.adjustForecastStats(tickUnixNanos)

	// Compute interval rates.
	var intRate PhysicalResourceValues
	intRate.CPU = (stats.CPU.Used - g.mu.lastStats.CPU.Used) * ticksPerSecond
	intRate.DiskWrite = g.mu.intRateForecast.DiskWrite
	clear(intRate.DiskWrite)
	for s, stat := range stats.Disk {
		a := stat.Used - g.mu.lastStats.Disk[s].Used
		if a < 0 {
			panic(errors.AssertionFailedf("disk usage decreased: %d", a))
		}
		intRate.DiskWrite[s] = (stat.Used - g.mu.lastStats.Disk[s].Used) * ticksPerSecond
	}
	// Replace lastStats since interval rates have been computed.
	g.mu.lastStats = stats

	// Use interval rate as the basis for computing the forecast.
	intRateForecast := intRate

	g.mu.workScratch = g.mu.workScratch[:0]
	for _, info := range g.mu.work {
		g.mu.workScratch = append(g.mu.workScratch, info)
	}
	if g.deterministicForTesting {
		slices.SortFunc(g.mu.workScratch, func(a, b *workInfo) int {
			return cmp.Compare(a.id, b.id)
		})
	}
	for _, info := range g.mu.workScratch {
		cumWorkUsage := info.usageReporter.getMetrics()
		// Interval work usage.
		var intWorkUsage [numResources]int64
		for i := range intWorkUsage {
			intWorkUsage[i] = cumWorkUsage.cumulative[i] - info.usage.cumulative[i]
			if i == 1 {
				if false || g.logEvery.ShouldLog() {
					usage := intWorkUsage[i]
					prefix := ""
					if usage < 0 {
						usage = -usage
						prefix = "-"
					}
					log.Infof(context.Background(),
						"CumulativeStats ID: %d at tick: disk usage %s%s %s-%s (%s-%s)",
						uint64(info.id),
						prefix, humanize.IBytes(uint64(usage)),
						humanize.IBytes(uint64(cumWorkUsage.cumulative[i])),
						humanize.IBytes(uint64(info.usage.cumulative[i])),
						humanize.IBytes(uint64(cumWorkUsage.debugging.estimated)),
						humanize.IBytes(uint64(info.usage.debugging.estimated)))
				}
			}
			// This can happen because of estimation in the caller of
			// usageReporterImpl.CumulativeWriteBytes
			if intWorkUsage[i] < 0 {
				// log.Infof(context.Background(), "Tick: usage negative %d: %d",
				//	i, intWorkUsage[i])
				intWorkUsage[i] = 0
			}
		}
		info.usage = cumWorkUsage
		g.mu.stats.category[info.kind.Category].cpu.observed += intWorkUsage[cpu]
		diskWriteMap := g.mu.stats.category[info.kind.Category].diskWrite
		diskWriteStats := diskWriteMap[info.kind.Store]
		if diskWriteStats == nil {
			diskWriteStats = &cumulativeResourceStats{}
			diskWriteMap[info.kind.Store] = diskWriteStats
		}
		diskWriteStats.observed += intWorkUsage[diskWrite]
		// If workDone has already executed, then info.rateEstimate is already zero.
		g.adjustWorkEstimatedStats(info, tickUnixNanos)
		var intervalFraction float64
		if info.usage.startTimeUnixNanos != 0 {
			// Work has started.
			workIntervalEnd := tickUnixNanos
			if info.usage.endTimeUnixNanos != 0 {
				workIntervalEnd = info.usage.endTimeUnixNanos
			}
			workIntervalStart := info.usage.startTimeUnixNanos
			if workIntervalStart < lastTickUnixNanos {
				workIntervalStart = lastTickUnixNanos
			}
			workInterval := workIntervalEnd - workIntervalStart
			if workInterval < 0 {
				workInterval = 0
			}
			g.mu.stats.category[info.kind.Category].cpu.workWallNanos += workInterval
			diskWriteStats.workWallNanos += workInterval

			// TODO: the use of tickInterval and ticksPerSecond is not correct in general.
			intervalFraction = float64(workInterval) / float64(tickInterval)
		}
		// We ignore work that has not done any disk writes since we have
		// examples like move compactions, that do no disk write and we don't
		// want those to affect the forecast. This is an ad-hoc hack. The
		// better thing would be to have an estimate for each sub-category of
		// work.
		if intervalFraction > 0.2 && cumWorkUsage.cumulative[diskWrite] > 0 {
			// The choice of 0.2 is arbitrary.
			var intWorkRate [numResources]int64
			// Incorporate its usage into estimate.
			intWorkRate = intWorkUsage
			if intervalFraction < 1.0 {
				for i := range intWorkRate {
					// Scale it up.
					intWorkRate[i] = int64((float64(intWorkRate[i])) / intervalFraction)
				}
			}
			for i := range intWorkRate {
				intWorkRate[i] *= ticksPerSecond
			}
			if intWorkRate[diskWrite] > 500<<20 {
				// Cap it.
				log.Infof(context.Background(),
					"HUGE: Observed disk write rate %s over interval %f and usage %s",
					humanize.IBytes(uint64(intWorkRate[diskWrite])), intervalFraction,
					humanize.IBytes(uint64(intWorkUsage[diskWrite])))
				intWorkRate[diskWrite] = 500 << 20
			}
			estimate := g.mu.workRateEstimate[info.kind]
			if estimate == (workResourceUsageRate{}) {
				// Use intWorkRate as the initial estimate.
				g.mu.workRateEstimate[info.kind] = intWorkRate
			} else {
				// Incorporate intWorkRate using exponential smoothing.
				alpha := 0.1
				if intervalFraction < 1.0 {
					// We trust the interval rate less.
					alpha *= intervalFraction
				}
				for i := range estimate {
					estimate[i] = int64((1-alpha)*float64(estimate[i]) + alpha*float64(intWorkRate[i]))
				}
				g.mu.workRateEstimate[info.kind] = estimate
			}
		}
		// Remove what was observed from the forecast. We may add this back later, or
		// do something else.
		intRateForecast.CPU -= intWorkUsage[cpu] * ticksPerSecond
		intRateForecast.DiskWrite[info.kind.Store] -= intWorkUsage[diskWrite] * ticksPerSecond
		// Now adjust the forecast.
		if info.usage.done {
			// No need to add to the forecast. Remove it from g.work map.
			delete(g.mu.work, info.id)
		} else {
			// Not ended.
			//
			// intWorkRate may not be initialized, in which case it is 0. Even
			// if it is initialized, it can show large fluctuations. It may be
			// a small value since we could have a 100ms interval where little
			// gets done (due to propagation delay in a wide-area setting), or
			// because of byte estimation errors in the caller. Those byte
			// estimation errors could also result in huge values of rate
			// (since the 20ms threshold could be met, and if 1MB was due to
			// estimation error, that results in a rate of 50MB/s). So we use
			// intWorkUsage instead, which cannot be magnified by the
			// interval.
			//
			// Note that g.mu.workRateEstimate[info.kind] can be modified in
			// future iterations of this loop because of other work, and we
			// are simply using what we have now.
			estimatedWorkRate := g.mu.workRateEstimate[info.kind]
			for i := range intWorkUsage {
				if intWorkUsage[i]*ticksPerSecond > estimatedWorkRate[i] {
					estimatedWorkRate[i] = intWorkUsage[i] * ticksPerSecond
				}
			}
			// Remember the estimate we used in the forecast so that we can subtract
			// it immediately when the work ends.
			info.rateEstimate = estimatedWorkRate
			info.estimateStartNanos = tickUnixNanos
			intRateForecast.CPU = intRateForecast.CPU + estimatedWorkRate[cpu]
			intRateForecast.DiskWrite[info.kind.Store] = intRateForecast.DiskWrite[info.kind.Store] +
				estimatedWorkRate[diskWrite]
		}
		if intRateForecast.CPU < 0 {
			intRateForecast.CPU = 0
		}
		if intRateForecast.DiskWrite[info.kind.Store] < 0 {
			intRateForecast.DiskWrite[info.kind.Store] = 0
		}
	}
	g.mu.intRateForecast = intRateForecast
	g.updateMetrics()
	// For fairness, we tryGrant here to prevent tryGet's from racing ahead
	// when requests are queued. This is best-effort since we tryGrant will
	// release and reacquire the mutex while executing, whch could all a
	// tryGet to slip through.
	g.tryGrant()
}

func (g *workGranterImpl) workDone(kind WorkCategoryAndStore, id workID, tryGrant bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	info := g.mu.work[id]
	if info != nil {
		g.adjustForecastStats(g.timeSource.Now().UnixNano())
		g.mu.intRateForecast.CPU -= info.rateEstimate[cpu]
		if g.mu.intRateForecast.CPU < 0 {
			g.mu.intRateForecast.CPU = 0
		}
		g.mu.intRateForecast.DiskWrite[info.kind.Store] -= info.rateEstimate[diskWrite]
		if g.mu.intRateForecast.DiskWrite[info.kind.Store] < 0 {
			g.mu.intRateForecast.DiskWrite[info.kind.Store] = 0
		}
		estimateEndNanos := g.timeSource.Now().UnixNano()
		g.adjustWorkEstimatedStats(info, estimateEndNanos)
		// Set it to 0
		info.rateEstimate = workResourceUsageRate{}
	}
	// Else already removed by a concurrent tick. And that concurrent tick has
	// already computed a new forecast, and the cumulative stats don't need
	// fixing since the tick and workDone happened at the same time.
	//
	// TODO(sumeer): the fact that we are not doing the following decrements in
	// tick is odd. Fix that.
	reqInfo := g.mu.requesters[kind]
	log.Infof(context.Background(), "workGranterImpl.workDone: tryGrant: %t", tryGrant)
	if reqInfo != nil {
		// Requester has not unregistered.
		reqInfo.granted--
	}
	g.mu.granted[kind.Category]--
	if !tryGrant {
		return
	}
	// For fairness, we tryGrant here to prevent tryGet's from racing ahead when
	// requests are queued.
	g.tryGrant()
}

func (g *workGranterImpl) adjustWorkEstimatedStats(info *workInfo, estimateEndNanos int64) {
	estimateIntervalSec := float64(estimateEndNanos-info.estimateStartNanos) / float64(time.Second)
	estimateCPU := int64(float64(info.rateEstimate[cpu]) * estimateIntervalSec)
	estimateDiskWrite := int64(float64(info.rateEstimate[diskWrite]) * estimateIntervalSec)
	g.mu.stats.category[info.kind.Category].cpu.estimated += estimateCPU
	diskWriteMap := g.mu.stats.category[info.kind.Category].diskWrite
	diskWriteStats := diskWriteMap[info.kind.Store]
	if diskWriteStats == nil {
		diskWriteStats = &cumulativeResourceStats{}
		diskWriteMap[info.kind.Store] = diskWriteStats
	}
	diskWriteStats.estimated += estimateDiskWrite
}

func (g *workGranterImpl) adjustForecastStats(nowUnixNanos int64) {
	intervalNanos := nowUnixNanos - g.mu.forecastUpdateUnixNanos
	if intervalNanos < 0 {
		intervalNanos = 0
	}
	g.mu.forecastUpdateUnixNanos = nowUnixNanos
	intervalSec := float64(intervalNanos) / float64(time.Second)
	g.mu.stats.cumCPUForecast += int64(float64(g.mu.intRateForecast.CPU) * intervalSec)
	for s, rate := range g.mu.intRateForecast.DiskWrite {
		if rate < 0 {
			panic(errors.AssertionFailedf("rate < 0 %d for store %d", rate, s))
		}
		g.mu.stats.cumDiskForecast[s] += int64(float64(rate) * intervalSec)
	}
}

type workScoreAndInfo struct {
	score WorkScore
	req   *requesterInfo

	// The heapIndex is maintained by the heap.Interface methods, and represents
	// the heapIndex of the item in the heap.
	heapIndex int
}

type requesterHeap []workScoreAndInfo

var _ heap.Interface = (*requesterHeap)(nil)

func (h *requesterHeap) Len() int {
	return len(*h)
}

func (h *requesterHeap) Less(i, j int) bool {
	return cmp.Or(func(a, b bool) int {
		if a == b {
			return 0
		}
		if a {
			return +1
		} else {
			return -1
		}
	}((*h)[i].score.Optional, (*h)[j].score.Optional),
		cmp.Compare((*h)[i].req.kind.Category, (*h)[j].req.kind.Category),
		cmp.Compare((*h)[j].score.Score, (*h)[i].score.Score)) < 0
}

func (h *requesterHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].heapIndex = i
	(*h)[j].heapIndex = j
}

func (h *requesterHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(workScoreAndInfo)
	item.heapIndex = n
	*h = append(*h, item)
}

func (h *requesterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = workScoreAndInfo{}
	item.heapIndex = -1
	*h = old[0 : n-1]
	return item
}

// Periodic or when something ends. g.mu must be held when calling this
// method, but can be released and reacquired while executing. It will be held
// when returning.
func (g *workGranterImpl) tryGrant() {
	g.mu.AssertHeld()
	for g.mu.isGranting {
		g.mu.isGrantingCond.Wait()
	}
	g.mu.isGranting = true
	defer func() {
		g.mu.AssertHeld()
		g.mu.isGranting = false
		g.mu.isGrantingCond.Broadcast()
	}()
	g.requestersScratch = g.requestersScratch[:0]
	for _, req := range g.mu.requesters {
		g.requestersScratch = append(g.requestersScratch, req)
	}
	if g.deterministicForTesting {
		slices.SortFunc(g.requestersScratch, func(a, b *requesterInfo) int {
			return cmp.Or(cmp.Compare(a.kind.Category, b.kind.Category),
				cmp.Compare(a.kind.Store, b.kind.Store))
		})
	}
	log.Infof(context.Background(), "workGranterImpl.TryGrant started")
	g.heapScratch = g.heapScratch[:0]
	for _, req := range g.requestersScratch {
		waitingState := req.getWaitingState()
		if !waitingState.value {
			continue
		}
		g.mu.Unlock()
		waiting, score := req.GetScore()
		g.mu.Lock()
		log.Infof(context.Background(), "workGranterImpl.TryGrant: %+v %t %+v", req.kind, waiting, score)
		if !waiting {
			// It is possible that another TryGet has happened, but we ignore this
			// WorkRequester for the rest of this TryGrant run.
			req.trySetWaitingToFalse(waitingState)
		} else {
			heap.Push(&g.heapScratch, workScoreAndInfo{
				score: score,
				req:   req,
			})
		}
	}
	reqHeap := g.heapScratch
	for len(reqHeap) > 0 {
		cand := reqHeap[0]
		granted, reporter := g.tryGetInternalLocked(cand.req.kind, cand.req)
		if granted {
			waitingState := cand.req.getWaitingState()
			g.mu.Unlock()
			if !g.mu.requesters[cand.req.kind].Grant(reporter) {
				reporter.workRejected()
				g.mu.Lock()
				// It is possible that another TryGet has happened, but we ignore this
				// WorkRequester for the rest of this TryGrant run.
				cand.req.trySetWaitingToFalse(waitingState)
				heap.Pop(&reqHeap)
			} else {
				// Accepted grant.
				waiting, score := cand.req.GetScore()
				g.mu.Lock()
				if !waiting {
					// It is possible that another TryGet has happened, but we ignore
					// this WorkRequester for the rest of this TryGrant run.
					cand.req.trySetWaitingToFalse(waitingState)
					heap.Pop(&reqHeap)
				} else {
					reqHeap[0].score = score
					heap.Fix(&reqHeap, 0)
				}
			}
		} else {
			// The failure to tryGet could be because the store or the node is out
			// of resources. If it is the former we can't exit this loop, so we have
			// to continue. If it is the latter, we can exit the loop, but since we
			// are not distinguishing these cases, we continue.
			//
			// TODO(sumeer): use the reason of the failure to avoid calling
			// tryGetInternalLocked for the same store, or to fully exit the loop if
			// the node is out of resources. The may be a worthwhile optimization
			// when the number of stores times the numWorkCategories is large.
			heap.Pop(&reqHeap)
		}
	}
}

func (g *workGranterImpl) updateMetrics() {
	for i := range g.mu.stats.category {
		cpu := &g.mu.stats.category[i].cpu
		g.mu.metrics.categoryCPU[i].Observed.Update(cpu.observed)
		g.mu.metrics.categoryCPU[i].Estimated.Update(cpu.estimated)
		g.mu.metrics.categoryCPU[i].WorkWallNanos.Update(cpu.workWallNanos)
		diskWrite := g.mu.stats.category[i].diskWrite
		for s, stats := range diskWrite {
			m := g.mu.metrics.categoryDiskWrite[i][s]
			if m == nil {
				panic(errors.AssertionFailedf("metric not found for category %d store %d", i, s))
			}
			m.Observed.Update(stats.observed)
			m.Estimated.Update(stats.estimated)
			m.WorkWallNanos.Update(stats.workWallNanos)
		}
	}
	g.mu.metrics.cpuForecast.Update(g.mu.stats.cumCPUForecast)
	for s, diskForecast := range g.mu.stats.cumDiskForecast {
		m := g.mu.metrics.diskWriteForecast[s]
		if m == nil {
			panic(errors.AssertionFailedf("metric not found for store %d", s))
		}
		m.Update(diskForecast)
	}
}

// TODO:
//
//  - pacing
//  - optimize allocations.
