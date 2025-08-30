// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"container/heap"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/benignerror"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// purgatoryReportInterval is the duration between reports on
	// purgatory status.
	purgatoryReportInterval = 10 * time.Minute
	// defaultProcessTimeout is the timeout when processing a replica.
	// The timeout prevents a queue from getting stuck on a replica.
	// For example, a replica whose range is not reachable for quorum.
	defaultProcessTimeout = 1 * time.Minute
	// defaultQueueMaxSize is the default max size for a queue.
	defaultQueueMaxSize = 10000
)

// queueGuaranteedProcessingTimeBudget is the smallest amount of time before
// which the processing of a queue may time out. It is an escape hatch to raise
// the timeout for queues.
var queueGuaranteedProcessingTimeBudget = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"kv.queue.process.guaranteed_time_budget",
	"the guaranteed duration before which the processing of a queue may time out",
	defaultProcessTimeout,
	settings.WithVisibility(settings.Reserved),
)

func defaultProcessTimeoutFunc(cs *cluster.Settings, _ replicaInQueue) time.Duration {
	return queueGuaranteedProcessingTimeBudget.Get(&cs.SV)
}

// The queues which traverse through the data in the range (i.e. send a snapshot
// or calculate a range checksum) while processing should have a timeout which
// is a function of the size of the range and the maximum allowed rate of data
// transfer that adheres to a minimum timeout specified in a cluster setting.
// When the queue contains different types of work items, with different rates,
// the timeout of all items is set according to the minimum rate of the
// different types, to prevent slower items from causing faster items appearing
// after them in the queue to time-out.
//
// The parameter controls which rate(s) to use.
func makeRateLimitedTimeoutFunc(rateSettings *settings.ByteSizeSetting) queueProcessTimeoutFunc {
	return makeRateLimitedTimeoutFuncByPermittedSlowdown(permittedRangeScanSlowdown, rateSettings)
}

// permittedRangeScanSlowdown is the factor of the above the estimated duration
// for a range scan given the configured rate which we use to configure
// the operations's timeout.
const permittedRangeScanSlowdown = 10

// makeRateLimitedTimeoutFuncByPermittedSlowdown creates a timeout function based on a permitted
// slowdown factor on the estimated queue processing duration based on the given rate settings.
// See makeRateLimitedTimeoutFunc for more information.
func makeRateLimitedTimeoutFuncByPermittedSlowdown(
	permittedSlowdown int, rateSettings *settings.ByteSizeSetting,
) queueProcessTimeoutFunc {
	return func(cs *cluster.Settings, r replicaInQueue) time.Duration {
		minimumTimeout := queueGuaranteedProcessingTimeBudget.Get(&cs.SV)
		// NB: In production code this will type assertion will always succeed.
		// Some tests set up a fake implementation of replicaInQueue in which
		// case we fall back to the configured minimum timeout.
		repl, ok := r.(interface{ GetMVCCStats() enginepb.MVCCStats })
		if !ok {
			return minimumTimeout
		}
		minSnapshotRate := rateSettings.Get(&cs.SV)
		estimatedDuration := time.Duration(repl.GetMVCCStats().Total()/minSnapshotRate) * time.Second
		timeout := estimatedDuration * time.Duration(permittedSlowdown)
		if timeout < minimumTimeout {
			timeout = minimumTimeout
		}
		return timeout
	}
}

// PurgatoryError indicates a replica processing failure which indicates the
// replica can be placed into purgatory for faster retries than the replica
// scanner's interval.
type PurgatoryError interface {
	error
	PurgatoryErrorMarker() // dummy method for unique interface
}

// noopProcessCallback is a processCallback that does nothing.
var noopProcessCallback = processCallback{
	onProcessResult: func(err error) {},
	onEnqueueResult: func(indexOnHeap int, err error) {},
}

// processCallback is a hook that is called when a replica is enqueued or
// finishes processing.
//
// NB: None of the fields below can be nil. Use noopProcessCallback if you do
// not need to register any callback.
//
// The callback behavior depends on when it's registered. Currently, addInternal
// and MaybeAddCallback are the only two users. See comments above them for more
// details on the exact behaviour.
//
// NB: Callback execution is not guaranteed since removeLocked or
// removeFromReplicaSetLocked may be called without executing callbacks. This
// happens when the replica is destroyed or recreated with a new replica ID.
//
// For now, the two use cases (decommissioning nudger and
// maybeBackpressureBatch) are okay with the current behaviour. But adding new
// uses is discouraged without cleaning up the contract of processCallback.
// TODO(wenyihu6): consider cleaning up the semantics after backports
type processCallback struct {
	// onEnqueueResult is called with the result of the enqueue attempt. It is
	// invoked when the range is added to the queue and if the range encounters
	// any errors and being enqueued again before being processed.
	//
	// If error is nil, the index on the priority queue where this item sits is
	// also passed in the callback. If error is non-nil, the index passed in the
	// callback is -1. Note: indexOnHeap does not represent the item's exact rank
	// by priority. It only reflects the item's position in the heap array, which
	// gives a rough idea of where it sits in the priority hierarchy.
	//
	// - May be invoked multiple times:
	//   1. Immediately after successful enqueue (err = nil).
	//   2. If the replica is later dropped due to full queue (err =
	//   errDroppedDueToFullQueueSize).
	//   3. If re-added with updated priority (err = nil, new heap index).
	//   4. If the replica is already in the queue and processing.
	// - May be skipped if the replica is already in queue and no priority changes
	// occur.
	onEnqueueResult func(indexOnHeap int, err error)

	// onProcessResult is called with the result of any process attempts. It is
	// only invoked if the base queue gets a chance to process this replica.
	//
	// - May be invoked multiple times if the replica goes through purgatory or
	// re-processing.
	// - May be skipped if the replica is removed with removeFromReplicaSetLocked
	// or registered with a new replica id before processing begins.
	onProcessResult func(err error)
}

// A replicaItem holds a replica and metadata about its queue state and
// processing state.
type replicaItem struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
	seq       int // enforce FIFO order for equal priorities

	// fields used when a replicaItem is enqueued in a priority queue. This field
	// is preserved for purgatory queue as well since baseQueue.processReplica
	// requies it.
	priority float64
	index    int // The index of the item in the heap, maintained by the heap.Interface methods

	// fields used when a replicaItem is processing.
	processing bool
	requeue    bool // enqueue again after processing?
	callbacks  []processCallback
}

// setProcessing moves the item from an enqueued state to a processing state.
func (i *replicaItem) setProcessing() {
	if i.index >= 0 {
		log.Dev.Fatalf(context.Background(),
			"r%d marked as processing but appears in prioQ", i.rangeID,
		)
	}
	i.processing = true
}

// registerCallback adds a new callback to be executed when the replicaItem is
// enqueued or finishes processing. There are two cases where the callback may
// be registered at:
// 1. bq.MaybeAddCallback: register the callback if the replicaItem has been
// added to bq.mu.replicas
// 2. bq.addInternal: register the callback if the replicaItem has not been
// added to bq.mu.replicas yet.
// Note that the contract here is tricky, so adding new uses is discouraged. See
// the comment on processCallback for more details.
func (i *replicaItem) registerCallback(cb processCallback) {
	i.callbacks = append(i.callbacks, cb)
}

// A priorityQueue implements heap.Interface and holds replicaItems.
type priorityQueue struct {
	seqGen int
	sl     []*replicaItem
}

func (pq priorityQueue) Len() int { return len(pq.sl) }

func (pq priorityQueue) Less(i, j int) bool {
	a, b := pq.sl[i], pq.sl[j]
	if a.priority == b.priority {
		// When priorities are equal, we want the lower sequence number to show
		// up first (FIFO).
		return a.seq < b.seq
	}
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return a.priority > b.priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq.sl[i], pq.sl[j] = pq.sl[j], pq.sl[i]
	pq.sl[i].index, pq.sl[j].index = i, j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(pq.sl)
	item := x.(*replicaItem)
	item.index = n
	pq.seqGen++
	item.seq = pq.seqGen
	pq.sl = append(pq.sl, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := pq.sl
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // for gc
	pq.sl = old[0 : n-1]
	return item
}

// update modifies the priority of a replicaItem in the queue.
func (pq *priorityQueue) update(item *replicaItem, priority float64) {
	item.priority = priority
	if len(pq.sl) <= item.index || pq.sl[item.index] != item {
		log.Dev.Fatalf(context.Background(), "updating item in heap that's not contained in it: %v", item)
	}
	heap.Fix(pq, item.index)
}

var (
	errQueueDisabled             = errors.New("queue disabled")
	errQueueStopped              = errors.New("queue stopped")
	errReplicaNotInitialized     = errors.New("replica not initialized")
	errReplicaAlreadyProcessing  = errors.New("replica already processing")
	errReplicaAlreadyInPurgatory = errors.New("replica in purgatory")
	errReplicaAlreadyInQueue     = errors.New("replica already in queue")
	errDroppedDueToFullQueueSize = errors.New("queue full")
)

func isExpectedQueueError(err error) bool {
	return err == nil || errors.Is(err, errQueueDisabled)
}

// shouldQueueAgain is a helper function to determine whether the
// replica should be queued according to the current time, the last
// time the replica was processed, and the minimum interval between
// successive processing. Specifying minInterval=0 queues all replicas.
// Returns a bool for whether to queue as well as a priority based
// on how long it's been since last processed.
func shouldQueueAgain(now, last hlc.Timestamp, minInterval time.Duration) (bool, float64) {
	if minInterval == 0 || last.IsEmpty() {
		return true, 0
	}
	if diff := now.GoTime().Sub(last.GoTime()); diff >= minInterval {
		priority := float64(1)
		// If there's a non-zero last processed timestamp, adjust the
		// priority by a multiple of how long it's been since the last
		// time this replica was processed.
		if !last.IsEmpty() {
			priority = float64(diff.Nanoseconds()) / float64(minInterval.Nanoseconds())
		}
		return true, priority
	}
	return false, 0
}

// replicaInQueue is the subset of *Replica required for interacting with queues.
//
// TODO(tbg): this interface is horrible, but this is what we do use at time of
// extraction. Establish a sane interface and use that.
type replicaInQueue interface {
	AnnotateCtx(context.Context) context.Context
	ReplicaID() roachpb.ReplicaID
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	IsInitialized() bool
	IsDestroyed() (DestroyReason, error)
	Desc() *roachpb.RangeDescriptor
	redirectOnOrAcquireLease(context.Context) (kvserverpb.LeaseStatus, *kvpb.Error)
	CurrentLeaseStatus(context.Context) kvserverpb.LeaseStatus
}

type queueImpl interface {
	// shouldQueue accepts current time, a replica, and the system config
	// and returns whether it should be queued and if so, at what priority.
	// The Replica is guaranteed to be initialized.
	shouldQueue(context.Context, hlc.ClockTimestamp, *Replica, spanconfig.StoreReader) (shouldQueue bool, priority float64)

	// process accepts a replica, and the system config and executes
	// queue-specific work on it. The Replica is guaranteed to be initialized.
	// We return a boolean to indicate if the Replica was processed successfully
	// (vs. it being a no-op or an error).
	process(context.Context, *Replica, spanconfig.StoreReader, float64) (processed bool, err error)

	// processScheduled is called after async task was created to run process.
	// This function is called by the process loop synchronously. This method is
	// called regardless of process being called or not since replica validity
	// checks are done asynchronously.
	postProcessScheduled(ctx context.Context, replica replicaInQueue, priority float64)

	// timer returns a duration to wait between processing the next item
	// from the queue. The duration of the last processing of a replica
	// is supplied as an argument. If no replicas have finished processing
	// yet, this can be 0.
	timer(time.Duration) time.Duration

	// purgatoryChan returns a channel that is signaled with the current
	// time when it's time to retry replicas which have been relegated to
	// purgatory due to failures. If purgatoryChan returns nil, failing
	// replicas are not sent to purgatory.
	purgatoryChan() <-chan time.Time

	// updateChan returns a channel that is signalled whenever there is an update
	// to the cluster state that might impact the replicas in the queue's
	// purgatory.
	updateChan() <-chan time.Time
}

// queueProcessTimeoutFunc controls the timeout for queue processing for a
// replicaInQueue.
type queueProcessTimeoutFunc func(*cluster.Settings, replicaInQueue) time.Duration

type queueConfig struct {
	// maxSize is the maximum number of replicas to queue.
	maxSize int
	// maxConcurrency is the maximum number of replicas that can be processed
	// concurrently. If not set, defaults to 1.
	maxConcurrency       int
	addOrMaybeAddSemSize int
	// needsLease controls whether this queue requires the range lease to operate
	// on a replica. If so, one will be acquired if necessary. Many queues set
	// needsLease not because they literally need a lease, but because they work
	// on a range level and use it to ensure that only one node in the cluster
	// processes that range.
	needsLease bool
	// needsSpanConfigs controls whether this queue requires a valid copy of the
	// span configs to operate on a replica. Not all queues require it, and it's
	// unsafe for certain queues to wait on it. For example, a raft snapshot may
	// be needed in order to make it possible for the span config range to
	// become available (as observed in #16268), so the raft snapshot queue
	// can't require the span configs to already be available.
	needsSpanConfigs bool
	// acceptsUnsplitRanges controls whether this queue can process ranges that
	// need to be split due to zone config settings. Ranges are checked before
	// calling queueImpl.shouldQueue and queueImpl.process.
	// This is to avoid giving the queue a replica that spans multiple config
	// zones (which might make the action of the queue ambiguous - e.g. we don't
	// want to try to replicate a range until we know which zone it is in and
	// therefore how many replicas are required). If needsSpanConfig is not set
	// then this setting is ignored.
	acceptsUnsplitRanges bool
	// processDestroyedReplicas controls whether or not we want to process
	// replicas that have been destroyed but not GCed.
	processDestroyedReplicas bool
	// processTimeout returns the timeout for processing a replica.
	processTimeoutFunc queueProcessTimeoutFunc
	// successes is a counter of replicas enqueued successfully.
	enqueueSuccesses *metric.Counter
	// failures is a counter of replicas that tries to enqueue but fails to do so.
	enqueueFailures *metric.Counter
	// enqueueSkipped is a counter of replicas that didn't attempt to enqueue but
	// returned early during maybeAdd.
	enqueueSkipped *metric.Counter
	// successes is a counter of replicas processed successfully.
	successes *metric.Counter
	// failures is a counter of replicas which failed processing.
	failures *metric.Counter
	// pending is a gauge measuring current replica count pending.
	pending *metric.Gauge
	// full is a counter measuring replicas dropped due to exceeding the queue max
	// size.
	// NB: this metric may be nil for queues that are not interested in tracking
	// this.
	full *metric.Counter
	// processingNanos is a counter measuring total nanoseconds spent processing
	// replicas.
	processingNanos *metric.Counter
	// purgatory is a gauge measuring current replica count in purgatory.
	purgatory *metric.Gauge
	// disabledConfig is a reference to the cluster setting that controls enabling
	// and disabling queues.
	disabledConfig *settings.BoolSetting
	// skipIfReplicaHasExternalFilesConfig is a reference to the
	// clsuter setting that controls whether replicas should be
	// processed in this queue if they have external files. May
	// be nil.
	//
	// skipIfReplicaHasExternalFilesConfig is only consulted after
	// shouldQueue returns true for the given replica.
	skipIfReplicaHasExternalFilesConfig *settings.BoolSetting
}

// baseQueue is the base implementation of the replicaQueue interface. Queue
// implementations should embed a baseQueue and implement queueImpl.
//
// A queue contains replicas in one of three stages: queued, processing, and
// purgatory. A "queued" replica is waiting for processing with some priority
// that was selected when it was added. A "processing" replica is actively being
// worked on by the queue, which delegates to the queueImpl's `process` method.
// Replicas are selected from the queue for processing purely in priority order.
// A "purgatory" replica has been marked by the queue implementation as
// temporarily uninteresting and it will not be processed again until some
// queue-specific event occurs. Not every queue has a purgatory.
//
// Generally, replicas are added to a queue by a replicaScanner, which is a
// Store-level object. The scanner is configured with a set of queues (which in
// practice is all of the queues) and will repeatedly iterate through every
// replica on the store at a measured pace, handing each replica to every
// queueImpl's `shouldQueue` method. This method is implemented differently by
// each queue and decides whether the replica is currently interesting. If so,
// it also selects a priority. Note that queues have a bounded size controlled
// by the `maxSize` config option, which means the ones with lowest priority may
// be dropped if processing cannot keep up and the queue fills.
//
// Replicas are added asynchronously through `MaybeAddAsync` or `AddAsync`.
// MaybeAddAsync checks the various requirements selected by the queue config
// (needsSpanConfigs, needsLease, acceptsUnsplitRanges) as well as the
// queueImpl's `shouldQueue`. AddAsync does not check any of this and accept a
// priority directly instead of getting it from `shouldQueue`. These methods run
// with shared a maximum concurrency of `addOrMaybeAddSemSize`. If the maximum
// concurrency is reached, MaybeAddAsync will silently drop the replica but
// AddAsync will block.
//
// Synchronous replica addition is intentionally not part of the public
// interface. Many queue impl's "processing" work functions acquire various
// locks on Replica, so it would be too easy for a callsite of such a method to
// deadlock. See #36413 for context. Additionally, the queues themselves process
// asynchronously and the bounded size means what you add isn't guaranteed to be
// processed, so the exclusive-async contract just forces callers to realize
// this early.
//
// Processing is rate limited by the queueImpl's `timer` which receives the
// amount of time it took to processes the previous replica and returns the
// amount of time to wait before processing the next one. A bounded amount of
// processing concurrency is allowed, which is controlled by the
// `maxConcurrency` option in the queue's configuration. If a replica is added
// while being processed, it's requeued after the processing finishes.
//
// Note that all sorts of things can change between when a replica is enqueued
// and when it is processed, so the queue makes sure to grab the latest one
// right before processing by looking up the current replica with the same
// RangeID. This replica could be gone or, in extreme cases, could have been
// removed and re-added and now has a new ReplicaID. Implementors needs to be
// resilient to this.
//
// A queueImpl can opt into a purgatory by returning a non-nil channel from the
// `purgatoryChan` method. A replica is put into purgatory when the `process`
// method returns an error with a `PurgatoryError` as an entry somewhere in the
// `Cause` chain. A replica in purgatory is not processed again until the
// channel is signaled, at which point every replica in purgatory is immediately
// processed. This catchup is run without the `timer` rate limiting but shares
// the same `maxConcurrency` semaphore as regular processing. Note that if a
// purgatory replica is pushed out of a full queue, it's also removed from
// purgatory. Replicas in purgatory count against the max queue size.
//
// After construction a queue needs to be `Start`ed, which spawns a goroutine to
// continually pop the "queued" replica with the highest priority and process
// it. In practice, this is done by the same replicaScanner that adds replicas.
type baseQueue struct {
	log.AmbientContext

	name       string
	getReplica func(roachpb.RangeID) (replicaInQueue, error)
	// The constructor of the queueImpl structure MUST return a pointer.
	// This is because assigning queueImpl to a function-local, then
	// passing a pointer to it to `makeBaseQueue`, and then returning it
	// from the constructor function will return a queueImpl containing
	// a pointer to a structure which is a copy of the one within which
	// it is contained. DANGER.
	impl  queueImpl
	store *Store
	queueConfig
	incoming         chan struct{} // Channel signaled when a new replica is added to the queue.
	processSem       chan struct{}
	addOrMaybeAddSem *quotapool.IntPool // for {Maybe,}AddAsync
	addLogN          log.EveryN         // avoid log spam when addSem, addOrMaybeAddSemSize are maxed out
	processDur       int64              // accessed atomically
	mu               struct {
		syncutil.Mutex                                    // Protects all variables in the mu struct
		replicas       map[roachpb.RangeID]*replicaItem   // Map from RangeID to replicaItem
		priorityQ      priorityQueue                      // The priority queue
		purgatory      map[roachpb.RangeID]PurgatoryError // Map of replicas to processing errors
		stopped        bool
		disabled       bool
		maxSize        int64
	}
}

// newBaseQueue returns a new instance of baseQueue with the specified
// shouldQueue function to determine which replicas to queue and maxSize to
// limit the growth of the queue. Note that maxSize doesn't prevent new
// replicas from being added, it just limits the total size. Higher priority
// replicas can still be added; their addition simply removes the lowest
// priority replica.
func newBaseQueue(name string, impl queueImpl, store *Store, cfg queueConfig) *baseQueue {
	// Use the default process timeout if none specified.
	if cfg.processTimeoutFunc == nil {
		cfg.processTimeoutFunc = defaultProcessTimeoutFunc
	}
	if cfg.maxConcurrency == 0 {
		cfg.maxConcurrency = 1
	}
	// NB: addOrMaybeAddSemSize coupled with tight scanner intervals in tests
	// unfortunately bog down the race build if they are increased too much.
	if cfg.addOrMaybeAddSemSize == 0 {
		cfg.addOrMaybeAddSemSize = 20
	}

	ambient := store.cfg.AmbientCtx
	ambient.AddLogTag(name, nil)

	if !cfg.acceptsUnsplitRanges && !cfg.needsSpanConfigs {
		log.Dev.Fatalf(ambient.AnnotateCtx(context.Background()),
			"misconfigured queue: acceptsUnsplitRanges=false requires needsSpanConfigs=true; got %+v", cfg)
	}

	bq := baseQueue{
		AmbientContext:   ambient,
		name:             name,
		impl:             impl,
		store:            store,
		queueConfig:      cfg,
		incoming:         make(chan struct{}, 1),
		processSem:       make(chan struct{}, cfg.maxConcurrency),
		addOrMaybeAddSem: quotapool.NewIntPool("queue-add", uint64(cfg.addOrMaybeAddSemSize)),
		addLogN:          log.Every(5 * time.Second),
		getReplica: func(id roachpb.RangeID) (replicaInQueue, error) {
			repl, err := store.GetReplica(id)
			if repl == nil || err != nil {
				// Don't return (*Replica)(nil) as replicaInQueue or NPEs will
				// ensue.
				return nil, err
			}
			return repl, err
		},
	}
	bq.mu.replicas = map[roachpb.RangeID]*replicaItem{}
	bq.mu.maxSize = int64(cfg.maxSize)
	bq.SetDisabled(!cfg.disabledConfig.Get(&store.cfg.Settings.SV))
	cfg.disabledConfig.SetOnChange(&store.cfg.Settings.SV, func(ctx context.Context) {
		bq.SetDisabled(!cfg.disabledConfig.Get(&store.cfg.Settings.SV))
	})

	return &bq
}

// Name returns the name of the queue.
func (bq *baseQueue) Name() string {
	return bq.name
}

// NeedsLease returns whether the queue requires a replica to be leaseholder.
func (bq *baseQueue) NeedsLease() bool {
	return bq.needsLease
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.mu.priorityQ.Len()
}

// PurgatoryLength returns the current size of purgatory.
func (bq *baseQueue) PurgatoryLength() int {
	// Lock processing while measuring the purgatory length. This ensures that
	// no purgatory replicas are concurrently being processed, during which time
	// they are removed from bq.mu.purgatory even though they may be re-added.
	defer bq.lockProcessing()()

	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.mu.purgatory)
}

// SetDisabled turns queue processing off or on as directed.
func (bq *baseQueue) SetDisabled(disabled bool) {
	bq.mu.Lock()
	bq.mu.disabled = disabled
	bq.mu.Unlock()
}

// SetMaxSize sets the max size of the queue.
func (bq *baseQueue) SetMaxSize(maxSize int64) {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	bq.mu.maxSize = maxSize
	// Drop replicas until no longer exceeding the max size. Note: We call
	// removeLocked to match the behavior of addInternal. In theory, only
	// removeFromQueueLocked should be triggered in removeLocked, since the item
	// is in the priority queue, it should not be processing or in the purgatory
	// queue. To be safe, however, we use removeLocked.
	for int64(bq.mu.priorityQ.Len()) > maxSize {
		pqLen := bq.mu.priorityQ.Len()
		bq.full.Inc(1)
		bq.removeLocked(bq.mu.priorityQ.sl[pqLen-1])
	}
}

// lockProcessing locks all processing in the baseQueue. It returns
// a function to unlock processing.
func (bq *baseQueue) lockProcessing() func() {
	semCount := cap(bq.processSem)

	// Drain process semaphore.
	for i := 0; i < semCount; i++ {
		bq.processSem <- struct{}{}
	}

	return func() {
		// Populate process semaphore.
		for i := 0; i < semCount; i++ {
			<-bq.processSem
		}
	}
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(stopper *stop.Stopper) {
	bq.processLoop(stopper)
}

type baseQueueHelper struct {
	bq *baseQueue
}

func (h baseQueueHelper) MaybeAdd(
	ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp,
) {
	h.bq.maybeAdd(ctx, repl, now)
}

func (h baseQueueHelper) Add(
	ctx context.Context, repl replicaInQueue, prio float64, cb processCallback,
) {
	_, err := h.bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), prio, cb)
	if err != nil && log.V(1) {
		log.Dev.Infof(ctx, "during Add: %s", err)
	}
}

type queueHelper interface {
	MaybeAdd(ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp)
	Add(ctx context.Context, repl replicaInQueue, prio float64, cb processCallback)
}

// baseQueueAsyncRateLimited indicates that the base queue async task was rate
// limited and the task was not executed.
var baseQueueAsyncRateLimited = errors.Newf("async task rate limited")

// Async is a more performant substitute for calling AddAsync or MaybeAddAsync
// when many operations are going to be carried out. It invokes the given helper
// function in a goroutine if semaphore capacity is available. If the semaphore
// is at capacity, the 'wait' parameter determines whether to block until
// capacity becomes available or return immediately with an error. Note that if
// the system is shutting down, the function may not be executed regardless of
// the 'wait' value.
//
// The caller is responsible for ensuring that opName does not contain PII.
// (Best is to pass a constant string.)
func (bq *baseQueue) Async(
	ctx context.Context, opName string, wait bool, fn func(ctx context.Context, h queueHelper),
) error {
	if log.V(3) {
		log.Dev.InfofDepth(ctx, 2, "%s", redact.Safe(opName))
	}
	opName += " (" + bq.name + ")"
	bgCtx, hdl, err := bq.store.stopper.GetHandle(
		bq.AnnotateCtx(context.Background()), stop.TaskOpts{
			TaskName:   opName,
			Sem:        bq.addOrMaybeAddSem,
			WaitForSem: wait,
		})
	if err != nil {
		if bq.addLogN.ShouldLog() {
			log.Dev.Infof(ctx, "rate limited in %s: %s", redact.Safe(opName), err)
		}
		return baseQueueAsyncRateLimited
	}
	go func(ctx context.Context) {
		defer hdl.Activate(ctx).Release(ctx)
		fn(ctx, baseQueueHelper{bq})
	}(bgCtx)
	return nil
}

// updateMetricsOnEnqueueSkipped increments the enqueueSkipped metric if it is
// not nil.
func (bq *baseQueue) updateMetricsOnEnqueueSkipped() {
	if bq.enqueueSkipped != nil {
		bq.enqueueSkipped.Inc(1)
	}
}

// updateMetricsOnEnqueueResult increments the enqueueSuccesses or enqueueFailures
// metric if it is not nil.
func (bq *baseQueue) updateMetricsOnEnqueueResult(success bool) {
	if success && bq.enqueueSuccesses != nil {
		bq.enqueueSuccesses.Inc(1)
	} else if !success && bq.enqueueFailures != nil {
		bq.enqueueFailures.Inc(1)
	}
}

// MaybeAddAsync offers the replica to the queue. The queue will only process a
// certain number of these operations concurrently, and will drop (i.e. treat as
// a noop) any additional calls.
func (bq *baseQueue) MaybeAddAsync(
	ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp,
) {
	if err := bq.Async(ctx, "MaybeAdd", false /* wait */, func(ctx context.Context, h queueHelper) {
		h.MaybeAdd(ctx, repl, now)
	}); err != nil {
		bq.updateMetricsOnEnqueueSkipped()
	}
}

// AddAsyncWithCallback is the same as AddAsync, but allows the caller to
// register a process callback that will be invoked when the replica is enqueued
// or processed.
func (bq *baseQueue) AddAsyncWithCallback(
	ctx context.Context, repl replicaInQueue, prio float64, cb processCallback,
) {
	if err := bq.Async(ctx, "Add", true /* wait */, func(ctx context.Context, h queueHelper) {
		h.Add(ctx, repl, prio, cb)
	}); err != nil {
		cb.onEnqueueResult(-1 /*indexOnHeap*/, err)
	}
}

// AddAsync adds the replica to the queue. Unlike MaybeAddAsync, it will wait
// for other operations to finish instead of turning into a noop (because
// unlikely MaybeAdd, Add is not subject to being called opportunistically).
func (bq *baseQueue) AddAsync(ctx context.Context, repl replicaInQueue, prio float64) {
	_ = bq.Async(ctx, "Add", true /* wait */, func(ctx context.Context, h queueHelper) {
		h.Add(ctx, repl, prio, noopProcessCallback)
	})
}

func (bq *baseQueue) maybeAdd(ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp) {
	ctx = repl.AnnotateCtx(ctx)
	ctx = bq.AnnotateCtx(ctx)

	ctx, undo := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer undo()
	if fn := bq.store.TestingKnobs().BaseQueueInterceptor; fn != nil {
		// Passes the context and baseQueue parameters to the interceptor to verify
		// the correct setting of the pprof label within the context.
		fn(ctx, bq)
	}

	bq.mu.Lock()
	stopped := bq.mu.stopped
	disabled := bq.mu.disabled
	bq.mu.Unlock()

	if stopped {
		bq.updateMetricsOnEnqueueSkipped()
		return
	}

	if disabled {
		// The disabled queue bypass is used in tests which enable manual
		// replication, however still require specific range(s) to be processed
		// through the queue.
		bypassDisabled := bq.store.TestingKnobs().BaseQueueDisabledBypassFilter
		if bypassDisabled == nil || !bypassDisabled(repl.GetRangeID()) {
			bq.updateMetricsOnEnqueueSkipped()
			return
		}
	}

	// Load the system config if it's needed.
	confReader, err := bq.replicaCanBeProcessed(ctx, repl, false /* acquireLeaseIfNeeded */)
	if err != nil {
		bq.updateMetricsOnEnqueueSkipped()
		return
	}

	// NB: in production code, this type assertion is always true. In tests,
	// it may not be and shouldQueue will be passed a nil realRepl. These tests
	// know what they're getting into so that's fine.
	realRepl, _ := repl.(*Replica)
	should, priority := bq.impl.shouldQueue(ctx, now, realRepl, confReader)
	if !should {
		bq.updateMetricsOnEnqueueSkipped()
		return
	}

	extConf := bq.skipIfReplicaHasExternalFilesConfig
	if extConf != nil && extConf.Get(&bq.store.cfg.Settings.SV) {
		hasExternal, err := realRepl.HasExternalBytes()
		if err != nil {
			log.Dev.Warningf(ctx, "could not determine if %s has external bytes: %s", realRepl, err)
			bq.updateMetricsOnEnqueueSkipped()
			return
		}
		if hasExternal {
			log.Dev.VInfof(ctx, 1, "skipping %s for %s because it has external bytes", bq.name, realRepl)
			bq.updateMetricsOnEnqueueSkipped()
			return
		}
	}
	_, err = bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), priority, noopProcessCallback)
	if !isExpectedQueueError(err) {
		log.Dev.Errorf(ctx, "unable to add: %+v", err)
	}
}

// addInternal adds the replica the queue with specified priority. If
// the replica is already queued at a lower priority, updates the existing
// priority. Expects the queue lock to be held by caller.
//
// processCallback allows the caller to register a callback that will be invoked
// when the replica is enqueued or processed.
//   - If the replicaItem has not been added to bq.mu.replicas yet, the callback
//     is registered and onEnqueueResult is invoked immediately with the result of
//     the enqueue attempt. If successfully enqueued, onProcessResult will be
//     invoked when processing completes.
//   - If the replicaItem has already been added to bq.mu.replicas, no new
//     callbacks will be registered. onEnqueueResult registered first time will be
//     invoked with the result of enqueue attempts:
//     1. Already processing or in purgatory: invoked with
//     errReplicaAlreadyProcessing/errReplicaAlreadyInPurgatory
//     2. Priority updated: invoked with error = nil and new heap index
//     3. Waiting in queue without priority change: not invoked
//     4. Dropped due to full queue: invoked with
//     errDroppedDueToFullQueueSizeonEnqueueResult registered first time is
//     invoked with the result of this enqueue attempt.
//     5. Other errors: invoked with the error.
//
// NB: callback invokation is not guanranteed since removeFromReplicaSetLocked
// may remove the replica from the queue at any time without invoking them.
func (bq *baseQueue) addInternal(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
	priority float64,
	cb processCallback,
) (added bool, err error) {
	defer func() {
		if err != nil {
			cb.onEnqueueResult(-1 /* indexOnHeap */, err)
            bq.updateMetricsOnEnqueueResult(added)
		}
	}()
	// NB: this is intentionally outside of bq.mu to avoid having to consider
	// lock ordering constraints.
	if !desc.IsInitialized() {
		// We checked this above in MaybeAdd(), but we need to check it
		// again for Add().
		return false, errReplicaNotInitialized
	}

	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped {
		return false, errQueueStopped
	}

	if bq.mu.disabled {
		// The disabled queue bypass is used in tests which enable manual
		// replication, however still require specific range(s) to be processed
		// through the queue.
		bypassDisabled := bq.store.TestingKnobs().BaseQueueDisabledBypassFilter
		if bypassDisabled == nil || !bypassDisabled(desc.RangeID) {
			if log.V(3) {
				log.Dev.Infof(ctx, "queue disabled")
			}
			return false, errQueueDisabled
		}
	}

	// If the replica is currently in purgatory, don't re-add it.
	if _, ok := bq.mu.purgatory[desc.RangeID]; ok {
		cb.onEnqueueResult(-1 /*indexOnHeap*/, errReplicaAlreadyInPurgatory)
		return false, nil
	}

	item, ok := bq.mu.replicas[desc.RangeID]
	if ok {
		// Replica is already processing. Mark to be requeued.
		if item.processing {
			wasRequeued := item.requeue
			item.requeue = true
			cb.onEnqueueResult(-1 /*indexOnHeap*/, errReplicaAlreadyProcessing)
			return !wasRequeued, nil
		}

		// Replica has already been added but at a lower priority; update priority.
		// Don't lower it since the previous queuer may have known more than this
		// one does.
		if priority > item.priority {
			if log.V(1) {
				log.Dev.Infof(ctx, "updating priority: %0.3f -> %0.3f", item.priority, priority)
			}
			bq.mu.priorityQ.update(item, priority)
			// item.index should be updated now based on heap property now.
			cb.onEnqueueResult(item.index /*indexOnHeap*/, nil)
		}
		return false, nil
	}

	if log.V(3) {
		log.Dev.Infof(ctx, "adding: priority=%0.3f", priority)
	}
	item = &replicaItem{rangeID: desc.RangeID, replicaID: replicaID, priority: priority}
	item.registerCallback(cb)
	bq.addLocked(item)

	// If adding this replica has pushed the queue past its maximum size, remove
	// an element. Note that it might not be the lowest priority since heap is not
	// guaranteed to be globally ordered. Ideally, we would remove the lowest
	// priority element, but it would require additional bookkeeping or a linear
	// scan.
	if pqLen := bq.mu.priorityQ.Len(); int64(pqLen) > bq.mu.maxSize {
		replicaItemToDrop := bq.mu.priorityQ.sl[pqLen-1]
		if bq.full != nil {
			bq.full.Inc(1)
		}
		log.Dev.VInfof(ctx, 1, "dropping due to exceeding queue max size: priority=%0.3f, replica=%v",
			priority, replicaItemToDrop.replicaID)
		// TODO(wenyihu6): when we introduce base queue max size cluster setting,
		// remember to invoke this callback when shrinking the size
		for _, callback := range replicaItemToDrop.callbacks {
			callback.onEnqueueResult(-1 /*indexOnHeap*/, errDroppedDueToFullQueueSize)
		}
		bq.removeLocked(replicaItemToDrop)
	}
	// Signal the processLoop that a replica has been added.
	select {
	case bq.incoming <- struct{}{}:
	default:
		// No need to signal again.
	}
	// Note: it may already be dropped or dropped afterwards.
	cb.onEnqueueResult(item.index /*indexOnHeap*/, nil)
	return true, nil
}

// MaybeAddCallback adds a callback to be called when the specified range
// finishes processing. The replica can be in one of several states:
//
//   - waiting: not in mu.replicas
//     Returns false and no callback is executed.
//
//   - queued: in mu.replicas and mu.priorityQ
//     Returns true. onProcessResult is executed when the replica is processed.
//
//   - purgatory: in mu.replicas and mu.purgatory
//     Returns true and the onProcessResult is called immediately with the
//     purgatory error. Note that the onProcessResult may be invoked again when
//     the purgatory finishes processing the replica..
//
//   - processing: only in mu.replicas and currently being processed
//     Returns true and onProcessResult is executed when processing completes.
//     If the replica is currently being processed by the purgatory queue, it
//     will not be in bq.mu.purgatory and the onProcessResult will only execute
//     when the purgatory finishes processing the replica.
//
// If it returns true, onEnqueueResult is invoked on subsequent invocations to
// addInternal as well.
//
// NB: Adding new uses is discouraged without cleaning up the contract of
// processCallback. For example, removeFromReplicaSetLocked may be called
// without invoking these callbacks. See replicaItem.registerCallback for more
// details.
func (bq *baseQueue) MaybeAddCallback(rangeID roachpb.RangeID, cb processCallback) bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if purgatoryErr, ok := bq.mu.purgatory[rangeID]; ok {
		cb.onProcessResult(purgatoryErr)
		return true
	}
	if item, ok := bq.mu.replicas[rangeID]; ok {
		item.registerCallback(cb)
		return true
	}
	return false
}

// MaybeRemove removes the specified replica from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(rangeID roachpb.RangeID) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.mu.stopped {
		return
	}

	if item, ok := bq.mu.replicas[rangeID]; ok {
		ctx := bq.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Dev.Infof(ctx, "%s: removing", item.rangeID)
		}
		bq.removeLocked(item)
	}
}

// processLoop processes the entries in the queue until the provided
// stopper signals exit.
func (bq *baseQueue) processLoop(stopper *stop.Stopper) {
	ctx := bq.AnnotateCtx(context.Background())
	ctx, undo := pprofutil.SetProfilerLabelsFromCtxTags(ctx)
	defer undo()

	done := func() {
		bq.mu.Lock()
		bq.mu.stopped = true
		bq.mu.Unlock()
	}
	if err := stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "queue-loop", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			defer done()

			// nextTime is initially nil; we don't start any timers until the queue
			// becomes non-empty.
			var nextTime <-chan time.Time

			immediately := make(chan time.Time)
			close(immediately)

			for {
				select {
				// Exit on stopper.
				case <-stopper.ShouldQuiesce():
					return

				// Incoming signal sets the next time to process if there were previously
				// no replicas in the queue.
				case <-bq.incoming:
					if nextTime == nil {
						// When a replica is added, wake up immediately. This is mainly
						// to facilitate testing without unnecessary sleeps.
						nextTime = immediately

						// In case we're in a test, still block on the impl.
						bq.impl.timer(0)
					}
				// Process replicas as the timer expires.
				case <-nextTime:
					// Acquire from the process semaphore.
					bq.processSem <- struct{}{}

					repl, priorityAtEnqueue := bq.pop()
					if repl != nil {
						bq.processOneAsyncAndReleaseSem(ctx, repl, stopper, priorityAtEnqueue)
						bq.impl.postProcessScheduled(ctx, repl, priorityAtEnqueue)
					} else {
						// Release semaphore if no replicas were available.
						<-bq.processSem
					}

					if bq.Length() == 0 {
						nextTime = nil
					} else {
						// lastDur will be 0 after the first processing attempt.
						lastDur := bq.lastProcessDuration()
						switch t := bq.impl.timer(lastDur); t {
						case 0:
							nextTime = immediately
						default:
							nextTime = time.After(t)
						}
					}
				}
			}
		}); err != nil {
		done()
	}
}

// processOneAsyncAndReleaseSem processes a replica if possible and releases the
// processSem when the processing is complete.
func (bq *baseQueue) processOneAsyncAndReleaseSem(
	ctx context.Context, repl replicaInQueue, stopper *stop.Stopper, priorityAtEnqueue float64,
) {
	ctx = repl.AnnotateCtx(ctx)
	taskName := bq.processOpName() + " [outer]"
	if err := stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: taskName},
		func(ctx context.Context) {
			// Release semaphore when finished processing.
			defer func() { <-bq.processSem }()
			start := timeutil.Now()
			err := bq.processReplica(ctx, repl, priorityAtEnqueue)
			bq.recordProcessDuration(ctx, timeutil.Since(start))
			bq.finishProcessingReplica(ctx, stopper, repl, err)
		}); err != nil {
		// Release semaphore if we can't start the task, normally this only
		// happens during a system shutdown. If the func is started this will
		// never return an error.
		bq.finishProcessingReplica(ctx, stopper, repl, err)
		log.Dev.Warningf(ctx, "%s: task did not start %v", taskName, err)
		<-bq.processSem
	}
}

// lastProcessDuration returns the duration of the last processing attempt.
func (bq *baseQueue) lastProcessDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&bq.processDur))
}

// recordProcessDuration records the duration of a processing run.
func (bq *baseQueue) recordProcessDuration(ctx context.Context, dur time.Duration) {
	if log.V(2) {
		log.Dev.Infof(ctx, "done %s", dur)
	}
	bq.processingNanos.Inc(dur.Nanoseconds())
	atomic.StoreInt64(&bq.processDur, int64(dur))
}

// processReplica processes a single replica. This should not be
// called externally to the queue. bq.mu.Lock must not be held
// while calling this method.
//
// ctx should already be annotated by both bq.AnnotateCtx() and
// repl.AnnotateCtx().
func (bq *baseQueue) processReplica(
	ctx context.Context, repl replicaInQueue, priorityAtEnqueue float64,
) error {

	ctx, span := tracing.EnsureChildSpan(ctx, bq.Tracer, bq.processOpName())
	defer span.Finish()

	log.VEventf(ctx, 1, "processing replica")

	// Load the system config if it's needed.
	conf, err := bq.replicaCanBeProcessed(ctx, repl, true /* acquireLeaseIfNeeded */)
	if err != nil {
		if errors.Is(err, errMarkNotAcquirableLease) {
			return nil
		}
		log.VErrEventf(ctx, 2, "replica can not be processed now: %s", err)
		return err
	}

	return timeutil.RunWithTimeout(ctx, redact.Sprintf("%s queue process replica %d", bq.name, repl.GetRangeID()),
		bq.processTimeoutFunc(bq.store.ClusterSettings(), repl), func(ctx context.Context) error {
			log.VEventf(ctx, 3, "processing...")
			// NB: in production code, this type assertion is always true. In tests,
			// it may not be and shouldQueue will be passed a nil realRepl. These tests
			// know what they're getting into so that's fine.
			realRepl, _ := repl.(*Replica)
			processed, err := bq.impl.process(ctx, realRepl, conf, priorityAtEnqueue)
			if err != nil {
				return err
			}
			if processed {
				log.VEventf(ctx, 3, "processing... done")
				bq.successes.Inc(1)
			}
			return nil
		})
}

// errMarkNotAcquirableLease Special case lease acquisition errors for cases
// where the lease can't be acquired.
var errMarkNotAcquirableLease = errors.New("lease can't be acquired")

// replicaCanBeProcessed validates that all the conditions for running this
// queue are satisfied according to the queue configuration and the status of
// the replica and its span config. This normalizes the logic for deciding
// whether a queue can be processed. It returns an err if the replica can not be
// processed right now. In some cases we want to attempt to acquire or renew a
// lease if we don't currently have it and the queue requires a lease. This will
// only return a nil SpanConfig if the queue does not require span configs.
func (bq *baseQueue) replicaCanBeProcessed(
	ctx context.Context, repl replicaInQueue, acquireLeaseIfNeeded bool,
) (spanconfig.StoreReader, error) {
	if !repl.IsInitialized() {
		// We checked this when adding the replica, but we need to check it again
		// in case this is a different replica with the same range ID (see #14193).
		// This is possible in the case where the replica was enqueued while not
		// having a replica ID, perhaps due to a pre-emptive snapshot, and has
		// since been removed and re-added at a different replica ID.
		return nil, errors.New("cannot process uninitialized replica")
	}

	// The replica GC queue can process destroyed replicas if it is stuck in
	// destroyReasonMergePending for too long.
	if reason, err := repl.IsDestroyed(); err != nil {
		if !bq.queueConfig.processDestroyedReplicas || reason == destroyReasonRemoved {
			log.VEventf(ctx, 3, "replica destroyed (%s); skipping", err)
			return nil, errors.Wrap(err, "cannot process destroyed replica")
		}
	}

	// The conf is only populated if the queue requires a span config. Otherwise
	// nil is always returned.
	var confReader spanconfig.StoreReader
	if bq.needsSpanConfigs {
		var err error
		confReader, err = bq.store.GetConfReader(ctx)
		if err != nil {
			if log.V(1) || !errors.Is(err, errSpanConfigsUnavailable) {
				log.Dev.Warningf(ctx, "unable to retrieve conf reader, skipping: %v", err)
			}
			return nil, err
		}

		if !bq.acceptsUnsplitRanges {
			// Queue does not accept unsplit ranges. Check to see if the range needs to
			// be spilt because of a span config.
			needsSplit, err := confReader.NeedsSplit(ctx, repl.Desc().StartKey, repl.Desc().EndKey)
			if err != nil {
				log.Dev.Warningf(ctx, "unable to compute NeedsSplit, skipping: %v", err)
				return nil, err
			}
			if needsSplit {
				log.VEventf(ctx, 3, "split needed; skipping")
				return nil, errors.New("split needed; skipping")
			}
		}
	}

	// If the queue requires a replica to have the range lease in
	// order to be processed, check whether this replica has range lease
	// and renew or acquire if necessary.
	if bq.needsLease {
		if acquireLeaseIfNeeded {
			_, pErr := repl.redirectOnOrAcquireLease(ctx)
			if pErr != nil {
				switch v := pErr.GetDetail().(type) {
				case *kvpb.NotLeaseHolderError, *kvpb.RangeNotFoundError:
					log.VEventf(ctx, 3, "%s; skipping", v)
					return nil, errMarkNotAcquirableLease
				}
				log.VErrEventf(ctx, 2, "could not obtain lease: %s", pErr)
				return nil, errors.Wrapf(pErr.GoError(), "%s: could not obtain lease", repl)
			}

			// TODO(baptist): Should this be added to replicaInQueue?
			realRepl, _ := repl.(*Replica)
			pErr = realRepl.maybeSwitchLeaseType(ctx)
			if pErr != nil {
				return nil, pErr.GoError()
			}
		} else {
			// Don't process if we don't own the lease.
			st := repl.CurrentLeaseStatus(ctx)
			if st.IsValid() && !st.OwnedBy(repl.StoreID()) {
				log.VEventf(ctx, 1, "needs lease; not adding: %v", st.Lease)
				// NB: this is an expected error, so make sure it doesn't get
				// logged loudly.
				return nil, benignerror.New(errors.Newf("needs lease, not adding: %v", st.Lease))
			}
		}
	}
	return confReader, nil
}

// IsPurgatoryError returns true iff the given error is a purgatory error.
func IsPurgatoryError(err error) (PurgatoryError, bool) {
	var purgErr PurgatoryError
	return purgErr, errors.As(err, &purgErr)
}

// assertInvariants codifies the guarantees upheld by the data structures in the
// base queue.
// 1. In summary, a replica is one of:
// - "queued" and in mu.replicas and mu.priorityQ
// - "processing" and only in mu.replicas
// - "purgatory" and in mu.replicas and mu.purgatory
// 2. For every item in bq.mu.priorityQ.sl, bq.mu.purgatory, and bq.mu.replicas,
// assertOnReplicaItem callback is called with the item. Note that we expect
// items in priorityQ and purgatory to be in replicas.
//
// Note that in particular, nothing is ever in both mu.priorityQ and
// mu.purgatory.
func (bq *baseQueue) assertInvariants(assertOnReplicaItem func(item *replicaItem)) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	ctx := bq.AnnotateCtx(context.Background())
	for _, item := range bq.mu.priorityQ.sl {
		assertOnReplicaItem(item)
		if item.processing {
			log.Dev.Fatalf(ctx, "processing item found in prioQ: %v", item)
		}
		if _, inReplicas := bq.mu.replicas[item.rangeID]; !inReplicas {
			log.Dev.Fatalf(ctx, "item found in prioQ but not in mu.replicas: %v", item)
		}
		if _, inPurg := bq.mu.purgatory[item.rangeID]; inPurg {
			log.Dev.Fatalf(ctx, "item found in prioQ and purgatory: %v", item)
		}
	}
	for rangeID := range bq.mu.purgatory {
		item, inReplicas := bq.mu.replicas[rangeID]
		assertOnReplicaItem(item)
		if !inReplicas {
			log.Dev.Fatalf(ctx, "item found in purg but not in mu.replicas: %v", item)
		}
		if item.processing {
			log.Dev.Fatalf(ctx, "processing item found in purgatory: %v", item)
		}
		// NB: we already checked above that item not in prioQ.
	}

	// At this point we know that the purgatory in prioQ are distinct, and we
	// also know that no processing replicas are tracked in each. Let's check
	// that there aren't any non-processing replicas *only* in bq.mu.replicas.
	var nNotProcessing int
	for _, item := range bq.mu.replicas {
		assertOnReplicaItem(item)
		if !item.processing {
			nNotProcessing++
		}
	}
	if nNotProcessing != len(bq.mu.purgatory)+len(bq.mu.priorityQ.sl) {
		log.Dev.Fatalf(ctx, "have %d non-processing replicas in mu.replicas, "+
			"but %d in purgatory and %d in prioQ; the latter two should add up"+
			"to the former", nNotProcessing, len(bq.mu.purgatory), len(bq.mu.priorityQ.sl))
	}
}

// finishProcessingReplica handles the completion of a replica process attempt.
// It removes the replica from the replica set and may re-enqueue the replica or
// add it to purgatory.
func (bq *baseQueue) finishProcessingReplica(
	ctx context.Context, stopper *stop.Stopper, repl replicaInQueue, err error,
) {
	bq.mu.Lock()
	// Remove item from replica set completely. We may add it
	// back in down below.
	item := bq.mu.replicas[repl.GetRangeID()]
	processing := item.processing
	callbacks := item.callbacks
	requeue := item.requeue
	priority := item.priority
	item.callbacks = nil
	bq.removeFromReplicaSetLocked(repl.GetRangeID())
	item = nil // prevent accidental use below
	bq.mu.Unlock()

	if !processing {
		log.Dev.Fatalf(ctx, "%s: attempt to remove non-processing replica %v", bq.name, repl)
	}

	// Call any registered callbacks.
	for _, cb := range callbacks {
		cb.onProcessResult(err)
	}

	// Handle failures.
	if err != nil {
		benign := benignerror.IsBenign(err)

		// Increment failures metric.
		//
		// TODO(tschottdorf): once we start asserting zero failures in tests
		// (and production), move benign failures into a dedicated category.
		bq.failures.Inc(1)

		// Determine whether a failure is a purgatory error. If it is, add
		// the failing replica to purgatory. Note that even if the item was
		// scheduled to be requeued, we ignore this if we add the replica to
		// purgatory.
		if purgErr, ok := IsPurgatoryError(err); ok {
			bq.mu.Lock()
			bq.addToPurgatoryLocked(ctx, stopper, repl, purgErr, priority /*priorityAtEnqueue*/, callbacks /*processCallback*/)
			bq.mu.Unlock()
			return
		}

		// If not a benign or purgatory error, log.
		if !benign {
			log.Dev.Errorf(ctx, "%v", err)
		}
	}

	// Maybe add replica back into queue, if requested.
	if requeue {
		bq.maybeAdd(ctx, repl, bq.store.Clock().NowAsClockTimestamp())
	}
}

// addToPurgatoryLocked adds the specified replica to the purgatory queue, which
// holds replicas which have failed processing.
func (bq *baseQueue) addToPurgatoryLocked(
	ctx context.Context,
	stopper *stop.Stopper,
	repl replicaInQueue,
	purgErr PurgatoryError,
	priorityAtEnqueue float64,
	cbs []processCallback,
) {
	bq.mu.AssertHeld()

	// Check whether the queue supports purgatory errors. If not then something
	// went wrong because a purgatory error should not have ended up here.
	if bq.impl.purgatoryChan() == nil {
		log.Dev.Errorf(ctx, "queue does not support purgatory errors, but saw %v", purgErr)
		return
	}

	if log.V(1) {
		log.Dev.Infof(ctx, "purgatory: %v", purgErr)
	}

	if _, found := bq.mu.replicas[repl.GetRangeID()]; found {
		// Don't add to purgatory if already in the queue (again). We need to
		// uphold the invariant that a replica is never both in the priority
		// queue and the purgatory at the same time or bad things will happen.
		// See bq.assertInvariants and:
		// https://github.com/cockroachdb/cockroach/issues/36277#issuecomment-482659939
		return
	}

	item := &replicaItem{
		rangeID:   repl.GetRangeID(),
		replicaID: repl.ReplicaID(),
		index:     -1,
		priority:  priorityAtEnqueue,
		callbacks: cbs,
	}

	bq.mu.replicas[repl.GetRangeID()] = item

	defer func() {
		bq.purgatory.Update(int64(len(bq.mu.purgatory)))
	}()

	// If purgatory already exists, just add to the map and we're done.
	if bq.mu.purgatory != nil {
		bq.mu.purgatory[repl.GetRangeID()] = purgErr
		return
	}

	// Otherwise, create purgatory and start processing.
	bq.mu.purgatory = map[roachpb.RangeID]PurgatoryError{
		repl.GetRangeID(): purgErr,
	}

	workerCtx := bq.AnnotateCtx(context.Background())
	_ = stopper.RunAsyncTaskEx(workerCtx, stop.TaskOpts{TaskName: bq.name + ".purgatory", SpanOpt: stop.SterileRootSpan}, func(ctx context.Context) {
		ticker := time.NewTicker(purgatoryReportInterval)
		defer ticker.Stop()
		for {
			select {
			case <-bq.impl.updateChan():
				if bq.processReplicasInPurgatory(ctx, stopper) {
					return
				}
			case <-bq.impl.purgatoryChan():
				if bq.processReplicasInPurgatory(ctx, stopper) {
					return
				}
			case <-ticker.C:
				// Report purgatory status.
				bq.mu.Lock()
				errMap := map[string]int{}
				for _, err := range bq.mu.purgatory {
					errMap[err.Error()]++
				}
				bq.mu.Unlock()
				for errStr, count := range errMap {
					log.Dev.Errorf(ctx, "%d replicas failing with %q", count, errStr)
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	},
	)
}

// processReplicasInPurgatory processes replicas currently in the queue's
// purgatory.
func (bq *baseQueue) processReplicasInPurgatory(
	ctx context.Context, stopper *stop.Stopper,
) (purgatoryCleared bool) {
	func() {
		// Acquire from the process semaphore, release when done.
		bq.processSem <- struct{}{}
		defer func() { <-bq.processSem }()

		// Remove all items from purgatory into a copied slice.
		bq.mu.Lock()
		ranges := make([]*replicaItem, 0, len(bq.mu.purgatory))
		for rangeID := range bq.mu.purgatory {
			item := bq.mu.replicas[rangeID]
			if item == nil {
				log.Dev.Fatalf(ctx, "r%d is in purgatory but not in replicas", rangeID)
			}
			item.setProcessing()
			ranges = append(ranges, item)
			bq.removeFromPurgatoryLocked(item)
		}
		bq.mu.Unlock()

		for _, item := range ranges {
			repl, err := bq.getReplica(item.rangeID)
			if err != nil || item.replicaID != repl.ReplicaID() {
				bq.mu.Lock()
				bq.removeFromReplicaSetLocked(item.rangeID)
				bq.mu.Unlock()
				continue
			}
			annotatedCtx := repl.AnnotateCtx(ctx)
			if stopper.RunTask(
				annotatedCtx, bq.processOpName(), func(ctx context.Context) {
					err = bq.processReplica(ctx, repl, item.priority /*priorityAtEnqueue*/)
					bq.finishProcessingReplica(ctx, stopper, repl, err)
				},
			) != nil {
				// NB: We do not need to worry about removing any unprocessed replicas
				// from the replica set here, as RunTask will only return an error when
				// the stopper is quiescing or stopping -- meaning the process is
				// shutting down.
				return
			}
		}
	}()

	// Clean up purgatory, if empty.
	bq.mu.Lock()
	if len(bq.mu.purgatory) == 0 {
		log.Dev.Infof(ctx, "purgatory is now empty")
		bq.mu.purgatory = nil
		bq.mu.Unlock()
		return true /* purgatoryCleared */
	}
	bq.mu.Unlock()
	return false /* purgatoryCleared */
}

// pop dequeues the highest priority replica, if any, in the queue. The
// replicaItem corresponding to the returned Replica will be moved to the
// "processing" state and should be cleaned up by calling
// finishProcessingReplica once the Replica has finished processing.
func (bq *baseQueue) pop() (replicaInQueue, float64) {
	bq.mu.Lock()
	for {
		if bq.mu.priorityQ.Len() == 0 {
			bq.mu.Unlock()
			return nil, 0
		}
		item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
		if item.processing {
			log.Dev.Fatalf(bq.AnnotateCtx(context.Background()), "%s pulled processing item from heap: %v", bq.name, item)
		}
		// We are saving priority because the state is reset by setProcessing()
		priority := item.priority
		item.setProcessing()
		bq.pending.Update(int64(bq.mu.priorityQ.Len()))
		bq.mu.Unlock()

		repl, _ := bq.getReplica(item.rangeID)
		if repl != nil && item.replicaID == repl.ReplicaID() {
			return repl, priority
		}
		// Replica not found or was recreated with a new replica ID, remove from
		// set and try again.
		bq.mu.Lock()
		bq.removeFromReplicaSetLocked(item.rangeID)
	}
}

// addLocked adds an element to the priority queue. Caller must hold mutex.
func (bq *baseQueue) addLocked(item *replicaItem) {
	heap.Push(&bq.mu.priorityQ, item)
	bq.pending.Update(int64(bq.mu.priorityQ.Len()))
	bq.mu.replicas[item.rangeID] = item
}

// removeLocked removes an element from purgatory (if it's experienced an
// error) or from the priority queue by index. Caller must hold mutex.
func (bq *baseQueue) removeLocked(item *replicaItem) {
	if item.processing {
		// The item is processing. We can't intererupt the processing
		// or remove it from the replica set yet, but we can make sure
		// it doesn't get requeued.
		item.requeue = false
	} else {
		if _, inPurg := bq.mu.purgatory[item.rangeID]; inPurg {
			bq.removeFromPurgatoryLocked(item)
		} else if item.index >= 0 {
			bq.removeFromQueueLocked(item)
		} else {
			log.Dev.Fatalf(bq.AnnotateCtx(context.Background()),
				"item for r%d is only in replicas map, but is not processing",
				item.rangeID,
			)
		}
		bq.removeFromReplicaSetLocked(item.rangeID)
	}
}

// Caller must hold mutex.
func (bq *baseQueue) removeFromPurgatoryLocked(item *replicaItem) {
	delete(bq.mu.purgatory, item.rangeID)
	bq.purgatory.Update(int64(len(bq.mu.purgatory)))
}

// Caller must hold mutex.
func (bq *baseQueue) removeFromQueueLocked(item *replicaItem) {
	heap.Remove(&bq.mu.priorityQ, item.index)
	bq.pending.Update(int64(bq.mu.priorityQ.Len()))
}

// Caller must hold mutex.
func (bq *baseQueue) removeFromReplicaSetLocked(rangeID roachpb.RangeID) {
	if _, found := bq.mu.replicas[rangeID]; !found {
		log.Dev.Fatalf(bq.AnnotateCtx(context.Background()),
			"attempted to remove r%d from queue, but it isn't in it",
			rangeID,
		)
	}
	delete(bq.mu.replicas, rangeID)
}

// DrainQueue locks the queue and processes the remaining queued replicas. It
// processes the replicas in the order they're queued in, one at a time.
// Exposed for testing only.
func (bq *baseQueue) DrainQueue(ctx context.Context, stopper *stop.Stopper) {
	// Lock processing while draining. This prevents the main process
	// loop from racing with this method and ensures that any replicas
	// queued up when this method was called will be processed by the
	// time it returns.
	defer bq.lockProcessing()()

	ctx = bq.AnnotateCtx(ctx)
	for repl, _ := bq.pop(); repl != nil; repl, _ = bq.pop() {
		annotatedCtx := repl.AnnotateCtx(ctx)
		if _, err := bq.replicaCanBeProcessed(annotatedCtx, repl, false); err != nil {
			bq.finishProcessingReplica(annotatedCtx, stopper, repl, err)
		} else {
			err = bq.processReplica(annotatedCtx, repl, -1 /*priorityAtEnqueue*/)
			bq.finishProcessingReplica(annotatedCtx, stopper, repl, err)
		}
	}
}

func (bq *baseQueue) processOpName() string {
	return fmt.Sprintf("queue.%s: processing replica", bq.name)
}
