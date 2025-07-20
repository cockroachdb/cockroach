// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type stringMatcher interface {
	MatchString(string) bool
}

type allMatcher struct{}

func (allMatcher) MatchString(string) bool {
	return true
}

// This is how multiple callback registrations are handled:
/*
+------------------+     +------------------+     +------------------+
| Callback Reg 1   |     | Callback Reg 2   | ... | Callback Reg N   |
| Pattern: "key1.*"|     | Pattern: "key2.*"| ... | Pattern: "keyN.*"|
+------------------+     +------------------+     +------------------+
        |                        |                        |
        v                        v                        v
+------------------+     +------------------+     +------------------+
| callbackWorker 1 |     | callbackWorker 2 | ... | callbackWorker N |
| - callbackCh     |     | - callbackCh     | ... | - callbackCh     |
| - stopperCh      |     | - stopperCh      | ... | - stopperCh      |
| - workQueue      |     | - workQueue      | ... | - workQueue      |
+------------------+     +------------------+     +------------------+
        |                        |                        |
        v                        v                        v
+------------------+     +------------------+     +------------------+
| Worker Goroutine |     | Worker Goroutine | ... | Worker Goroutine |
| 1                |     | 2                | ... | N                |
+------------------+     +------------------+     +------------------+
*/
// When a new info is added, it is checked against all the callback matchers.
// The work is added inside each workQueue which matches the info key.
// Each worker goroutine independently processes its own workQueue in FIFO
// order.

// callback holds regexp pattern match, GossipCallback method, and a queue of
// remaining work items.
type callback struct {
	matcher   stringMatcher
	method    Callback
	redundant bool
	// cw contains all the information needed to orchestrate, schedule, and run
	// callbacks for this specific matcher.
	cw *callbackWork
}

// callbackWorkItem is a struct that contains the information needed to run
// a callback.
type callbackWorkItem struct {
	// schedulingTime is the time when the callback was scheduled.
	schedulingTime time.Time
	method         Callback
	// key, content, origTimestamp are the parameters that will be passed to the
	// callback method. They are based on the infos added to the infostore.
	key           string
	content       roachpb.Value
	origTimestamp int64
}

type callbackWork struct {
	// callbackCh is used to signal the callback worker to execute the work.
	callbackCh chan struct{}
	// stopperCh is used to signal the callback worker to stop.
	stopperCh chan struct{}
	mu        struct {
		syncutil.Mutex
		// workQueue contains the queue of callbacks that need to be called.
		workQueue []callbackWorkItem
	}
}

func newCallbackWork() *callbackWork {
	return &callbackWork{
		callbackCh: make(chan struct{}, 1),
		stopperCh:  make(chan struct{}, 1),
		mu: struct {
			syncutil.Mutex
			workQueue []callbackWorkItem
		}{
			workQueue: make([]callbackWorkItem, 0),
		},
	}
}

// infoStore objects manage maps of Info objects. They maintain a
// sequence number generator which they use to allocate new info
// objects.
//
// infoStores can be queried for incremental updates occurring since a
// specified map of peer node high water timestamps.
//
// infoStores can be combined using deltas from peer nodes.
//
// infoStores are not thread safe.
type infoStore struct {
	log.AmbientContext

	nodeID  *base.NodeIDContainer
	stopper *stop.Stopper
	metrics Metrics

	Infos           infoMap                  `json:"infos,omitempty"` // Map from key to info
	NodeAddr        util.UnresolvedAddr      `json:"-"`               // Address of node owning this info store: "host:port"
	highWaterStamps map[roachpb.NodeID]int64 // Per-node information for gossip peers
	callbacks       []*callback
}

var monoTime struct {
	syncutil.Mutex
	last int64
}

var errNotFresh = errors.New("info not fresh")

// monotonicUnixNano returns a monotonically increasing value for
// nanoseconds in Unix time. Since equal times are ignored with
// updates to infos, we're careful to avoid incorrectly ignoring a
// newly created value in the event one is created within the same
// nanosecond. Really unlikely except for the case of unittests, but
// better safe than sorry.
func monotonicUnixNano() int64 {
	monoTime.Lock()
	defer monoTime.Unlock()

	now := timeutil.Now().UnixNano()
	if now <= monoTime.last {
		now = monoTime.last + 1
	}
	monoTime.last = now
	return now
}

// ratchetMonotonic increases the monotonic clock to be at least v. Used to
// guarantee that clock values generated by the local node ID always increase
// even in the presence of local infos that were received from a remote with a
// timestamp in the future (which can happen in the presence of backward clock
// jumps and a crash).
func ratchetMonotonic(v int64) {
	monoTime.Lock()
	if monoTime.last < v {
		monoTime.last = v
	}
	monoTime.Unlock()
}

// ratchetHighWaterStamp sets stamps[nodeID] to max(stamps[nodeID], newStamp).
func ratchetHighWaterStamp(stamps map[roachpb.NodeID]int64, nodeID roachpb.NodeID, newStamp int64) {
	if nodeID != 0 && stamps[nodeID] < newStamp {
		stamps[nodeID] = newStamp
	}
}

// mergeHighWaterStamps merges the high water stamps in src into dest by
// performing a ratchet operation for each stamp in src. The existing stamps in
// dest will either remain the same (if they are smaller than the corresponding
// stamp in src) or be bumped to the higher value in src.
func mergeHighWaterStamps(dest *map[roachpb.NodeID]int64, src map[roachpb.NodeID]int64) {
	if *dest == nil {
		*dest = src
		return
	}
	for nodeID, newStamp := range src {
		ratchetHighWaterStamp(*dest, nodeID, newStamp)
	}
}

// String returns a string representation of an infostore.
func (is *infoStore) String() string {
	var buf strings.Builder
	if infoCount := len(is.Infos); infoCount > 0 {
		fmt.Fprintf(&buf, "infostore with %d info(s): ", infoCount)
	} else {
		return "infostore (empty)"
	}

	prepend := ""

	if err := is.visitInfos(func(key string, i *Info) error {
		fmt.Fprintf(&buf, "%sinfo %q: %+v", prepend, key, i.Value)
		prepend = ", "
		return nil
	}, false /* deleteExpired */); err != nil {
		// This should never happen because the func we pass above never errors out.
		panic(err)
	}
	return buf.String()
}

// newInfoStore allocates and returns a new infoStore.
func newInfoStore(
	ambient log.AmbientContext,
	nodeID *base.NodeIDContainer,
	nodeAddr util.UnresolvedAddr,
	stopper *stop.Stopper,
	metrics Metrics,
) *infoStore {
	is := &infoStore{
		AmbientContext:  ambient,
		nodeID:          nodeID,
		stopper:         stopper,
		metrics:         metrics,
		Infos:           make(infoMap),
		NodeAddr:        nodeAddr,
		highWaterStamps: map[roachpb.NodeID]int64{},
	}
	return is
}

// cleanupCallbackMetric decrements the callback metric by the number of remaining
// items in the queue since they will never be processed. This is called when the
// callback worker is stopped to avoid having the wrong metric value.
// This should only be called when no new work can be added to the queue.
func (is *infoStore) cleanupCallbackMetric(cw *callbackWork) {
	cw.mu.Lock()
	remainingItems := len(cw.mu.workQueue)
	if remainingItems > 0 {
		is.metrics.CallbacksPending.Dec(int64(remainingItems))
	}
	cw.mu.Unlock()
}

// launchCallbackWorker launches a worker goroutine that is responsible for
// executing callbacks for one registered callback pattern.
func (is *infoStore) launchCallbackWorker(ambient log.AmbientContext, cw *callbackWork) {
	bgCtx := ambient.AnnotateCtx(context.Background())
	_ = is.stopper.RunAsyncTask(bgCtx, "callback worker", func(ctx context.Context) {
		// If we exit the loop, we are never going to process the work in the queues anymore, so
		// clean up the pending callbacks metric.
		defer is.cleanupCallbackMetric(cw)
		for {
			for {
				cw.mu.Lock()
				wq := cw.mu.workQueue
				cw.mu.workQueue = nil
				cw.mu.Unlock()

				if len(wq) == 0 {
					break
				}

				// Execute all the callbacks in the queue, making sure to update the
				// metrics accordingly.
				for _, work := range wq {
					afterQueue := timeutil.Now()
					queueDur := afterQueue.Sub(work.schedulingTime)
					is.metrics.CallbacksPending.Dec(1)
					if queueDur >= minCallbackDurationToRecord {
						is.metrics.CallbacksPendingDuration.RecordValue(queueDur.Nanoseconds())
					}

					work.method(work.key, work.content, work.origTimestamp)

					afterProcess := timeutil.Now()
					processDur := afterProcess.Sub(afterQueue)
					is.metrics.CallbacksProcessed.Inc(1)
					if processDur > minCallbackDurationToRecord {
						is.metrics.CallbacksProcessingDuration.RecordValue(processDur.Nanoseconds())
					}
					afterQueue = afterProcess // update for next iteration
				}
			}

			select {
			case <-cw.callbackCh:
				// New work has just arrived.
			case <-is.stopper.ShouldQuiesce():
				return
			case <-cw.stopperCh:
				return
			}
		}
	})
}

// newInfo allocates and returns a new info object using the specified
// value and time-to-live.
func (is *infoStore) newInfo(val []byte, ttl time.Duration) *Info {
	nodeID := is.nodeID.Get()
	if nodeID == 0 {
		panic("gossip infostore's NodeID is 0")
	}
	now := monotonicUnixNano()
	ttlStamp := now + int64(ttl)
	if ttl == 0 {
		ttlStamp = math.MaxInt64
	}
	v := roachpb.MakeValueFromBytesAndTimestamp(val, hlc.Timestamp{WallTime: now})
	return &Info{
		Value:    v,
		TTLStamp: ttlStamp,
		NodeID:   nodeID,
	}
}

// getInfo returns the Info at key. Returns nil when key is not present
// in the infoStore. Does not modify the infoStore.
func (is *infoStore) getInfo(key string) *Info {
	if info, ok := is.Infos[key]; ok {
		// Check TTL and ignore if too old.
		if !info.expired(monotonicUnixNano()) {
			return info
		}
	}
	return nil
}

// addInfo adds or updates an info in the infos map.
//
// Returns nil if info was added; error otherwise.
func (is *infoStore) addInfo(key string, i *Info) error {
	if i.NodeID == 0 {
		panic("gossip info's NodeID is 0")
	}
	// Only replace an existing info if new timestamp is greater, or if
	// timestamps are equal, but new hops is smaller.
	existingInfo, ok := is.Infos[key]
	if ok {
		iNanos := i.Value.Timestamp.WallTime
		existingNanos := existingInfo.Value.Timestamp.WallTime
		if iNanos < existingNanos || (iNanos == existingNanos && i.Hops >= existingInfo.Hops) {
			return errNotFresh
		}
	}
	if i.OrigStamp == 0 {
		i.Value.InitChecksum([]byte(key))
		i.OrigStamp = monotonicUnixNano()
		if highWaterStamp, ok := is.highWaterStamps[i.NodeID]; ok && highWaterStamp >= i.OrigStamp {
			// Report both timestamps in the crash.
			log.Fatalf(context.Background(),
				"high water stamp %d >= %d", redact.Safe(highWaterStamp), redact.Safe(i.OrigStamp))
		}
	}
	// Update info map.
	is.Infos[key] = i
	// Update the high water timestamp & min hops for the originating node.
	ratchetHighWaterStamp(is.highWaterStamps, i.NodeID, i.OrigStamp)
	changed := existingInfo == nil ||
		!bytes.Equal(existingInfo.Value.RawBytes, i.Value.RawBytes)
	is.processCallbacks(key, i.Value, i.OrigStamp, changed)
	return nil
}

// getHighWaterStamps returns a copy of the high water stamps map of
// gossip peer info maintained by this infostore. Does not modify
// the infoStore.
func (is *infoStore) getHighWaterStamps() map[roachpb.NodeID]int64 {
	copy := make(map[roachpb.NodeID]int64, len(is.highWaterStamps))
	for k, hws := range is.highWaterStamps {
		copy[k] = hws
	}
	return copy
}

// getHighWaterStampsWithDiff returns a copy of the high water stamps that are
// different from the ones provided in prevFull. It also returns a full map of
// updated high water stamps. The caller should use this in subsequent calls to
// this method.
func (is *infoStore) getHighWaterStampsWithDiff(
	prevFull map[roachpb.NodeID]int64,
) (newFull, diff map[roachpb.NodeID]int64) {
	diff = make(map[roachpb.NodeID]int64)
	for k, hws := range is.highWaterStamps {
		if prevFull[k] != hws {
			prevFull[k] = hws
			diff[k] = hws
		}
	}
	return prevFull, diff
}

// registerCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern. Returns a function to unregister the callback.
// Note: the callback may fire after being unregistered.
func (is *infoStore) registerCallback(
	pattern string, method Callback, opts ...CallbackOption,
) func() {
	var matcher stringMatcher
	if pattern == ".*" {
		matcher = allMatcher{}
	} else {
		matcher = regexp.MustCompile(pattern)
	}
	cb := &callback{matcher: matcher, method: method}
	for _, opt := range opts {
		opt.apply(cb)
	}

	cb.cw = newCallbackWork()
	is.launchCallbackWorker(is.AmbientContext, cb.cw)
	is.callbacks = append(is.callbacks, cb)

	if err := is.visitInfos(func(key string, i *Info) error {
		if matcher.MatchString(key) {
			is.runCallbacks(key, i.Value, i.OrigStamp, cb)
		}
		return nil
	}, true /* deleteExpired */); err != nil {
		panic(err)
	}

	return func() {
		// Stop the callback worker's goroutine.
		cb.cw.stopperCh <- struct{}{}

		// Remove the callback from the list.
		for i, targetCB := range is.callbacks {
			if targetCB == cb {
				numCBs := len(is.callbacks)
				is.callbacks[i] = is.callbacks[numCBs-1]
				is.callbacks[numCBs-1] = nil // for GC
				is.callbacks = is.callbacks[:numCBs-1]
				break
			}
		}
	}
}

// processCallbacks processes callbacks for the specified key by
// matching each callback's regular expression against the key and invoking
// the corresponding callback method on a match.
func (is *infoStore) processCallbacks(
	key string, content roachpb.Value, origTimestamp int64, changed bool,
) {
	var callbacks []*callback
	for _, cb := range is.callbacks {
		if (changed || cb.redundant) && cb.matcher.MatchString(key) {
			callbacks = append(callbacks, cb)
		}
	}
	is.runCallbacks(key, content, origTimestamp, callbacks...)
}

// runCallbacks receives a list of callbacks and contents that match the key.
// It adds work to the callback work slices, and signals the associated callback
// workers to execute the work.
func (is *infoStore) runCallbacks(
	key string, content roachpb.Value, origTimestamp int64, callbacks ...*callback,
) {
	// Check if the stopper is quiescing. If so, do not add the callbacks to the
	// callback work list because they won't be processed anyways.
	select {
	case <-is.stopper.ShouldQuiesce():
		return
	default:
	}

	// Add the callbacks to the callback work list.
	beforeQueue := timeutil.Now()
	for _, cb := range callbacks {
		cb.cw.mu.Lock()
		is.metrics.CallbacksPending.Inc(1)
		cb.cw.mu.workQueue = append(cb.cw.mu.workQueue, callbackWorkItem{
			schedulingTime: beforeQueue,
			method:         cb.method,
			key:            key,
			content:        content,
			origTimestamp:  origTimestamp,
		})
		cb.cw.mu.Unlock()

		// Signal the associated callback worker goroutine. Callbacks run in a
		// goroutine to avoid mutex reentry. We also guarantee callbacks are run
		// in order such that if a key is updated twice in succession, the second
		// callback will never be run before the first.
		select {
		case cb.cw.callbackCh <- struct{}{}:
		default:
		}
	}
}

// visitInfos implements a visitor pattern to run the visitInfo function against
// each info in turn. If deleteExpired is specified as true then the method will
// delete any infos that it finds which are expired, so it may modify the
// infoStore. If it is specified as false, the method will ignore expired infos
// without deleting them or modifying the infoStore.
func (is *infoStore) visitInfos(visitInfo func(string, *Info) error, deleteExpired bool) error {
	now := monotonicUnixNano()

	if visitInfo != nil {
		for k, i := range is.Infos {
			if i.expired(now) {
				if deleteExpired {
					delete(is.Infos, k)
				}
				continue
			}
			if err := visitInfo(k, i); err != nil {
				return err
			}
		}
	}

	return nil
}

// combine combines an incremental delta with the current infoStore.
// All hop distances on infos are incremented to indicate they've
// arrived from an external source. Returns the count of "fresh"
// infos in the provided delta.
func (is *infoStore) combine(
	infos map[string]*Info, nodeID roachpb.NodeID,
) (freshCount int, err error) {
	localNodeID := is.nodeID.Get()
	for key, i := range infos {
		if i.NodeID == localNodeID {
			ratchetMonotonic(i.OrigStamp)
		}

		infoCopy := *i
		infoCopy.Hops++
		infoCopy.PeerID = nodeID
		if infoCopy.OrigStamp == 0 {
			panic(errors.Errorf("combining info from n%d with 0 original timestamp", nodeID))
		}
		// errNotFresh errors from addInfo are ignored; they indicate that
		// the data in *is is newer than in *delta.
		if addErr := is.addInfo(key, &infoCopy); addErr == nil {
			freshCount++
		} else if !errors.Is(addErr, errNotFresh) {
			err = addErr
		}
	}
	return
}

// delta returns a map of infos which have originating timestamps
// newer than the high water timestamps indicated by the supplied
// map (which is taken from the perspective of the peer node we're
// taking this delta for).
//
// May modify the infoStore.
func (is *infoStore) delta(highWaterTimestamps map[roachpb.NodeID]int64) map[string]*Info {
	infos := make(map[string]*Info)
	// Compute delta of infos.
	if err := is.visitInfos(func(key string, i *Info) error {
		if i.isFresh(highWaterTimestamps[i.NodeID]) {
			infos[key] = i
		}
		return nil
	}, true /* deleteExpired */); err != nil {
		panic(err)
	}

	return infos
}

// populateMostDistantMarkers adds the node ID infos to the infos map. The node
// ID infos are used as markers in the mostDistant calculation and need to be
// propagated regardless of high water stamps.
func (is *infoStore) populateMostDistantMarkers(infos map[string]*Info) {
	if err := is.visitInfos(func(key string, i *Info) error {
		if IsNodeDescKey(key) {
			infos[key] = i
		}
		return nil
	}, true /* deleteExpired */); err != nil {
		panic(err)
	}
}

// mostDistant returns the most distant gossip node known to the store
// as well as the number of hops to reach it.
//
// Uses haveOutgoingConn to check for whether or not this node is already
// in the process of connecting to a given node (but haven't yet received
// Infos from it) for the purposes of excluding them from the result.
// This check is particularly useful if mostDistant is called multiple times
// in quick succession.
//
// May modify the infoStore.
func (is *infoStore) mostDistant(
	hasOutgoingConn func(roachpb.NodeID) bool,
) (roachpb.NodeID, uint32) {
	localNodeID := is.nodeID.Get()
	var nodeID roachpb.NodeID
	var maxHops uint32
	if err := is.visitInfos(func(key string, i *Info) error {
		// Only consider NodeID keys here because they're re-gossiped every time a
		// node restarts and periodically after that, so their Hops values are more
		// likely to be accurate than keys which are rarely re-gossiped, which can
		// acquire unreliably high Hops values in some pathological cases such as
		// those described in #9819.
		if i.NodeID != localNodeID && i.Hops > maxHops &&
			IsNodeDescKey(key) && !hasOutgoingConn(i.NodeID) {
			maxHops = i.Hops
			nodeID = i.NodeID
		}
		return nil
	}, true /* deleteExpired */); err != nil {
		panic(err)
	}
	return nodeID, maxHops
}

// leastUseful determines which node ID from amongst the set is
// currently contributing the least. Returns the node ID. If nodes is
// empty, returns 0.
//
// May modify the infoStore.
func (is *infoStore) leastUseful(nodes nodeSet) roachpb.NodeID {
	contrib := make(map[roachpb.NodeID]map[roachpb.NodeID]struct{}, nodes.len())
	for node := range nodes.nodes {
		contrib[node] = map[roachpb.NodeID]struct{}{}
	}
	if err := is.visitInfos(func(key string, i *Info) error {
		if _, ok := contrib[i.PeerID]; !ok {
			contrib[i.PeerID] = map[roachpb.NodeID]struct{}{}
		}
		contrib[i.PeerID][i.NodeID] = struct{}{}
		return nil
	}, true /* deleteExpired */); err != nil {
		panic(err)
	}

	least := math.MaxInt32
	var leastNode roachpb.NodeID
	for id, m := range contrib {
		count := len(m)
		if nodes.hasNode(id) {
			if count < least {
				least = count
				leastNode = id
			}
		}
	}
	return leastNode
}
