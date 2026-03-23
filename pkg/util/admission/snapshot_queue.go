// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type snapshotWorkItem struct {
	admitCh        chan bool
	count          int64
	enqueueingTime time.Time
	mu             struct {
		// These fields are updated after creation. The mutex in SnapshotQueue must
		// be held to read and write to these fields.

		// The granted value transitions at most once from false to true. Granting
		// can race with context cancellation, so when context cancellation is
		// processed, it is possible that the grant was already made. In that
		// case, the grant needs to be returned.
		granted bool
		// The cancelled value transitions at most once from false to true. Since
		// cancelled items are not immediately removed from SnapshotQueue.mu.q,
		// this bool tells the queue to ignore (and lazily remove) an item that
		// has been cancelled, when a grant happens.
		cancelled bool
	}
}

// SnapshotBurstSize represents the maximum number of bytes a snapshot ingest
// request can write before asking for admission.
//
// TODO(aaditya): Maybe make it a cluster setting.
const SnapshotBurstSize = 1 << 20 // 1MB

var snapshotWorkItemPool = sync.Pool{
	New: func() interface{} {
		return &snapshotWorkItem{}
	},
}

// DiskBandwidthForSnapshotIngest determines whether range snapshot ingests will
// be subject to disk write control tokens in Admission Control.
var DiskBandwidthForSnapshotIngest = settings.RegisterBoolSetting(
	settings.SystemOnly, "kvadmission.store.snapshot_ingest_bandwidth_control.enabled",
	"if set to true, snapshot ingests will be subject to disk write control in AC",
	metamorphic.ConstantWithTestBool("kvadmission.store.snapshot_ingest_bandwidth_control.enabled", true),
	settings.WithPublic,
)

var DiskBandwidthForSnapshotIngestMinRateEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.store.snapshot_ingest_bandwidth_control.min_rate.enabled",
	"if set to true, snapshot ingests will be admitted at a minimum rate when "+
		"kvadmission.store.provisioned_bandwidth is set to a non-zero value. Disabling this "+
		"setting can lead to snapshots being starved out by foreground traffic.",
	true,
	settings.WithPublic,
)

var snapshotWaitDur = metric.Metadata{
	Name:        "admission.wait_durations.snapshot_ingest",
	Help:        "Wait time for snapshot ingest requests that waited",
	Measurement: "Wait time Duration",
	Unit:        metric.Unit_NANOSECONDS,
}

type SnapshotMetrics struct {
	WaitDurations         metric.IHistogram
	AdmittedSnapshotBytes metric.Counter
}

func makeSnapshotQueueMetrics(registry *metric.Registry) *SnapshotMetrics {
	m := &SnapshotMetrics{
		WaitDurations: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     snapshotWaitDur,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
		AdmittedSnapshotBytes: *metric.NewCounter(metric.Metadata{
			Name:        "admission.admitted_snapshot_bytes",
			Help:        "Number of bytes admitted for snapshot ingests when provisioned bandwidth AC is enabled",
			Measurement: "Bytes",
			Unit:        metric.Unit_BYTES,
		}),
	}
	registry.AddMetricStruct(m)
	return m
}

// snapshotRequester is a wrapper used for test purposes.
type snapshotRequester interface {
	Admit(ctx context.Context, count int64, minRate int64, timerForMinRate timeutil.TimerI) error
}

// SnapshotQueue implements the requester interface. It is used to request
// admission for KV range snapshot requests. Internally, it uses queue.Queue to
// maintain FIFO-ordering.
type SnapshotQueue struct {
	snapshotGranter granter
	mu              struct {
		// Reminder: this mutex must not be held when calling into
		// snapshotGranter, as documented near the declaration of requester and
		// granter.
		syncutil.Mutex
		q *queue.Queue[*snapshotWorkItem]
	}
	metrics *SnapshotMetrics
	ts      timeutil.TimeSource
}

func makeSnapshotQueue(snapshotGranter granter, metrics *SnapshotMetrics) *SnapshotQueue {
	sq := &SnapshotQueue{
		snapshotGranter: snapshotGranter,
		metrics:         metrics,
	}
	// We ignore the error here since we are not applying any options.
	q, _ := queue.NewQueue[*snapshotWorkItem]()
	sq.mu.q = q
	sq.ts = timeutil.DefaultTimeSource{}
	return sq
}

var _ requester = &SnapshotQueue{}
var _ snapshotRequester = &SnapshotQueue{}

func (s *SnapshotQueue) hasWaitingRequests() (bool, burstQualification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.mu.q.Empty(), canBurst /*arbitrary*/
}

func (s *SnapshotQueue) granted(_ grantChainID) int64 {
	var item *snapshotWorkItem
	s.mu.Lock()
	defer s.mu.Unlock()
	// Loop until we find a valid item, or queue becomes empty.
	for {
		item = s.popLocked()
		if item == nil {
			return 0
		}
		if item.mu.cancelled {
			// Item was cancelled, we can ignore it.
			releaseSnapshotWorkItem(item)
			continue
		}
		break
	}
	count := item.count
	item.mu.granted = true
	// After signalling to the channel, we transfer ownership of item back to the
	// `Admit` goroutine, it should no longer be accessed here.
	item.admitCh <- true
	return count
}

func (s *SnapshotQueue) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.mu.q.Empty() {
		s.mu.q.Dequeue()
	}
}

// Admit is called whenever a snapshot ingest request needs to update the number
// of byte tokens it is using. Note that it accepts negative values, in which
// case it will return the tokens back to the granter.
func (s *SnapshotQueue) Admit(
	ctx context.Context, count int64, minRate int64, timerForMinRate timeutil.TimerI,
) (err error) {
	defer func() {
		if err == nil && count > 0 {
			s.metrics.AdmittedSnapshotBytes.Inc(count)
		}
	}()
	if count == 0 {
		return nil
	}
	if count < 0 {
		s.snapshotGranter.returnGrant(count)
		return nil
	}
	if s.snapshotGranter.tryGet(canBurst /*arbitrary*/, count) {
		return nil
	}
	// INVARIANT: count > 0.

	// We were unable to get tokens for admission, so we queue.
	//
	// Reminder: there is a race here where a call to hasWaitingRequests after
	// tryGet and before the mutex is acquired and the item is added to the
	// queue will not see the waiting request. Such a race would result in an
	// end state where the granter has resources and the requester has waiting
	// requests. This is harmless since hasWaitingRequests is called
	// periodically at a high enough frequency when tokens are limited.
	shouldRelease := true
	item := newSnapshotWorkItem(count)
	defer func() {
		if shouldRelease {
			releaseSnapshotWorkItem(item)
		}
	}()

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.addLocked(item)
	}()

	if minRate != 0 {
		maxWaitDuration := (time.Second * time.Duration(count)) / time.Duration(minRate)
		timerForMinRate.Reset(maxWaitDuration)
		defer timerForMinRate.Stop()
	}
	// Start waiting for admission.
	select {
	case <-ctx.Done():
		waitDur := timeutil.Since(item.enqueueingTime).Nanoseconds()
		// INVARIANT: tokensToReturn >= 0, since item.count >= 0.
		var tokensToReturn int64
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if item.mu.granted {
				// NB: we must call snapshotGranter.returnGrant after releasing the
				// mutex.
				tokensToReturn = item.count
			}
			// TODO(aaditya): Ideally, we also remove the item from the actual queue.
			// Right now, if we cancel the work, it remains in the queue. A call to
			// hasWaitingRequests() will return true even if all items in the queue are
			// cancelled but this is a very rare occurrence. A call to granted() will
			// work around this and skip any work item that is cancelled. This is
			// non-ideal behavior, but still provides accurate token accounting.
			item.mu.cancelled = true
		}()
		if tokensToReturn != 0 {
			s.snapshotGranter.returnGrant(tokensToReturn)
		}
		shouldRelease = false
		s.metrics.WaitDurations.RecordValue(waitDur)
		var deadlineSubstring string
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			deadlineSubstring = fmt.Sprintf("deadline: %v, ", deadline)
		}
		return errors.Wrapf(ctx.Err(),
			"context canceled while waiting in queue: %sstart: %v, dur: %v",
			deadlineSubstring, item.enqueueingTime, waitDur)
	case <-item.admitCh:
		waitDur := timeutil.Since(item.enqueueingTime).Nanoseconds()
		s.metrics.WaitDurations.RecordValue(waitDur)
		return nil
	case t := <-timerForMinRate.Ch():
		waitDur := t.Sub(item.enqueueingTime).Nanoseconds()
		// INVARIANT: tokensToSubtract >= 0, since item.count >= 0.
		tokensToSubtract := item.count
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if item.mu.granted {
				// NB: we must call snapshotGranter.tookWithoutPermission after
				// releasing the mutex.
				tokensToSubtract = 0
			}
			// NB: See the ctx.Done() case above for why we mark the item as
			// cancelled instead of removing the item from the queue.
			item.mu.cancelled = true
		}()
		if tokensToSubtract != 0 {
			s.snapshotGranter.tookWithoutPermission(tokensToSubtract)
		}
		shouldRelease = false
		s.metrics.WaitDurations.RecordValue(waitDur)
		return nil
	}
}

func (s *SnapshotQueue) addLocked(item *snapshotWorkItem) {
	item.enqueueingTime = timeutil.Now()
	s.mu.q.Enqueue(item)
}

func (s *SnapshotQueue) popLocked() *snapshotWorkItem {
	item, ok := s.mu.q.Dequeue()
	if !ok {
		return nil
	}
	return item
}

func (s *SnapshotQueue) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.q.Empty()
}

func releaseSnapshotWorkItem(sw *snapshotWorkItem) {
	ch := sw.admitCh
	select {
	case <-ch:
		panic("channel must be empty and not closed")
	default:
	}
	*sw = snapshotWorkItem{
		admitCh: ch,
	}
	snapshotWorkItemPool.Put(sw)
}

func newSnapshotWorkItem(count int64) *snapshotWorkItem {
	item := snapshotWorkItemPool.Get().(*snapshotWorkItem)
	ch := item.admitCh
	if ch == nil {
		ch = make(chan bool, 1)
	}
	*item = snapshotWorkItem{
		admitCh:        ch,
		enqueueingTime: timeutil.Now(),
		count:          count,
	}
	item.mu.cancelled = false
	item.mu.granted = false
	return item
}

type SnapshotPacer struct {
	snapshotQ       snapshotRequester
	intWriteBytes   int64
	minRate         int64
	timerForMinRate timeutil.TimerI
}

func NewSnapshotPacer(
	q snapshotRequester, minRate int64, timerForMinRate timeutil.TimerI,
) *SnapshotPacer {
	return &SnapshotPacer{
		snapshotQ:       q,
		intWriteBytes:   0,
		minRate:         minRate,
		timerForMinRate: timerForMinRate,
	}
}

func (p *SnapshotPacer) Pace(ctx context.Context, writeBytes int64, final bool) error {
	// Return early if nil pacer.
	if p == nil {
		return nil
	}
	p.intWriteBytes += writeBytes
	if p.intWriteBytes <= SnapshotBurstSize && !final {
		return nil
	}
	if err := p.snapshotQ.Admit(ctx, p.intWriteBytes, p.minRate, p.timerForMinRate); err != nil {
		return errors.Wrapf(err, "snapshot admission queue")
	}
	p.intWriteBytes = 0
	return nil
}
