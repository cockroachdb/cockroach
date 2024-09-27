// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type snapshotWorkItem struct {
	admitCh        chan bool
	enqueueingTime time.Time
	count          int64
	inQueue        bool
	cancelled      bool
}

// SnapshotBurstSize represents the maximum number of bytes a snapshot ingest
// request can write before asking for admission.
//
// TODO(aaditya): Move the constant elsewhere, and maybe make it a cluster
// setting.
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
	"if set to true, snapshot ingests will be subject to disk write control in AC.",
	// TODO(aaditya): Enable by default once enough experimentation is done.
	false,
	settings.WithPublic)

var snapshotWaitDur = metric.Metadata{
	Name:        "admission.wait_durations.snapshot_ingest",
	Help:        "Wait time for snapshot ingest requests that waited",
	Measurement: "Wait time Duration",
	Unit:        metric.Unit_NANOSECONDS,
}

type SnapshotMetrics struct {
	WaitDurations metric.IHistogram
}

func makeSnapshotQueueMetrics(registry *metric.Registry) *SnapshotMetrics {
	m := &SnapshotMetrics{
		WaitDurations: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     snapshotWaitDur,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
	}
	registry.AddMetricStruct(m)
	return m
}

// SnapshotQueue implements the requester interface. It is used to request
// admission for KV range snapshot requests. Internally, it uses queue.Queue to
// maintain FIFO-ordering.
type SnapshotQueue struct {
	snapshotGranter granter
	mu              struct {
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

func (s *SnapshotQueue) hasWaitingRequests() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.mu.q.Empty()
}

func (s *SnapshotQueue) granted(_ grantChainID) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.popLocked()
	if item == nil {
		return 0
	}
	if item.cancelled {
		// Item was cancelled, we can ignore it.
		releaseSnapshotWorkItem(item)
		return 0
	}
	item.admitCh <- true
	return item.count
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
func (s *SnapshotQueue) Admit(ctx context.Context, count int64) error {
	if count == 0 {
		return nil
	}
	if count < 0 {
		s.snapshotGranter.returnGrant(count)
		return nil
	}
	if s.snapshotGranter.tryGet(count) {
		return nil
	}
	// We were unable to get tokens for admission, so we queue.
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

	// Start waiting for admission.
	select {
	case <-ctx.Done():
		waitDur := timeutil.Since(item.enqueueingTime).Nanoseconds()
		if !item.inQueue {
			s.snapshotGranter.returnGrant(item.count)
		}
		// TODO(aaditya): Ideally, we also remove the item from the actual queue.
		// Right now, if we cancel the work, a call to granted() may still look at
		// this item. It is fine because the token count will be 0, and it will be
		// removed from the queue. This is non-ideal behavior but still provides
		// accurate token accounting.
		shouldRelease = false
		item.cancelled = true
		deadline, _ := ctx.Deadline()
		s.metrics.WaitDurations.RecordValue(waitDur)
		return errors.Wrapf(ctx.Err(),
			"context canceled while waiting in queue: deadline: %v, start: %v, dur: %v",
			deadline, item.enqueueingTime, waitDur)
	case <-item.admitCh:
		waitDur := timeutil.Since(item.enqueueingTime).Nanoseconds()
		s.metrics.WaitDurations.RecordValue(waitDur)
		return nil
	}
}

func (s *SnapshotQueue) addLocked(item *snapshotWorkItem) {
	item.enqueueingTime = timeutil.Now()
	s.mu.q.Enqueue(item)
	item.inQueue = true
}

func (s *SnapshotQueue) popLocked() *snapshotWorkItem {
	item, ok := s.mu.q.Dequeue()
	if !ok {
		return nil
	}
	item.inQueue = false
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
	waitingWorkPool.Put(sw)
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
		cancelled:      false,
		inQueue:        false,
	}
	return item
}
