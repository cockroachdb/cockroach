// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package provider

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// Config holds the information necessary to create a Provider.
type Config struct {
	// NodeID is the ID of the node on which the Provider is housed.
	NodeID   roachpb.NodeID
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Storage  closedts.Storage
	Clock    closedts.LiveClockFn
	Close    closedts.CloseFn
}

type subscriber struct {
	ch    chan<- ctpb.Entry
	queue []ctpb.Entry
}

// Provider implements closedts.Provider. It orchestrates the flow of closed
// timestamps and lets callers check whether they can serve reads.
type Provider struct {
	cfg *Config

	mu struct {
		syncutil.RWMutex
		*sync.Cond // on RWMutex.RLocker()
		// The current subscribers. The goroutine associated to each
		// subscriber uses the RLock to mutate its slot. Thus, when
		// accessing this slice for any other reason, the write lock
		// needs to be acquired.
		subscribers []*subscriber
		draining    bool // tell subscribers to terminate
	}

	everyClockLog log.EveryN
}

var _ closedts.Provider = (*Provider)(nil)

// NewProvider initializes a Provider, that has yet to be started.
func NewProvider(cfg *Config) *Provider {
	p := &Provider{
		cfg:           cfg,
		everyClockLog: log.Every(time.Minute),
	}
	p.mu.Cond = sync.NewCond(p.mu.RLocker())
	return p
}

// Start implements closedts.Provider.
func (p *Provider) Start() {
	if err := p.cfg.Stopper.RunAsyncTask(
		logtags.AddTag(context.Background(), "ct-closer", nil), "ct-closer", p.runCloser,
	); err != nil {
		p.drain()
	}
}

func (p *Provider) drain() {
	p.mu.Lock()
	p.mu.draining = true
	p.mu.Unlock()
	for {
		p.mu.Broadcast()
		p.mu.Lock()
		done := true
		for _, sub := range p.mu.subscribers {
			done = done && sub == nil
		}
		p.mu.Unlock()

		if done {
			return
		}
	}
}

func (p *Provider) runCloser(ctx context.Context) {
	// The loop below signals the subscribers, so when it exits it needs to do
	// extra work to help the subscribers terminate.
	defer p.drain()

	if p.cfg.NodeID == 0 {
		// This Provider is likely misconfigured.
		panic("can't use NodeID zero")
	}
	ch := p.Notify(p.cfg.NodeID)
	defer close(ch)

	confCh := make(chan struct{}, 1)
	confChanged := func(ctx context.Context) {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.TargetDuration.SetOnChange(&p.cfg.Settings.SV, confChanged)
	// Track whether we've ever been live to avoid logging warnings about not
	// being live during node startup.
	var everBeenLive bool
	t := timeutil.NewTimer()
	defer t.Stop()
	for {
		// If the "new" closed timestamps mechanism is enabled, we inhibit this old one.
		if p.cfg.Settings.Version.IsActive(ctx, clusterversion.ClosedTimestampsRaftTransport) {
			log.Infof(ctx, "disabling legacy closed-timestamp mechanism; the new one is enabled")
			break
		}

		closeFraction := closedts.CloseFraction.Get(&p.cfg.Settings.SV)
		targetDuration := float64(closedts.TargetDuration.Get(&p.cfg.Settings.SV))
		if targetDuration > 0 {
			t.Reset(time.Duration(closeFraction * targetDuration))
		} else {
			// Disable closing when the target duration is non-positive.
			t.Stop()
			t = timeutil.NewTimer()
		}
		select {
		case <-p.cfg.Stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
		case <-confCh:
			// Loop around to use the updated timer.
			continue
		}

		next, liveAtEpoch, err := p.cfg.Clock(p.cfg.NodeID)
		next = next.Add(-int64(targetDuration), 0)
		if err != nil {
			if everBeenLive && p.everyClockLog.ShouldLog() {
				log.Warningf(ctx, "unable to move closed timestamp forward: %+v", err)
			}
			// Broadcast even if nothing new was queued, so that the subscribers
			// loop to check their client's context.
			p.mu.Broadcast()
		} else {
			everBeenLive = true
			// Close may fail if the data being closed does not correspond to the
			// current liveAtEpoch.
			closed, m, ok := p.cfg.Close(next, liveAtEpoch)
			if !ok {
				if log.V(1) {
					log.Infof(ctx, "failed to close %v due to liveness epoch mismatch at %v",
						next, liveAtEpoch)
				}
				continue
			}
			if log.V(1) {
				log.Infof(ctx, "closed ts=%s with %+v, next closed timestamp should be %s",
					closed, m, next)
			}
			entry := ctpb.Entry{
				Epoch:           liveAtEpoch,
				ClosedTimestamp: closed,
				MLAI:            m,
			}

			// Simulate a subscription to the local node, so that the new information
			// is added to the storage (and thus becomes available to future subscribers
			// as well, not only to existing ones). The other end of the chan will Broadcast().
			//
			// TODO(tschottdorf): the transport should ignore connection requests from
			// the node to itself. Those connections would pointlessly loop this around
			// once more.
			select {
			case ch <- entry:
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			}
		}
	}
}

// Notify implements closedts.Notifyee. It passes the incoming stream of Entries
// to the local Storage.
func (p *Provider) Notify(nodeID roachpb.NodeID) chan<- ctpb.Entry {
	ch := make(chan ctpb.Entry)

	_ = p.cfg.Stopper.RunAsyncTask(context.Background(), "provider-notify", func(ctx context.Context) {
		handle := func(entry ctpb.Entry) {
			p.cfg.Storage.Add(nodeID, entry)
		}
		// Special-case data about the origin node, which folks can subscribe to.
		// This is easily generalized to also allow subscriptions for data that
		// originated on other nodes, but this doesn't seem necessary right now.
		if nodeID == p.cfg.NodeID {
			handle = func(entry ctpb.Entry) {
				// Add to the Storage first.
				p.cfg.Storage.Add(nodeID, entry)
				// Notify existing subscribers.
				p.mu.Lock()
				for _, sub := range p.mu.subscribers {
					if sub == nil {
						continue
					}
					sub.queue = append(sub.queue, entry)
				}
				p.mu.Unlock()
				// Wake up all clients.
				p.mu.Broadcast()
			}
		}
		for {
			select {
			case entry, ok := <-ch:
				if !ok {
					return
				}
				handle(entry)
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			}
		}
	})

	return ch
}

// Subscribe implements closedts.Producer. It produces a stream of Entries
// pertaining to the local Node.
//
// TODO(tschottdorf): consider not forcing the caller to launch the goroutine.
func (p *Provider) Subscribe(ctx context.Context, ch chan<- ctpb.Entry) {
	var i int
	sub := &subscriber{ch, nil}
	p.mu.Lock()
	for i = 0; i < len(p.mu.subscribers); i++ {
		if p.mu.subscribers[i] == nil {
			p.mu.subscribers[i] = sub
			break
		}
	}
	if i == len(p.mu.subscribers) {
		p.mu.subscribers = append(p.mu.subscribers, sub)
	}
	draining := p.mu.draining
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.mu.subscribers[i] = nil
		p.mu.Unlock()
		close(ch)
	}()

	if draining {
		return
	}

	if log.V(1) {
		log.Infof(ctx, "new subscriber (slot %d) connected", i)
	}

	// The subscription is already active, so any storage snapshot from now on is
	// going to fully catch up the subscriber without a gap.
	{
		var entries []ctpb.Entry

		p.cfg.Storage.VisitAscending(p.cfg.NodeID, func(entry ctpb.Entry) (done bool) {
			// Don't block in this method.
			entries = append(entries, entry)
			return false // not done
		})

		for _, entry := range entries {
			select {
			case ch <- entry:
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	}

	for {
		p.mu.RLock()
		var done bool
		for len(p.mu.subscribers[i].queue) == 0 {
			if ctx.Err() != nil || p.mu.draining {
				done = true
				break
			}
			p.mu.Wait()
		}
		var queue []ctpb.Entry
		// When only readers are around (as they are now), we can actually
		// mutate our slot because that's all the others do as well.
		queue, p.mu.subscribers[i].queue = p.mu.subscribers[i].queue, nil
		p.mu.RUnlock()

		if done {
			return
		}

		shouldLog := log.V(1)
		var n int
		minMLAI := ctpb.LAI(math.MaxInt64)
		var minRangeID, maxRangeID roachpb.RangeID
		var maxMLAI ctpb.LAI

		for _, entry := range queue {
			if shouldLog {
				n += len(entry.MLAI)
				for rangeID, mlai := range entry.MLAI {
					if mlai < minMLAI {
						minMLAI = mlai
						minRangeID = rangeID
					}
					if mlai > maxMLAI {
						maxMLAI = mlai
						maxRangeID = rangeID
					}
				}
			}

			select {
			case ch <- entry:
			case <-p.cfg.Stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
		if shouldLog {
			log.Infof(ctx, "sent %d closed timestamp entries to client %d (%d range updates total, min/max mlai: %d@r%d / %d@r%d)", len(queue), i, n, minMLAI, minRangeID, maxMLAI, maxRangeID)
		}
	}
}

// MaxClosed implements closedts.Provider.
func (p *Provider) MaxClosed(
	nodeID roachpb.NodeID, rangeID roachpb.RangeID, epoch ctpb.Epoch, lai ctpb.LAI,
) hlc.Timestamp {
	var maxTS hlc.Timestamp
	p.cfg.Storage.VisitDescending(nodeID, func(entry ctpb.Entry) (done bool) {
		if mlai, found := entry.MLAI[rangeID]; found {
			if entry.Epoch == epoch && mlai <= lai {
				maxTS = entry.ClosedTimestamp
				return true
			}
		}
		return false
	})

	return maxTS
}
