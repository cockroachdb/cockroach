// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package provider

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
//
// TODO(tschottdorf): the closer functionality could be extracted into its own
// component, which would make the interfaces a little cleaner. Decide whether
// it's worth it during testing.
func (p *Provider) Start() {
	p.cfg.Stopper.RunWorker(log.WithLogTagStr(context.Background(), "ct-closer", ""), p.runCloser)
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

	var t timeutil.Timer
	defer t.Stop()
	var lastEpoch ctpb.Epoch
	for {
		closeFraction := closedts.CloseFraction.Get(&p.cfg.Settings.SV)
		targetDuration := float64(closedts.TargetDuration.Get(&p.cfg.Settings.SV))
		t.Reset(time.Duration(closeFraction * targetDuration))

		select {
		case <-p.cfg.Stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
		}

		next, epoch, err := p.cfg.Clock(p.cfg.NodeID)

		next.WallTime -= int64(targetDuration)
		if err != nil {
			if p.everyClockLog.ShouldLog() {
				log.Warningf(ctx, "unable to move closed timestamp forward: %s", err)
			}
			// Broadcast even if nothing new was queued, so that the subscribers
			// loop to check their client's context.
			p.mu.Broadcast()
		} else {
			closed, m := p.cfg.Close(next)
			if log.V(1) {
				log.Infof(ctx, "closed ts=%s with %+v, next closed timestamp should be %s", closed, m, next)
			}
			entry := ctpb.Entry{
				Epoch:           lastEpoch,
				ClosedTimestamp: closed,
				MLAI:            m,
			}
			// TODO(tschottdorf): this one-off between the epoch is awkward. Clock() gives us the epoch for `next`
			// but the entry wants the epoch for the current closed timestamp. Better to pass both into Close and
			// to get both back from it as well.
			lastEpoch = epoch

			// Simulate a subscription to the local node, so that the new information
			// is added to the storage (and thus becomes available to future subscribers
			// as well, not only to existing ones). The other end of the chan will Broadcast().
			//
			// TODO(tschottdorf): the transport should ignore connection requests from
			// the node to itself. Those connections would pointlessly loop this around
			// once more.
			ch <- entry
		}
	}
}

// Notify implements closedts.Notifyee. It passes the incoming stream of Entries
// to the local Storage.
func (p *Provider) Notify(nodeID roachpb.NodeID) chan<- ctpb.Entry {
	ch := make(chan ctpb.Entry)

	p.cfg.Stopper.RunWorker(context.Background(), func(ctx context.Context) {
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
		for entry := range ch {
			handle(entry)
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
		log.Info(ctx, "new subscriber connected")
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

		for _, entry := range queue {
			select {
			case ch <- entry:
			case <-ctx.Done():
				return
			}
		}
	}
}

// CanServe implements closedts.Provider.
func (p *Provider) CanServe(
	nodeID roachpb.NodeID, ts hlc.Timestamp, rangeID roachpb.RangeID, epoch ctpb.Epoch, lai ctpb.LAI,
) bool {
	var ok bool
	p.cfg.Storage.VisitDescending(nodeID, func(entry ctpb.Entry) bool {
		mlai, found := entry.MLAI[rangeID]
		ctOK := !entry.ClosedTimestamp.Less(ts)

		ok = found &&
			ctOK &&
			mlai <= lai &&
			entry.Epoch == epoch

		// We're done either if we proved that the read is possible, or if we're
		// already done looking at closed timestamps large enough to satisfy it.
		done := ok || !ctOK
		return done
	})

	return ok
}
