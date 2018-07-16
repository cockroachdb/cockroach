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
	"github.com/cockroachdb/cockroach/pkg/storage/ct"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Config holds the information necessary to create a Provider.
type Config struct {
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	Storage  ct.Storage
	Clock    ct.LiveClockFn
	Close    ct.CloseFn
}

type subscriber struct {
	ch    chan<- ctpb.Entry
	queue []ctpb.Entry
}

// Provider implements ct.Provider. It orchestrates the flow of closed
// timestamps and lets callers check whether they can serve reads.
type Provider struct {
	cfg *Config

	mu struct {
		syncutil.RWMutex
		subscribers []*subscriber
	}
	cond *sync.Cond

	everyClockLog log.EveryN
}

var _ ct.Provider = (*Provider)(nil)

// NewProvider initializes a Provider, that has yet to be started.
func NewProvider(cfg *Config) *Provider {
	return &Provider{
		cfg:           cfg,
		cond:          sync.NewCond((&syncutil.RWMutex{}).RLocker()),
		everyClockLog: log.Every(time.Minute),
	}
}

// Start implements ct.Provider.
//
// TODO(tschottdorf): the closer functionality could be extracted into its own
// component, which would make the interfaces a little cleaner. Decide whether
// it's worth it during testing.
func (p *Provider) Start() {
	p.cfg.Stopper.RunWorker(log.WithLogTagStr(context.Background(), "ct-closer", ""), p.runCloser)
}

func (p *Provider) runCloser(ctx context.Context) {
	// The local loop is driven as a notification for NodeID zero. This side-
	// steps the need to plumb a getter for the NodeID (which is annoyingly
	// determined fairly late for new servers).
	ch := p.Notify(0)
	defer close(ch)

	t := timeutil.NewTimer()
	for {
		closeFraction := ct.CloseFraction.Get(&p.cfg.Settings.SV)
		targetDuration := float64(ct.TargetDuration.Get(&p.cfg.Settings.SV))
		t.Reset(time.Duration(closeFraction * targetDuration))

		select {
		case <-p.cfg.Stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		case <-t.C:
			t.Read = true
		}

		next, epoch, err := p.cfg.Clock()
		next.WallTime -= int64(targetDuration)
		if err != nil {
			if p.everyClockLog.ShouldLog() {
				log.Warningf(ctx, "unable to move closed timestamp forward: %s", err)
			}
		} else {
			closed, m := p.cfg.Close(next)

			ch <- ctpb.Entry{
				Epoch:           epoch,
				ClosedTimestamp: closed,
				MLAI:            m,
			}
		}

		// Broadcast even if nothing new was queued, so that the subscribers
		// loop to check their client's context.
		p.cond.Broadcast()
	}
}

func (p *Provider) processNotification(nodeID roachpb.NodeID, entry ctpb.Entry) {
	p.cfg.Storage.Add(nodeID, entry)
	if nodeID == 0 {
		p.mu.Lock()
		for _, sub := range p.mu.subscribers {
			sub.queue = append(sub.queue, entry)
		}
		p.mu.Unlock()
	}
}

// Notify implements ct.Notifyee.
func (p *Provider) Notify(nodeID roachpb.NodeID) chan<- ctpb.Entry {
	ch := make(chan ctpb.Entry)
	ctx := context.TODO()

	p.cfg.Stopper.RunWorker(ctx, func(ctx context.Context) {
		for entry := range ch {
			p.processNotification(nodeID, entry)
		}
	})

	return ch
}

// Subscribe implements ct.Producer.
func (p *Provider) Subscribe(ctx context.Context, ch chan<- ctpb.Entry) {
	var i int
	sub := &subscriber{ch, nil}
	p.mu.Lock()
	for i = range p.mu.subscribers {
		if p.mu.subscribers == nil {
			p.mu.subscribers[i] = sub
		}
	}
	if i == len(p.mu.subscribers) {
		p.mu.subscribers = append(p.mu.subscribers, sub)
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.mu.subscribers[i] = nil
		p.mu.Unlock()
		close(ch)
	}()

	// The subscription is already active, so any storage snapshot from now on is
	// going to fully catch up the subscriber without a gap.
	{
		p.cfg.Storage.VisitAscending(0 /* TODO: NodeID */, func(e ctpb.Entry) (done bool) {
			select {
			case ch <- e:
			case <-ctx.Done():
				return true // done
			}
			return false // want more
		})
	}

	for {
		p.cond.L.Lock()
		p.cond.Wait()
		p.cond.L.Unlock()

		if err := ctx.Err(); err != nil {
			if log.V(1) {
				log.Info(ctx, err)
			}
			return
		}

		var queue []ctpb.Entry
		p.mu.RLock()
		queue, p.mu.subscribers[i].queue = p.mu.subscribers[i].queue, nil
		p.mu.RUnlock()

		for _, entry := range queue {
			select {
			case ch <- entry:
			default:
				// Abort the subscription if consumer doesn't keep up.
				log.Warning(ctx, "closed timestamp update subscriber did not catch up; terminating")
				return
			}
		}
	}
}

// CanServe implements ct.Provider.
func (p *Provider) CanServe(
	nodeID roachpb.NodeID, ts hlc.Timestamp, rangeID roachpb.RangeID, epoch ct.EpochT, lai ct.LAIT,
) bool {
	var ok bool
	// TODO(tschottdorf): add and use VisitDescending.
	p.cfg.Storage.VisitAscending(nodeID, func(entry ctpb.Entry) bool {
		mlai, found := entry.MLAI[rangeID]
		ok = found &&
			mlai <= lai.V() &&
			entry.Epoch == epoch.V() &&
			!entry.ClosedTimestamp.Less(ts)
		return ok // done when ok == true
	})
	return ok
}
