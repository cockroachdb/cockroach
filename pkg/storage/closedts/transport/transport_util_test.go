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

package transport_test

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/transport"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/transport/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type TestContainer struct {
	Settings  *cluster.Settings
	Stopper   *stop.Stopper
	Producer  *TestProducer
	Notifyee  *TestNotifyee
	Refreshed *RefreshTracker
	Server    *transport.Server
	Dialer    *testutils.ChanDialer
	Clients   *transport.Clients
}

type TestProducer struct {
	syncutil.Mutex
	chs []chan<- ctpb.Entry
}

func (tp *TestProducer) Subscribe(ctx context.Context, ch chan<- ctpb.Entry) {
	tp.Lock()
	tp.chs = append(tp.chs, ch)
	tp.Unlock()
}

func (tp *TestProducer) numSubscriptions() int {
	tp.Lock()
	defer tp.Unlock()
	return len(tp.chs)
}

func (tp *TestProducer) sendAll(entry ctpb.Entry) {
	tp.Lock()
	for _, ch := range tp.chs {
		ch <- entry
	}
	tp.Unlock()
}

type TestNotifyee struct {
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		entries map[roachpb.NodeID][]ctpb.Entry
	}
}

func newTestNotifyee(stopper *stop.Stopper) *TestNotifyee {
	tn := &TestNotifyee{
		stopper: stopper,
	}
	tn.mu.entries = make(map[roachpb.NodeID][]ctpb.Entry)
	return tn
}

func (tn *TestNotifyee) Notify(nodeID roachpb.NodeID) chan<- ctpb.Entry {
	ch := make(chan ctpb.Entry)
	tn.stopper.RunWorker(context.Background(), func(ctx context.Context) {
		for entry := range ch {
			tn.mu.Lock()
			tn.mu.entries[nodeID] = append(tn.mu.entries[nodeID], entry)
			tn.mu.Unlock()
		}
	})
	return ch
}

type RefreshTracker struct {
	syncutil.Mutex
	rangeIDs []roachpb.RangeID
}

func (r *RefreshTracker) Get() []roachpb.RangeID {
	r.Lock()
	defer r.Unlock()
	return append([]roachpb.RangeID(nil), r.rangeIDs...)
}

func (r *RefreshTracker) Add(rangeIDs ...roachpb.RangeID) {
	r.Lock()
	r.rangeIDs = append(r.rangeIDs, rangeIDs...)
	r.Unlock()
}
