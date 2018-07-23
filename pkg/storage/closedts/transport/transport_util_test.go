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
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/transport"
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
	Dialer    *Dialer
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

type client struct {
	ctx     context.Context
	stopper *stop.Stopper
	send    chan *ctpb.Reaction
	recv    chan *ctpb.Entry

	observe func(interface{})
}

func (c *client) Send(msg *ctpb.Reaction) error {
	select {
	case <-c.stopper.ShouldQuiesce():
		return io.EOF
	case c.send <- msg:
		c.observe(msg)
		return nil
	}
}

func (c *client) Recv() (*ctpb.Entry, error) {
	select {
	case <-c.stopper.ShouldQuiesce():
		return nil, io.EOF
	case msg := <-c.recv:
		c.observe(msg)
		return msg, nil
	}
}

func (c *client) CloseSend() error {
	close(c.send)
	return nil
}

func (c *client) Context() context.Context {
	return c.ctx
}

type incomingClient client

func (c *incomingClient) Send(msg *ctpb.Entry) error {
	select {
	case <-c.stopper.ShouldQuiesce():
		return io.EOF
	case c.recv <- msg:
		return nil
	}
}

func (c *incomingClient) Recv() (*ctpb.Reaction, error) {
	select {
	case <-c.stopper.ShouldQuiesce():
		return nil, io.EOF
	case msg, ok := <-c.send:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

func (c *incomingClient) Context() context.Context {
	return c.ctx
}

type Dialer struct {
	stopper *stop.Stopper
	server  *transport.Server

	mu struct {
		syncutil.Mutex
		transcripts map[roachpb.NodeID][]interface{}
	}
}

func (d *Dialer) transcript(nodeID roachpb.NodeID) []interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]interface{}(nil), d.mu.transcripts[nodeID]...)
}

func (d *Dialer) Dial(ctx context.Context, nodeID roachpb.NodeID) (ctpb.Client, error) {
	c := &client{
		ctx:     ctx,
		send:    make(chan *ctpb.Reaction),
		recv:    make(chan *ctpb.Entry),
		stopper: d.stopper,
		observe: func(msg interface{}) {
			d.mu.Lock()
			if d.mu.transcripts == nil {
				d.mu.transcripts = map[roachpb.NodeID][]interface{}{}
			}
			d.mu.transcripts[nodeID] = append(d.mu.transcripts[nodeID], msg)
			d.mu.Unlock()
		},
	}

	d.stopper.RunWorker(ctx, func(ctx context.Context) {
		_ = d.server.Handle((*incomingClient)(c))
	})
	return c, nil

}
func (d *Dialer) Ready(nodeID roachpb.NodeID) bool {
	return true
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
