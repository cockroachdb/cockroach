// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ChanDialer is an implementation of closedts.Dialer that connects clients
// directly via a channel to a Server.
type ChanDialer struct {
	stopper *stop.Stopper
	server  ctpb.Server

	mu struct {
		syncutil.Mutex
		transcripts map[roachpb.NodeID][]interface{}
	}
}

// NewChanDialer sets up a ChanDialer.
func NewChanDialer(stopper *stop.Stopper, server ctpb.Server) *ChanDialer {
	d := &ChanDialer{
		stopper: stopper,
		server:  server,
	}
	d.mu.transcripts = make(map[roachpb.NodeID][]interface{})
	return d
}

// Transcript returns a slice of messages sent over the "wire".
func (d *ChanDialer) Transcript(nodeID roachpb.NodeID) []interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]interface{}(nil), d.mu.transcripts[nodeID]...)
}

// Dial implements closedts.Dialer.
func (d *ChanDialer) Dial(ctx context.Context, nodeID roachpb.NodeID) (ctpb.Client, error) {
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

	if err := d.stopper.RunAsyncTask(ctx, "closedts-dial", func(ctx context.Context) {
		_ = d.server.Get((*incomingClient)(c))
	}); err != nil {
		return nil, err
	}
	return c, nil

}

// Ready implements closedts.Dialer by always returning true.
func (d *ChanDialer) Ready(nodeID roachpb.NodeID) bool {
	return true
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
