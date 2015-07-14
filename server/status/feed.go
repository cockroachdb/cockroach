// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package status

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

// StartNodeEvent is published when a node is started.
type StartNodeEvent struct {
	Desc      proto.NodeDescriptor
	StartedAt int64
}

// CallSuccessEvent is published when a call to a node completes without error.
type CallSuccessEvent struct {
	NodeID proto.NodeID
	Method proto.Method
}

// CallErrorEvent is published when a call to a node returns an error.
type CallErrorEvent struct {
	NodeID proto.NodeID
	Method proto.Method
}

// TestSyncEvent is intended for testing only; it can be used by tests to
// synchronize with feed consumers.
type TestSyncEvent struct {
	count  int32
	closer chan struct{}
}

// NewTestSyncEvent creates a new TestSyncEvent which expects to be consumed by
// the given number of consumers.
func NewTestSyncEvent(expectedConsumers int32) *TestSyncEvent {
	return &TestSyncEvent{
		count:  expectedConsumers,
		closer: make(chan struct{}),
	}
}

// Sync will wait for the TestSyncEvent to be consumed by the expected number of
// consumers, returning an error if the event is not consumed within the
// specified timeout. If the TestSyncEvent has already been sufficiently
// consumed, Sync will return immediately.
func (tse *TestSyncEvent) Sync(timeout time.Duration) error {
	select {
	case <-tse.closer:
		return nil
	case <-time.After(timeout):
		return util.Errorf("failed to synchronize with feed after specified timeout (%s)", timeout)
	}
}

// consume is called by NodeEventListener when it receives a TestSyncEvent.
func (tse *TestSyncEvent) consume() {
	val := atomic.AddInt32(&tse.count, -1)
	if val == 0 {
		close(tse.closer)
	}
}

// NodeEventFeed is a helper structure which publishes node-specific events to a
// util.Feed. If the target feed is nil, event methods become no-ops.
type NodeEventFeed struct {
	id proto.NodeID
	f  *util.Feed
}

// NewNodeEventFeed creates a new NodeEventFeed which publishes events for a
// specific node to the supplied feed.
func NewNodeEventFeed(id proto.NodeID, feed *util.Feed) NodeEventFeed {
	return NodeEventFeed{
		id: id,
		f:  feed,
	}
}

// StartNode is called by a node when it has started.
func (nef NodeEventFeed) StartNode(desc proto.NodeDescriptor, startedAt int64) {
	if nef.f == nil {
		return
	}
	nef.f.Publish(&StartNodeEvent{
		Desc:      desc,
		StartedAt: startedAt,
	})
}

// CallComplete is called by a node whenever it completes a request. This will
// publish an appropriate event to the feed based on the results of the call.
func (nef NodeEventFeed) CallComplete(args proto.Request, reply proto.Response) {
	if nef.f == nil {
		return
	}
	if err := reply.Header().Error; err != nil &&
		err.CanRestartTransaction() == proto.TransactionRestart_ABORT {
		nef.f.Publish(&CallErrorEvent{
			NodeID: nef.id,
			Method: args.Method(),
		})
	} else {
		nef.f.Publish(&CallSuccessEvent{
			NodeID: nef.id,
			Method: args.Method(),
		})
	}
}

// NodeEventListener is an interface that can be implemented by objects which
// listen for events published by nodes.
type NodeEventListener interface {
	OnStartNode(event *StartNodeEvent)
	OnCallSuccess(event *CallSuccessEvent)
	OnCallError(event *CallErrorEvent)
}

// ProcessNodeEvents reads node events from the supplied channel and passes them
// to the correct methods of the supplied NodeEventListener. This method will
// run until the Subscription's events channel is closed.
func ProcessNodeEvents(l NodeEventListener, sub *util.Subscription) {
	for event := range sub.Events() {
		// TODO(tamird): https://github.com/barakmich/go-nyet/issues/7
		switch specificEvent := event.(type) {
		case *StartNodeEvent:
			l.OnStartNode(specificEvent)
		case *CallSuccessEvent:
			l.OnCallSuccess(specificEvent)
		case *CallErrorEvent:
			l.OnCallError(specificEvent)
		case *TestSyncEvent:
			specificEvent.consume()
		}
	}
}
