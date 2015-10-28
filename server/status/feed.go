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
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// StartNodeEvent is published when a node is started.
type StartNodeEvent struct {
	Desc      roachpb.NodeDescriptor
	StartedAt int64
}

// String implements fmt.Stringer.
func (e StartNodeEvent) String() string {
	return fmt.Sprintf("%d.start", e.Desc.NodeID)
}

// CallSuccessEvent is published when a call to a node completes without error.
type CallSuccessEvent struct {
	NodeID roachpb.NodeID
	Method roachpb.Method
}

// String implements fmt.Stringer.
func (e CallSuccessEvent) String() string {
	return fmt.Sprintf("%d.ok.%s", e.NodeID, e.Method)
}

// CallErrorEvent is published when a call to a node returns an error.
type CallErrorEvent struct {
	NodeID roachpb.NodeID
	Method roachpb.Method
}

// String implements fmt.Stringer.
func (e CallErrorEvent) String() string {
	return fmt.Sprintf("%d.err.%s", e.NodeID, e.Method)
}

// NodeEventFeed is a helper structure which publishes node-specific events to a
// util.Feed. If the target feed is nil, event methods become no-ops.
type NodeEventFeed struct {
	id roachpb.NodeID
	f  *util.Feed
}

// NewNodeEventFeed creates a new NodeEventFeed which publishes events for a
// specific node to the supplied feed.
func NewNodeEventFeed(id roachpb.NodeID, feed *util.Feed) NodeEventFeed {
	return NodeEventFeed{
		id: id,
		f:  feed,
	}
}

// StartNode is called by a node when it has started.
func (nef NodeEventFeed) StartNode(desc roachpb.NodeDescriptor, startedAt int64) {
	nef.f.Publish(&StartNodeEvent{
		Desc:      desc,
		StartedAt: startedAt,
	})
}

// CallComplete is called by a node whenever it completes a request. This will
// publish appropriate events to the feed:
// - For a successful request, a corresponding event for each request in the batch,
// - on error without index information, a failure of the Batch, and
// - on an indexed error a failure of the individual request.
func (nef NodeEventFeed) CallComplete(ba roachpb.BatchRequest, pErr *roachpb.Error) {
	if pErr != nil && pErr.TransactionRestart == roachpb.TransactionRestart_ABORT {
		method := roachpb.Batch
		if iErr, ok := pErr.GoError().(roachpb.IndexedError); ok {
			if index, ok := iErr.ErrorIndex(); ok {
				method = ba.Requests[index].GetInner().Method()
			}
		}
		nef.f.Publish(&CallErrorEvent{
			NodeID: nef.id,
			Method: method,
		})
		return
	}
	for _, union := range ba.Requests {
		nef.f.Publish(&CallSuccessEvent{
			NodeID: nef.id,
			Method: union.GetInner().Method(),
		})
	}
}

// NodeEventListener is an interface that can be implemented by objects which
// listen for events published by nodes.
type NodeEventListener interface {
	OnStartNode(event *StartNodeEvent)
	OnCallSuccess(event *CallSuccessEvent)
	OnCallError(event *CallErrorEvent)
	// TODO(tschottdorf): break this out into a TraceEventListener.
	OnTrace(event *tracer.Trace)
}

// ProcessNodeEvent dispatches an event on the NodeEventListener.
func ProcessNodeEvent(l NodeEventListener, event interface{}) {
	switch specificEvent := event.(type) {
	case *StartNodeEvent:
		l.OnStartNode(specificEvent)
	case *tracer.Trace:
		l.OnTrace(specificEvent)
	case *CallSuccessEvent:
		l.OnCallSuccess(specificEvent)
	case *CallErrorEvent:
		l.OnCallError(specificEvent)
	}
}
