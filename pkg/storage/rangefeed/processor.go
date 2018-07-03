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

package rangefeed

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
// 1. it translates logical updates into rangefeed events.
// 2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//    timestamp.
type Processor struct {
	clock *hlc.Clock
	eng   engine.Engine
	span  roachpb.RSpan
	reg   registry
	rts   resolvedTimestamp

	regC   chan registration
	lenC   chan int
	eventC chan event
	stopC  chan error
}

// event is a union of different event types that the Processor goroutine needs
// to be informed of. It is used so that all events can be sent over the same
// channel, which is necessary to prevent reordering.
type event struct {
	ops []enginepb.MVCCLogicalOp
	ct  hlc.Timestamp
}

// NewProcessor creates a new rangefeed Processor. The corresponding goroutine
// should be launched using the Start method.
func NewProcessor(clock *hlc.Clock, eng engine.Engine, span roachpb.RSpan) *Processor {
	return &Processor{
		clock: clock,
		eng:   eng,
		span:  span,
		reg:   makeRegistry(),
		rts:   makeResolvedTimestamp(),

		regC:   make(chan registration),
		lenC:   make(chan int),
		eventC: make(chan event, 64),
		stopC:  make(chan error),
	}
}

// Start launches a goroutine to process rangefeed events and send them to
// registrations. The provided stopper is used to finish processing.
func (p *Processor) Start(stopper *stop.Stopper) {
	const intentPushInterval = 250 * time.Millisecond
	const checkStreamsInterval = 100 * time.Millisecond

	// WIP: annotate context.
	ctx := context.Background()
	stopper.RunWorker(ctx, func(ctx context.Context) {
		// Assert that all registrations have disconnected before exiting.
		defer func() {
			if p.reg.Len() != 0 {
				panic("registry not empty")
			}
		}()

		// intentPushTicker periodically pushes all unresolved intents that are
		// above a certain age, helping to ensure that the resolved timestamp
		// continues to make progress.
		intentPushTicker := time.NewTicker(intentPushInterval)
		intentPushTickerC := intentPushTicker.C
		defer intentPushTicker.Stop()
		var intentPushAttemptC chan struct{}

		// checkStreamsTicker periodically checks whether any streams have
		// disconnected. If so, the registration is unregistered.
		checkStreamsTicker := time.NewTicker(checkStreamsInterval)
		defer checkStreamsTicker.Stop()

		for {
			select {
			// Handle new registrations.
			case r := <-p.regC:
				p.reg.Register(r)

			// Respond to answers about the processor goroutine state.
			case <-p.lenC:
				p.lenC <- p.reg.Len()

			// Transform and route events.
			case event := <-p.eventC:
				switch {
				case len(event.ops) > 0:
					p.consumeLogicalOps(ctx, event.ops)
				case event.ct != hlc.Timestamp{}:
					p.forwardClosedTS(ctx, event.ct)
				default:
					panic("missing event variant")
				}

			// Check whether any unresolved intents need a push.
			case <-intentPushTickerC:
				// WIP: maybe move to a method somewhere.
				// WIP: pull out this constant duration.
				before := p.clock.Now().Add(-10*time.Second.Nanoseconds(), 0)
				oldTxns := p.rts.intentQ.Before(before)

				if len(oldTxns) > 0 {
					// Set the ticker channel to nil so that it can't trigger a second
					// concurrent push. Create a push attempt response channel.
					intentPushTickerC = nil
					intentPushAttemptC = make(chan struct{})

					if err := stopper.RunAsyncTask(
						ctx, "rangefeed: WIP need a good name for this",
						func(ctx context.Context) {
							fmt.Printf("need to push the following txns: %v\n", oldTxns)
							intentPushAttemptC <- struct{}{}
						},
					); err != nil {
						// Ignore. stopper.ShouldQuiesce will trigger soon.
					}
				}

			// Update the resolved timestamp based on the push attempt.
			case <-intentPushAttemptC:
				// Reset the ticker channel so that it can trigger push attempts
				// again. Set the push attempt channel back to nil.
				intentPushTickerC = intentPushTicker.C
				intentPushAttemptC = nil

				// WIP: update p.rts based on push attempt.

			// Check whether any streams have disconnected.
			case <-checkStreamsTicker.C:
				p.reg.CheckStreams()

			// Close registrations and exit when signalled.
			case err := <-p.stopC:
				p.reg.DisconnectWithErr(all, err)
				return

			// Exit on stopper.
			case <-stopper.ShouldQuiesce():
				// WIP: should this return an error?
				p.reg.Disconnect(all)
				return
			}
		}
	})

	// WIP: catch up scan. Also need this on registration.
}

// Register registers the stream over the specified span of keys. The channel
// will be provided an error when the registration closes. NOT safe to call on
// nil Processor.
func (p *Processor) Register(span roachpb.Span, stream Stream, errC chan<- error) {
	p.regC <- registration{
		keys:   span.AsRange(),
		stream: stream,
		errC:   errC,
	}
}

// Len returns the number of registrations attached to the processor.
func (p *Processor) Len() int {
	if p == nil {
		return 0
	}
	// Ask the processor goroutine.
	p.lenC <- 0
	return <-p.lenC
}

// Stop shuts down the processor and closes all registrations. Safe to call on
// nil Processor.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr shuts down the processor and closes all registrations with the
// specified error. Safe to call on nil Processor.
func (p *Processor) StopWithErr(err error) {
	if p == nil {
		return
	}
	// Send on the channel instead of closing it. This ensures synchronous
	// communication so that when this method returns the caller can be sure
	// that the Processor goroutine has cancelled all registrations and shut
	// down.
	p.stopC <- err
}

// ConsumeLogicalOps informs the rangefeed processor of the set of logical
// operations. Safe to call on nil Processor.
func (p *Processor) ConsumeLogicalOps(ops []enginepb.MVCCLogicalOp) {
	if p == nil {
		return
	}
	// WIP: backpressure or disconnect on blocking call.
	p.eventC <- event{ops: ops}
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the rangefeed processor's resolved timestamp has advanced. Safe to call
// on nil Processor.
func (p *Processor) ForwardClosedTS(closedTS hlc.Timestamp) {
	if p == nil {
		return
	}
	p.eventC <- event{ct: closedTS}
}

func (p *Processor) consumeLogicalOps(ctx context.Context, ops []enginepb.MVCCLogicalOp) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			val := roachpb.Value{RawBytes: t.Value, Timestamp: t.Timestamp}
			p.publishValue(t.Key, val)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// An intent was just committed. Read the committed value from the
			// engine and publish it. It is very likely that we hit RocksDB's
			// memtable because we just wrote the value, so this should be
			// quick.
			key := t.Key
			val, _, err := engine.MVCCGetWithTombstone(ctx, p.eng,
				key, t.Timestamp, true /* consistent */, nil /* txn */)
			if val == nil && err == nil {
				err = errors.Errorf("value missing for key %v @ ts %v", key, t.Timestamp)
			}
			if err != nil {
				p.reg.DisconnectWithErr(roachpb.Span{Key: key}, err)
				continue
			}
			p.publishValue(key, *val)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(op) {
			p.publishCheckpoint()
		}
	}
}

func (p *Processor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint()
	}
}

func (p *Processor) publishValue(key roachpb.Key, value roachpb.Value) {
	span := roachpb.Span{Key: key}
	var event roachpb.RangeFeedEvent
	event.SetValue(&roachpb.RangeFeedValue{
		Key:   key,
		Value: value,
	})
	p.reg.PublishToOverlapping(span, &event)
}

func (p *Processor) publishCheckpoint() {
	// WIP: persist resolvedTimestamp. Give Processor a client.DB.
	// WIP: rate limit these? send them periodically?

	var event roachpb.RangeFeedEvent
	event.SetValue(&roachpb.RangeFeedCheckpoint{
		Span:       p.span.AsRawSpanWithNoLocals(),
		ResolvedTS: p.rts.Get(),
	})
	p.reg.PublishToOverlapping(p.span.AsRawSpanWithNoLocals(), &event)
}
