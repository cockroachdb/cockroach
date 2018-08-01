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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const (
	// defaultCheckStreamsInterval is the default interval at which a Processor
	// will check all streams to make sure they have not been canceled.
	defaultCheckStreamsInterval = 100 * time.Millisecond
)

// Config encompasses the configuration required to create a Processor.
type Config struct {
	log.AmbientContext
	Clock *hlc.Clock
	Span  roachpb.RSpan
	// TODO(nvanbenschoten): add an IntentPusher dependency.

	EventChanCap         int
	PushIntentsInterval  time.Duration
	CheckStreamsInterval time.Duration
}

// SetDefaults initializes unset fields in Config to values
// suitable for use by a Processor.
func (sc *Config) SetDefaults() {
	if sc.CheckStreamsInterval == 0 {
		sc.CheckStreamsInterval = defaultCheckStreamsInterval
	}
}

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
// 1. it translates logical updates into rangefeed events.
// 2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//    timestamp.
type Processor struct {
	Config
	reg registry
	rts resolvedTimestamp

	regC    chan registration
	lenReqC chan struct{}
	lenResC chan int
	eventC  chan event
	stopC   chan *roachpb.Error
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
func NewProcessor(cfg Config) *Processor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	return &Processor{
		Config: cfg,
		reg:    makeRegistry(),
		rts:    makeResolvedTimestamp(),

		regC:    make(chan registration),
		lenReqC: make(chan struct{}),
		lenResC: make(chan int),
		eventC:  make(chan event, cfg.EventChanCap),
		stopC:   make(chan *roachpb.Error),
	}
}

// Start launches a goroutine to process rangefeed events and send them to
// registrations. The provided stopper is used to finish processing.
func (p *Processor) Start(stopper *stop.Stopper) {
	ctx := p.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		// intentPushTicker periodically pushes all unresolved intents that are
		// above a certain age, helping to ensure that the resolved timestamp
		// continues to make progress.
		var intentPushTicker *time.Ticker
		var intentPushTickerC <-chan time.Time
		var intentPushAttemptC chan struct{}
		if p.PushIntentsInterval > 0 {
			intentPushTicker = time.NewTicker(p.PushIntentsInterval)
			intentPushTickerC = intentPushTicker.C
			defer intentPushTicker.Stop()
		}

		// checkStreamsTicker periodically checks whether any streams have
		// disconnected. If so, the registration is unregistered.
		checkStreamsTicker := time.NewTicker(p.CheckStreamsInterval)
		defer checkStreamsTicker.Stop()

		for {
			select {
			// Handle new registrations.
			case r := <-p.regC:
				if !p.Span.AsRawSpanWithNoLocals().Contains(r.span) {
					log.Fatalf(ctx, "registration %+v not contained in processor span %v", r, p.Span)
				}

				// TODO(nvanbenschoten): catch up scan.
				p.reg.Register(r)

			// Respond to answers about the processor goroutine state.
			case <-p.lenReqC:
				p.lenResC <- p.reg.Len()

			// Transform and route events.
			case e := <-p.eventC:
				p.consumeEvent(ctx, e)

			// Check whether any unresolved intents need a push.
			case <-intentPushTickerC:
				// TODO(nvanbenschoten): maybe move to a method somewhere.
				// TODO(nvanbenschoten): pull out this constant duration.
				before := p.Clock.Now().Add(-10*time.Second.Nanoseconds(), 0)
				oldTxns := p.rts.intentQ.Before(before)

				if len(oldTxns) > 0 {
					// Set the ticker channel to nil so that it can't trigger a second
					// concurrent push. Create a push attempt response channel.
					intentPushTickerC = nil
					intentPushAttemptC = make(chan struct{})

					if err := stopper.RunAsyncTask(ctx, "rangefeed: pushing old intents",
						func(ctx context.Context) {
							log.Infof(ctx, "need to push the following txns: %v\n", oldTxns)
							close(intentPushAttemptC)
						},
					); err != nil {
						// Ignore. stopper.ShouldQuiesce will trigger soon.
						// Set err to avoid "empty branch" lint warning.
						err = nil
					}
				}

			// Update the resolved timestamp based on the push attempt.
			case <-intentPushAttemptC:
				// Reset the ticker channel so that it can trigger push attempts
				// again. Set the push attempt channel back to nil.
				intentPushTickerC = intentPushTicker.C
				intentPushAttemptC = nil

				// TODO(nvanbenschoten): update p.rts based on push attempt.

			// Check whether any streams have disconnected.
			case <-checkStreamsTicker.C:
				p.reg.CheckStreams()

			// Close registrations and exit when signaled.
			case pErr := <-p.stopC:
				// Process any events still in the event channel.
				for loop := true; loop; {
					select {
					case e := <-p.eventC:
						p.consumeEvent(ctx, e)
					default:
						loop = false
					}
				}
				p.reg.DisconnectWithErr(all, pErr)
				return

			// Exit on stopper.
			case <-stopper.ShouldQuiesce():
				// TODO(nvanbenschoten): should this return an error?
				p.reg.Disconnect(all)
				return
			}
		}
	})
}

// Register registers the stream over the specified span of keys. The channel
// will be provided an error when the registration closes. NOT safe to call on
// nil Processor.
func (p *Processor) Register(
	span roachpb.RSpan, startTS hlc.Timestamp, stream Stream, errC chan<- *roachpb.Error,
) {
	p.regC <- registration{
		span:    span.AsRawSpanWithNoLocals(),
		startTS: startTS,
		stream:  stream,
		errC:    errC,
	}
}

// Len returns the number of registrations attached to the processor.
func (p *Processor) Len() int {
	if p == nil {
		return 0
	}
	// Ask the processor goroutine.
	p.lenReqC <- struct{}{}
	return <-p.lenResC
}

// Stop shuts down the processor and closes all registrations. Safe to call on
// nil Processor. It is not valid to restart a processor after it has been
// stopped.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr shuts down the processor and closes all registrations with the
// specified error. Safe to call on nil Processor. It is not valid to restart a
// processor after it has been stopped.
func (p *Processor) StopWithErr(pErr *roachpb.Error) {
	if p == nil {
		return
	}
	// Send on the channel instead of closing it. This ensures synchronous
	// communication so that when this method returns the caller can be sure
	// that the Processor goroutine is canceling all registrations and shutting
	// down.
	p.stopC <- pErr
}

// ConsumeLogicalOps informs the rangefeed processor of the set of logical
// operations. Safe to call on nil Processor.
func (p *Processor) ConsumeLogicalOps(ops []enginepb.MVCCLogicalOp) {
	if p == nil {
		return
	}
	if len(ops) == 0 {
		return
	}
	// TODO(nvanbenschoten): backpressure or disconnect on blocking call.
	p.eventC <- event{ops: ops}
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the rangefeed processor's resolved timestamp has advanced. Safe to call
// on nil Processor.
func (p *Processor) ForwardClosedTS(closedTS hlc.Timestamp) {
	if p == nil {
		return
	}
	if closedTS == (hlc.Timestamp{}) {
		return
	}
	p.eventC <- event{ct: closedTS}
}

func (p *Processor) consumeEvent(ctx context.Context, e event) {
	switch {
	case len(e.ops) > 0:
		p.consumeLogicalOps(ctx, e.ops)
	case e.ct != hlc.Timestamp{}:
		p.forwardClosedTS(ctx, e.ct)
	default:
		panic("missing event variant")
	}
}

func (p *Processor) consumeLogicalOps(ctx context.Context, ops []enginepb.MVCCLogicalOp) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			val := roachpb.Value{RawBytes: t.Value, Timestamp: t.Timestamp}
			p.publishValue(ctx, t.Key, val)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// Publish the newly committed value.
			val := roachpb.Value{RawBytes: t.Value, Timestamp: t.Timestamp}
			p.publishValue(ctx, t.Key, val)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(op) {
			p.publishCheckpoint(ctx)
		}
	}
}

func (p *Processor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *Processor) publishValue(ctx context.Context, key roachpb.Key, value roachpb.Value) {
	if !p.Span.ContainsKey(roachpb.RKey(key)) {
		log.Fatalf(ctx, "key %v not in Processor's key range %v", key, p.Span)
	}

	span := roachpb.Span{Key: key}
	var event roachpb.RangeFeedEvent
	event.SetValue(&roachpb.RangeFeedValue{
		Key:   key,
		Value: value,
	})
	p.reg.PublishToOverlapping(span, &event)
}

func (p *Processor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	span := p.Span.AsRawSpanWithNoLocals()
	var event roachpb.RangeFeedEvent
	event.SetValue(&roachpb.RangeFeedCheckpoint{
		Span:       span,
		ResolvedTS: p.rts.Get(),
	})
	p.reg.PublishToOverlapping(span, &event)
}
