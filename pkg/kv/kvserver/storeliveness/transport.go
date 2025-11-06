// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"runtime/pprof"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskpacer"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

const (
	// Outgoing messages are queued per-node on a channel of this size.
	sendBufferSize = 1000

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	idleTimeout = time.Minute

	// The duration for which messages are continuously pulled from the queue to
	// be sent out as a batch.
	batchDuration = 10 * time.Millisecond

	// connClass is the rpc ConnectionClass used by Store Liveness traffic.
	connClass = rpcbase.SystemClass
)

// TODO(dodeca12): Currently this complexity allows the fallback to immediate
// heartbeat sends. Once the smearing has been battle-tested, remove this and
// default to using the smeared heartbeat sends approach (no more fallback).
var HeartbeatSmearingEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_smearing.enabled",
	"if enabled, heartbeat sends are smeared across a certain duration, "+
		"via the transport goroutine, "+
		"otherwise heartbeat sends are sent when they are enqueued "+
		"at the sendQueue, bypassing heartbeat smearing",

	true,
)

var HeartbeatSmearingRefreshInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_coordinator.refresh",
	"the total time window within which all queued heartbeat messages should be "+
		"sent; the pacer distributes the work across this duration to complete "+
		"by the deadline",
	10*time.Millisecond,
	settings.PositiveDuration,
)

var HeartbeatSmearingInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_coordinator.smear",
	"the interval between sending successive batches of heartbeats across all "+
		"send queues",
	1*time.Millisecond,
	settings.PositiveDuration,
)

var logQueueFullEvery = log.Every(1 * time.Second)

// MessageHandler is the interface that must be implemented by
// arguments to Transport.ListenMessages.
type MessageHandler interface {
	// HandleMessage is called for each incoming Message. This function shouldn't
	// block (e.g. do a synchronous disk write) to prevent a single store with a
	// problem (e.g. a stalled disk) from affecting message receipt by other
	// stores on the same node.
	HandleMessage(msg *slpb.Message) error
}

// sendQueue is a queue of outgoing Messages.
type sendQueue struct {
	// sendMessages is signaled by the transport'smearing sender goroutine
	// to tell processQueue to send messages (smearing mechanism).
	sendMessages chan struct{}
	// directSend is signaled when a message is enqueued (while smearing is
	// disabled) to tell processQueue to send immediately, bypassing the
	// smearing sender goroutine.
	directSend chan struct{}
	// messages is a channel of outgoing Messages.
	messages chan slpb.Message
}

// Transport handles the RPC messages for Store Liveness.
//
// Each node maintains a single instance of Transport that handles sending and
// receiving messages on behalf of all stores on the node.
//
// The transport is asynchronous with respect to the caller, and internally
// multiplexes outbound messages by queuing them on a per-node queue before
// delivering them asynchronously.
type Transport struct {
	log.AmbientContext
	stopper  *stop.Stopper
	clock    *hlc.Clock
	dialer   *nodedialer.Dialer
	metrics  *TransportMetrics
	settings *clustersettings.Settings

	// queues stores outgoing message queues keyed by the destination node ID.
	queues syncutil.Map[roachpb.NodeID, sendQueue]
	// handlers stores the MessageHandler for each store on the node.
	handlers syncutil.Map[roachpb.StoreID, MessageHandler]

	// sendAllMessages is signaled to instruct the heartbeat smearing sender to
	// signal all sendQueues to send their enqueued messages.
	sendAllMessages chan struct{}

	// TransportKnobs includes all knobs for testing.
	knobs *TransportKnobs
}

var _ MessageSender = (*Transport)(nil)

type pacerConfig struct {
	settings *clustersettings.Settings
}

// GetRefresh implements the taskpacer.Config interface.
func (c pacerConfig) GetRefresh() time.Duration {
	return HeartbeatSmearingRefreshInterval.Get(&c.settings.SV)
}

// GetSmear implements the taskpacer.Config interface.
func (c pacerConfig) GetSmear() time.Duration {
	return HeartbeatSmearingInterval.Get(&c.settings.SV)
}

// SendHeartbeatsSmeared returns true if the cluster setting to send heartbeats
// via the heartbeat smearing sender (the goroutine in `transport.go`) is enabled.
// The smearing sender is responsible for smearing the heartbeats over
// a certain duration, so expect heartbeats sends to be paced when enabled.
func (t *Transport) SendHeartbeatsSmeared() bool {
	return HeartbeatSmearingEnabled.Get(&t.settings.SV)
}

// NewTransport creates a new Store Liveness Transport.
func NewTransport(
	ambient log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	drpcMux drpc.Mux,
	settings *clustersettings.Settings,
	knobs *TransportKnobs,
) (*Transport, error) {
	if knobs == nil {
		knobs = &TransportKnobs{}
	}
	t := &Transport{
		AmbientContext:  ambient,
		stopper:         stopper,
		clock:           clock,
		dialer:          dialer,
		metrics:         newTransportMetrics(),
		settings:        settings,
		sendAllMessages: make(chan struct{}, 1),
		knobs:           knobs,
	}
	if grpcServer != nil {
		slpb.RegisterStoreLivenessServer(grpcServer, t)
	}
	if drpcMux != nil {
		if err := slpb.DRPCRegisterStoreLiveness(drpcMux, t.AsDRPCServer()); err != nil {
			return nil, err
		}
	}

	// Start background goroutine to act as the transport smearing sender.
	// It is responsible for smearing the heartbeat sends across a certain duration.
	if err := stopper.RunAsyncTask(
		context.Background(), "storeliveness transport smearing sender",
		func(ctx context.Context) {
			var batchTimer timeutil.Timer
			defer batchTimer.Stop()

			conf := pacerConfig{settings: settings}
			pacer := taskpacer.New(conf)

			// This will hold the channels we need to signal to send messages.
			toSignal := make([]chan struct{}, 0)

			for {
				select {
				case <-stopper.ShouldQuiesce():
					return
				case <-t.sendAllMessages:
					// We received a signal to send all messages.
					// Wait for a short duration to give other stores a chance to
					// enqueue messages which will increase batching opportunities.
					batchTimer.Reset(batchDuration)
					for done := false; !done; {
						select {
						case <-t.sendAllMessages:
							// Consume any additional signals to send all messages.

						case <-batchTimer.C:
							// We have waited to batch messages.
							done = true
						}
					}

					// Collect all sendQueues.
					t.queues.Range(func(nodeID roachpb.NodeID, q *sendQueue) bool {
						// This check is vital to avoid unnecessary signaling
						// on empty queues - otherwise, especially on
						// heartbeat responses, we'd wake up N processesQueue
						// goroutines where N is the number of destination
						// nodes.
						if len(q.messages) == 0 { // no messages to send
							return true
						}
						toSignal = append(toSignal, q.sendMessages)
						return true
					})

					// There is a benign race condition where a new message may be
					// enqueued to a new queue after we collect the toSignal channels.
					// In this case, t.sendAllMessages will be set again, and we will
					// pick it up in the next iteration of the for loop.

					// Pace the signalling of the channels.
					pacer.StartTask(timeutil.Now())
					workLeft := len(toSignal)
					for workLeft > 0 {
						todo, by := pacer.Pace(timeutil.Now(), workLeft)

						// Pop todo items off the toSignal slice and signal them.
						for i := 0; i < todo && workLeft > 0; i++ {
							ch := toSignal[len(toSignal)-1]
							toSignal = toSignal[:len(toSignal)-1]
							select {
							case ch <- struct{}{}:
							default:
							}
							workLeft--
						}

						if workLeft > 0 {
							if wait := timeutil.Until(by); wait > 0 {
								time.Sleep(wait)
							}
						}
					}
				}
			}
		},
	); err != nil {
		return nil, err
	}

	return t, nil
}

type drpcTransport Transport

// AsDRPCServer returns the DRPC server implementation for the StoreLiveness service.
func (t *Transport) AsDRPCServer() slpb.DRPCStoreLivenessServer {
	return (*drpcTransport)(t)
}

// Metrics returns metrics tracking this transport.
func (t *Transport) Metrics() *TransportMetrics {
	return t.metrics
}

// ListenMessages registers a MessageHandler to receive proxied messages.
func (t *Transport) ListenMessages(storeID roachpb.StoreID, handler MessageHandler) {
	t.handlers.Store(storeID, &handler)
}

// Stream proxies the incoming requests to the corresponding store's
// MessageHandler. Messages between a pair of nodes are delivered in order
// only within the same RPC call; in other words, this guarantee does not hold
// across stream re-connects.
func (t *Transport) Stream(stream slpb.StoreLiveness_StreamServer) error {
	return t.stream(stream)
}

// Stream proxies the incoming requests to the corresponding store's
// MessageHandler. Messages between a pair of nodes are delivered in order
// only within the same RPC call; in other words, this guarantee does not hold
// across stream re-connects.
func (t *drpcTransport) Stream(stream slpb.DRPCStoreLiveness_StreamStream) error {
	return (*Transport)(t).stream(stream)
}

func (t *Transport) stream(stream slpb.RPCStoreLiveness_StreamStream) error {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	taskCtx, cancel := t.stopper.WithCancelOnQuiesce(stream.Context())
	taskCtx = t.AnnotateCtx(taskCtx)
	defer cancel()

	if err := t.stopper.RunAsyncTaskEx(
		taskCtx, stop.TaskOpts{
			TaskName: "storeliveness.Transport: processing incoming stream",
			SpanOpt:  stop.ChildSpan,
		}, func(ctx context.Context) {
			errCh <- func() error {
				// Infinite loop pulling incoming messages from the RPC service's stream.
				for {
					batch, err := stream.Recv()
					if err != nil {
						return err
					}
					t.metrics.BatchesReceived.Inc(1)
					if !batch.Now.IsEmpty() {
						t.clock.Update(batch.Now)
					}
					for i := range batch.Messages {
						t.handleMessage(ctx, &batch.Messages[i])
					}
				}
			}()
		},
	); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		return err
	case <-t.stopper.ShouldQuiesce():
		return nil
	}
}

// handleMessage proxies a request to the corresponding store's MessageHandler.
func (t *Transport) handleMessage(ctx context.Context, msg *slpb.Message) {
	handler, ok := t.handlers.Load(msg.To.StoreID)
	if !ok {
		log.KvExec.Warningf(
			ctx, "unable to accept message %+v from %+v: no handler registered for %+v",
			msg, msg.From, msg.To,
		)
		return
	}
	if err := (*handler).HandleMessage(msg); err != nil {
		if logQueueFullEvery.ShouldLog() {
			log.KvExec.Warningf(
				t.AnnotateCtx(context.Background()),
				"error handling message to store %v: %v", msg.To, err,
			)
		}
		t.metrics.MessagesReceiveDropped.Inc(1)
		return
	}
	t.metrics.MessagesReceived.Inc(1)
}

// EnqueueMessage implements the MessageSender interface. It enqueues a message
// to the sendQueue for the recipient node, and returns false if the outgoing
// queue is full or the node dialer's circuit breaker has tripped.
//
// The returned bool may be a false positive but will never be a false negative;
// if enqueued is true the message may or may not actually be sent but if it's
// false the message definitely was not enqueued.
func (t *Transport) EnqueueMessage(ctx context.Context, msg slpb.Message) (enqueued bool) {
	toNodeID := msg.To.NodeID
	fromNodeID := msg.From.NodeID
	// If this is a message from one local store to another local store, do not
	// dial the node and go through GRPC; instead, handle the message directly
	// using the corresponding message handler.
	if toNodeID == fromNodeID {
		t.metrics.MessagesSent.Inc(1)
		// Make a copy of the message to avoid escaping the function argument
		// msg to the heap.
		msgCopy := msg
		t.handleMessage(ctx, &msgCopy)
		return true
	}
	if b, ok := t.dialer.GetCircuitBreaker(toNodeID, connClass); ok && b.Signal().Err() != nil {
		t.metrics.MessagesSendDropped.Inc(1)
		return false
	}

	q, existingQueue := t.getQueue(toNodeID)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		ctx := t.AnnotateCtx(context.Background())
		if !t.startProcessNewQueue(ctx, toNodeID) {
			return false
		}
	}

	select {
	case q.messages <- msg:
		t.metrics.SendQueueSize.Inc(1)
		t.metrics.SendQueueBytes.Inc(int64(msg.Size()))
		// Signal the processQueue goroutine if in direct mode (smearing disabled).
		if !t.SendHeartbeatsSmeared() {
			select {
			case q.directSend <- struct{}{}:
			default:
			}
		}
		return true
	default:
		if logQueueFullEvery.ShouldLog() {
			log.KvExec.Warningf(
				t.AnnotateCtx(context.Background()),
				"store liveness send queue to n%d is full", toNodeID,
			)
		}
		t.metrics.MessagesSendDropped.Inc(1)
		return false
	}
}

// SendAllEnqueuedMessages signals all queues to send all their messages.
func (t *Transport) SendAllEnqueuedMessages(ctx context.Context) {
	select {
	case t.sendAllMessages <- struct{}{}:
	default:
	}
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *Transport) getQueue(nodeID roachpb.NodeID) (*sendQueue, bool) {
	queue, ok := t.queues.Load(nodeID)
	if !ok {
		q := sendQueue{
			sendMessages: make(chan struct{}, 1),
			directSend:   make(chan struct{}, 1),
			messages:     make(chan slpb.Message, sendBufferSize),
		}
		queue, ok = t.queues.LoadOrStore(nodeID, &q)
	}
	return queue, ok
}

// startProcessNewQueue connects to the node and launches a worker goroutine
// that processes the queue for the given node ID (which must exist) until
// the underlying connection is closed or an error occurs. This method
// takes on the responsibility of deleting the queue when the worker shuts down.
//
// Returns whether the worker was started (the queue is deleted either way).
func (t *Transport) startProcessNewQueue(
	ctx context.Context, toNodeID roachpb.NodeID,
) (started bool) {
	cleanup := func() {
		q, ok := t.getQueue(toNodeID)
		t.queues.Delete(toNodeID)
		// Account for all remaining messages in the queue. EnqueueMessage may be
		// writing to the queue concurrently, so it's possible that we won't
		// account for a few messages below.
		if ok {
			for {
				select {
				case m := <-q.messages:
					t.metrics.MessagesSendDropped.Inc(1)
					t.metrics.SendQueueSize.Dec(1)
					t.metrics.SendQueueBytes.Dec(int64(m.Size()))
				default:
					return
				}
			}
		}
	}
	worker := func(ctx context.Context) {
		q, existingQueue := t.getQueue(toNodeID)
		if !existingQueue {
			log.KvExec.Fatalf(ctx, "queue for n%d does not exist", toNodeID)
		}
		defer cleanup()
		client, err := slpb.DialStoreLivenessClient(t.dialer, ctx, toNodeID, connClass)
		if err != nil {
			// DialNode already logs sufficiently, so just return.
			return
		}
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := client.Stream(streamCtx) // closed via cancellation
		if err != nil {
			log.KvExec.Warningf(ctx, "creating stream client for node %d failed: %s", toNodeID, err)
			return
		}

		if err = t.processQueue(q, stream); err != nil {
			log.KvExec.Warningf(ctx, "processing outgoing queue to node %d failed: %s:", toNodeID, err)
		}
	}
	err := t.stopper.RunAsyncTask(
		ctx, "storeliveness.Transport: sending messages",
		func(ctx context.Context) {
			pprof.Do(ctx, pprof.Labels("remote_node_id", toNodeID.String()), worker)
		},
	)
	if err != nil {
		cleanup()
		return false
	}
	return true
}

// processQueue opens a Store Liveness client stream and sends messages from the
// designated queue via that stream, exiting when an error is received or when
// it idles out. All messages remaining in the queue at that point are lost and
// a new instance of processQueue will be started by the next message to be sent.
func (t *Transport) processQueue(
	q *sendQueue, stream slpb.RPCStoreLiveness_StreamClient,
) (err error) {
	defer func() {
		_, closeErr := stream.CloseAndRecv()
		err = errors.Join(err, closeErr)
	}()

	getIdleTimeout := func() time.Duration {
		if overrideFn := t.knobs.OverrideIdleTimeout; overrideFn != nil {
			return overrideFn()
		} else {
			return idleTimeout
		}
	}
	var idleTimer timeutil.Timer
	defer idleTimer.Stop()
	var batchTimer timeutil.Timer
	defer batchTimer.Stop()
	batch := &slpb.MessageBatch{}

	for {
		idleTimer.Reset(getIdleTimeout())
		select {
		case <-t.stopper.ShouldQuiesce():
			return nil

		case <-idleTimer.C:
			t.metrics.SendQueueIdle.Inc(1)
			return nil

		case <-q.sendMessages:
			// Signal received - proceed to batching logic below.

		case <-q.directSend:
			// Signal received - proceed to batching logic below.
		}

		// Common batching logic: pull off as many queued messages as possible
		// within batchDuration, then send the batch.
		batchTimer.Reset(batchDuration)
		for done := false; !done; {
			select {
			case msg := <-q.messages:
				batch.Messages = append(batch.Messages, msg)
				t.metrics.SendQueueSize.Dec(1)
				t.metrics.SendQueueBytes.Dec(int64(msg.Size()))
			case <-batchTimer.C:
				done = true
			}
		}

		if len(batch.Messages) > 0 {
			batch.Now = t.clock.NowAsClockTimestamp()
			if err = stream.Send(batch); err != nil {
				t.metrics.MessagesSendDropped.Inc(int64(len(batch.Messages)))
				return err
			}
			t.metrics.BatchesSent.Inc(1)
			t.metrics.MessagesSent.Inc(int64(len(batch.Messages)))
			// Reuse the Messages slice, but zero out the contents to avoid delaying GC.
			for i := range batch.Messages {
				batch.Messages[i] = slpb.Message{}
			}
			batch.Messages = batch.Messages[:0]
			batch.Now = hlc.ClockTimestamp{}
		}
	}
}
