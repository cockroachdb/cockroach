// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sort"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var HeartbeatBatchingDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_batching_duration",
	"duration over which heartbeat messages are batched to per-node destination queues;",
	10*time.Millisecond,
)

type HeartbeatCoordinator struct {
	transport  BatchMessageSender
	stopper    *stop.Stopper
	settings   *cluster.Settings
	nodeQueues syncutil.Map[roachpb.NodeID, *DestinationQueue]
	signalChan chan struct{}
	metrics    *HeartbeatCoordinatorMetrics
}

type DestinationQueue struct {
	messages chan slpb.Message
}

var _ MessageSender = (*HeartbeatCoordinator)(nil)

func NewHeartbeatCoordinator(
	transport BatchMessageSender,
	stopper *stop.Stopper,
	settings *cluster.Settings,
) *HeartbeatCoordinator {
	metrics := newHeartbeatCoordinatorMetrics()

	c := &HeartbeatCoordinator{
		transport:  transport,
		stopper:    stopper,
		settings:   settings,
		signalChan: make(chan struct{}, 1),
		metrics:    metrics,
	}

	if err := stopper.RunAsyncTask(
		context.Background(),
		"storeliveness.HeartbeatCoordinator: run",
		func(ctx context.Context) {
			c.run(ctx)
		},
	); err != nil {
		log.KvExec.Warningf(context.Background(), "failed to start HeartbeatCoordinator: %v", err)
	}

	return c
}

// run contains the main processing goroutine which waits for signals and processes heartbeats.
func (c *HeartbeatCoordinator) run(ctx context.Context) {
	var batchTimer timeutil.Timer
	defer batchTimer.Stop()
	for {
		select {
		case <-c.signalChan:
			batchingDuration := HeartbeatBatchingDuration.Get(&c.settings.SV)
			batchTimer.Reset(batchingDuration)
			for done := false; !done; {
				select {
				case <-c.signalChan:

				case <-batchTimer.C:
					done = true
				}
			}

			// Process the batch
			c.drainAndSend(ctx)

		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

// SendAsync implements the MessageSender interface.
// Heartbeat responses are sent immediately to bypass smearing.
// Heartbeat requests are enqueued for smeared delivery.
func (c *HeartbeatCoordinator) SendAsync(ctx context.Context, msg slpb.Message) bool {
	if msg.Type == slpb.MsgHeartbeatResp {
		c.metrics.MessagesSentImmediate.Inc(1)
		log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator sending immediate response to node %d", msg.To.NodeID)

		success := c.transport.SendAsync(ctx, msg)
		if !success {
			c.metrics.SendErrors.Inc(1)
			log.KvExec.Warningf(ctx, "Failed to send immediate heartbeat response to node %d", msg.To.NodeID)
		}
		return success
	}

	return true
}

// Enqueue adds messages to the appropriate destination queue.
func (c *HeartbeatCoordinator) Enqueue(messages []slpb.Message) int {
	log.KvExec.VInfof(context.Background(), 2, "HeartbeatCoordinator enqueuing %d messages", len(messages))
	successes := 0
	for _, msg := range messages {
		nodeID := msg.To.NodeID

		queue, isNew := c.getOrCreateQueue(nodeID)
		if isNew {
			c.metrics.ActiveQueues.Inc(1)
			log.KvExec.VInfof(context.Background(), 1, "HeartbeatCoordinator created new queue for node %d", nodeID)
		}

		queue.messages <- msg
		c.metrics.MessagesEnqueued.Inc(1)
		successes++
	}

	c.updateTotalMessagesMetric()

	return successes
}

// updateTotalMessagesMetric updates the total messages in queues metric.
func (c *HeartbeatCoordinator) updateTotalMessagesMetric() {
	totalMessages := 0
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		totalMessages += len((*queue).messages)
		return true
	})
	c.metrics.TotalMessagesInQueues.Update(int64(totalMessages))
}

// getOrCreateQueue returns the queue for the given nodeID, creating it if necessary.
// Returns (queue, isNew) where isNew indicates if the queue was just created.
func (c *HeartbeatCoordinator) getOrCreateQueue(nodeID roachpb.NodeID) (*DestinationQueue, bool) {
	if queue, ok := c.nodeQueues.Load(nodeID); ok {
		return *queue, false
	}

	queue := &DestinationQueue{
		messages: make(chan slpb.Message, 1000),
	}

	stored, loaded := c.nodeQueues.LoadOrStore(nodeID, &queue)
	return *stored, !loaded
}

func (c *HeartbeatCoordinator) ClearQueues() {
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		for {
			select {
			case <-(*queue).messages:
			default:
				return true
			}
		}
	})
	c.updateTotalMessagesMetric()
}

func (c *HeartbeatCoordinator) SignalToSend() {
	select {
	case c.signalChan <- struct{}{}:
		c.metrics.SignalsAccepted.Inc(1)
	default:
		c.metrics.SignalsIgnored.Inc(1)
	}
}

func (c *HeartbeatCoordinator) drainAndSend(ctx context.Context) {

	var nodeIDs []roachpb.NodeID
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		if len((*queue).messages) > 0 {
			nodeIDs = append(nodeIDs, nodeID)
		}
		return true
	})

	if len(nodeIDs) == 0 {
		return
	}

	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	for _, nodeID := range nodeIDs {
		c.processNodeQueue(ctx, nodeID)
	}

	log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator processed %d nodes", len(nodeIDs))
}

// processNodeQueue processes all messages in a specific node's queue.
func (c *HeartbeatCoordinator) processNodeQueue(ctx context.Context, nodeID roachpb.NodeID) {
	queue, ok := c.nodeQueues.Load(nodeID)
	if !ok {
		return
	}

	var messages []slpb.Message
	for {
		select {
		case msg := <-(*queue).messages:
			messages = append(messages, msg)
		default:
			goto done
		}
	}
done:

	if len(messages) == 0 {
		return
	}

	success := c.transport.SendBatchDirect(ctx, nodeID, messages)
	if !success {
		c.metrics.SendErrors.Inc(int64(len(messages)))
		log.KvExec.Warningf(ctx, "Failed to send batch to node %d", nodeID)
	} else {
		c.metrics.MessagesSent.Inc(int64(len(messages)))
	}
}
