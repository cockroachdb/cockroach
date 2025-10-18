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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskpacer"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// heartbeatPacerConfig implements taskpacer.Config for heartbeat message pacing.
type heartbeatPacerConfig struct {
	refresh time.Duration // Interval between tasks (smear duration)
	smear   time.Duration // Interval between batches within a task
}

func (c *heartbeatPacerConfig) GetRefresh() time.Duration {
	return c.refresh
}

func (c *heartbeatPacerConfig) GetSmear() time.Duration {
	return c.smear
}

type HeartbeatCoordinator struct {
	transport    BatchMessageSender
	tickDuration time.Duration
	stopper      *stop.Stopper
	settings     *cluster.Settings

	nodeQueues syncutil.Map[roachpb.NodeID, *DestinationQueue]

	signalChan chan struct{} // NEW: buffered size 1

	metrics *HeartbeatCoordinatorMetrics
}

type DestinationQueue struct {
	messages chan slpb.Message
}

// Ensure HeartbeatCoordinator implements MessageSender interface
var _ MessageSender = (*HeartbeatCoordinator)(nil)

// NewHeartbeatCoordinator creates a new HeartbeatCoordinator.
func NewHeartbeatCoordinator(
	transport BatchMessageSender,
	tickDuration time.Duration,
	stopper *stop.Stopper,
	settings *cluster.Settings,
) *HeartbeatCoordinator {
	metrics := newHeartbeatCoordinatorMetrics()

	// Initialize gauge metrics
	metrics.TickDuration.Update(tickDuration.Milliseconds())
	smearDuration := HeartbeatSmearDuration.Get(&settings.SV)
	metrics.SmearDuration.Update(smearDuration.Milliseconds())

	log.KvExec.Infof(context.Background(), "HeartbeatCoordinator created with tick_duration=%v, smear_duration=%v",
		tickDuration, smearDuration)

	c := &HeartbeatCoordinator{
		transport:    transport,
		tickDuration: tickDuration,
		stopper:      stopper,
		settings:     settings,
		signalChan:   make(chan struct{}, 1),
		metrics:      metrics,
	}

	// Start the coordinator goroutine
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

// SendAsync implements the MessageSender interface.
// Heartbeat responses are sent immediately to bypass smearing.
// Heartbeat requests are enqueued for smeared delivery.
func (c *HeartbeatCoordinator) SendAsync(ctx context.Context, msg slpb.Message) bool {
	// Bypass smearing for responses - send immediately
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

	// Enqueue requests for smeared delivery
	c.metrics.MessagesEnqueued.Inc(1)
	log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator enqueuing request to node %d", msg.To.NodeID)

	c.Enqueue([]slpb.Message{msg})
	return true
}

// Enqueue adds messages to the appropriate destination queue.
func (c *HeartbeatCoordinator) Enqueue(messages []slpb.Message) int {
	log.KvExec.VInfof(context.Background(), 2, "HeartbeatCoordinator enqueuing %d messages", len(messages))
	successes := 0
	for _, msg := range messages {
		nodeID := msg.To.NodeID

		// Get or create queue for this destination
		queue, isNew := c.getOrCreateQueue(nodeID)
		if isNew {
			c.metrics.ActiveQueues.Inc(1)
			log.KvExec.VInfof(context.Background(), 1, "HeartbeatCoordinator created new queue for node %d", nodeID)
		}

		// Enqueue the message
		queue.messages <- msg
		successes++
	}

	// Update total messages in queues metric
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
	// Try to get existing queue
	if queue, ok := c.nodeQueues.Load(nodeID); ok {
		return *queue, false
	}

	// Create new queue with buffered channel
	queue := &DestinationQueue{
		messages: make(chan slpb.Message, 1000),
	}

	// Store the new queue (or get existing if another goroutine created it)
	stored, loaded := c.nodeQueues.LoadOrStore(nodeID, &queue)
	return *stored, !loaded
}

// drainAndSmear drains all queues and sends messages with pacing over the smear duration.
func (c *HeartbeatCoordinator) drainAndSmear(ctx context.Context) {
	startTime := timeutil.Now()

	// Collect all nodeIDs that have messages
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

	// Sort for deterministic ordering
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	// Setup pacer for smearing
	smearDuration := HeartbeatSmearDuration.Get(&c.settings.SV)
	pacerConfig := &heartbeatPacerConfig{
		refresh: smearDuration,
		smear:   smearDuration / 10,
	}
	pacer := taskpacer.New(pacerConfig)
	pacer.StartTask(timeutil.Now())

	// Process each node with pacing
	workLeft := len(nodeIDs)
	nodeIndex := 0

	for workLeft > 0 {
		todo, by := pacer.Pace(timeutil.Now(), workLeft)

		if todo <= 0 {
			c.metrics.PacerErrors.Inc(1)
			break
		}

		// Process batch of nodes
		for j := 0; j < todo && nodeIndex < len(nodeIDs); j++ {
			nodeID := nodeIDs[nodeIndex]
			c.processNodeQueue(ctx, nodeID)
			nodeIndex++
		}

		workLeft -= todo

		// Sleep until next batch
		if workLeft > 0 && by.After(timeutil.Now()) {
			sleepDuration := by.Sub(timeutil.Now())
			if sleepDuration > 0 {
				time.Sleep(sleepDuration)
			}
		}
	}

	totalDuration := timeutil.Since(startTime)
	log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator processed %d nodes in %v", len(nodeIDs), totalDuration)
}

// processScheduledNodes function removed - processing is now synchronous

// processNodeQueue processes all messages in a specific node's queue.
func (c *HeartbeatCoordinator) processNodeQueue(ctx context.Context, nodeID roachpb.NodeID) {
	queue, ok := c.nodeQueues.Load(nodeID)
	if !ok {
		return
	}

	// Drain channel into slice
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

	// Send batch directly via Transport, bypassing its queues
	success := c.transport.SendBatchDirect(ctx, nodeID, messages)
	if !success {
		c.metrics.SendErrors.Inc(int64(len(messages)))
		log.KvExec.Warningf(ctx, "Failed to send batch to node %d", nodeID)
	} else {
		c.metrics.MessagesSent.Inc(int64(len(messages)))
	}
}

// ClearQueues clears all pending messages from all queues.
// This is useful when heartbeats are disabled to prevent processing
// of previously enqueued messages.
func (c *HeartbeatCoordinator) ClearQueues() {
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		// Drain channel
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

// run contains the main processing goroutine which waits for signals and processes batches.
func (c *HeartbeatCoordinator) run(ctx context.Context) {
	var batchTimer timeutil.Timer
	defer batchTimer.Stop()
	for {
		select {
		case <-c.signalChan:
			// Collection window: allow other stores to enqueue
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
			c.drainAndSmear(ctx)

		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

// drainPendingSignals drains any additional signals that arrived during the collection window.
func (c *HeartbeatCoordinator) drainPendingSignals() {
	for {
		select {
		case <-c.signalChan:
			c.metrics.SignalsDrained.Inc(1)
		default:
			return
		}
	}
}

// Signal sends a signal to the coordinator to process pending messages.
// This is non-blocking - if the coordinator is already processing, the signal is ignored.
func (c *HeartbeatCoordinator) Signal() {
	select {
	case c.signalChan <- struct{}{}:
		c.metrics.SignalsAccepted.Inc(1)
	default:
		c.metrics.SignalsIgnored.Inc(1)
	}
}
