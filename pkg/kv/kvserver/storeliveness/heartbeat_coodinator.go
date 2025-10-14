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
	"github.com/cockroachdb/cockroach/pkg/util/taskpacer"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var HeartbeatBatchingDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_batching_duration",
	"duration over which heartbeat messages are batched to per-node destination queues;",
	10*time.Millisecond,
)

var HeartbeatSmearDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_smear_duration",
	"duration over which heartbeat messages are smeared to prevent goroutine spikes; "+
		"all heartbeats for all nodes are sent within this window at the start of each tick",
	10*time.Millisecond,
)

type heartbeatPacerConfig struct {
	refresh time.Duration
	smear   time.Duration
}

func (c *heartbeatPacerConfig) GetRefresh() time.Duration {
	return c.refresh
}

func (c *heartbeatPacerConfig) GetSmear() time.Duration {
	return c.smear
}

type HeartbeatCoordinator struct {
	transport BatchMessageSender
	stopper   *stop.Stopper
	settings  *cluster.Settings
	// nodeQueues maps node IDs to their destination queues.
	// The node IDs represent the destination nodes of the heartbeat messages.
	// This map is used for efficient batched sending of heartbeat messages.
	nodeQueues syncutil.Map[roachpb.NodeID, DestinationQueue]
	// signalChan is a channel used to signal the HeartbeatCoordinator to start
	// sending heartbeat messages to their destination nodes in a smeared manner.
	signalChan chan struct{}
	metrics    *HeartbeatCoordinatorMetrics
}

type DestinationQueue struct {
	messages chan slpb.Message
}

var _ MessageSender = (*HeartbeatCoordinator)(nil)

func NewHeartbeatCoordinator(
	transport BatchMessageSender, stopper *stop.Stopper, settings *cluster.Settings,
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
		// N.B. this outer most case captures the first signal.
		case <-c.signalChan:
			// N.B. (cont.) once the first signal is captured, we start a batching timer.
			batchingDuration := HeartbeatBatchingDuration.Get(&c.settings.SV)
			batchTimer.Reset(batchingDuration)
			for done := false; !done; {
				select {
				// N.B. (cont.) this inner most case captures & ingores subsequent
				// signals that occur within the batching window.
				case <-c.signalChan:
					// Do nothing here.

				case <-batchTimer.C:
					// N.B. (cont.) once the batching window is reached, we break
					// out of the loop. This pattern essentially collects multiple
					// signals and acts as a batching mechanism.
					done = true
				}
			}
			// N.B. (fin.) thi is to increase the bactches we send per batching
			// window as we wait for other stores to enqueue their messages.
			// There's no guarentee that all stores, even though they
			// share the HeartbeatTicker, will signal the coordinator at exactly
			// the same time. This mecahnism ensures we account for jitter in
			// signaling times from across all stores' goroutines.

			// Process the batch.
			c.drainAndSmear(ctx)

		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

// SendAsync implements MessageSender.
// This method is a thin shim over the underlying transport's SendAsync:
//   - Heartbeat responses (MsgHeartbeatResp) are sent immediately to bypass smearing,
//     with metrics and logging recorded here.
//   - All other messages are simply forwarded to the transport.
//
// Note: heartbeat request batching/smearing is handled via Enqueue/SignalToSend,
// not through SendAsync.
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
	return c.transport.SendAsync(ctx, msg)
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
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue *DestinationQueue) bool {
		totalMessages += len(queue.messages)
		return true
	})
	c.metrics.TotalMessagesInQueues.Update(int64(totalMessages))
}

// getOrCreateQueue returns the queue for the given nodeID, creating it if necessary.
// Returns (queue, isNew) where isNew indicates if the queue was just created.
func (c *HeartbeatCoordinator) getOrCreateQueue(nodeID roachpb.NodeID) (DestinationQueue, bool) {
	if queue, ok := c.nodeQueues.Load(nodeID); ok {
		return *queue, false
	}

	queue := &DestinationQueue{
		messages: make(chan slpb.Message, 1000),
	}

	stored, loaded := c.nodeQueues.LoadOrStore(nodeID, queue)
	return *stored, !loaded
}

func (c *HeartbeatCoordinator) ClearQueues() {
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue *DestinationQueue) bool {
		for {
			select {
			case <-queue.messages:
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

func (c *HeartbeatCoordinator) drainAndSmear(ctx context.Context) {
	startTime := timeutil.Now()

	var nodeIDs []roachpb.NodeID
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue *DestinationQueue) bool {
		if len(queue.messages) > 0 {
			nodeIDs = append(nodeIDs, nodeID)
		}
		return true
	})

	if len(nodeIDs) == 0 {
		return
	}

	// Sort for deterministic ordering.
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	smearDuration := HeartbeatSmearDuration.Get(&c.settings.SV)
	pacerConfig := &heartbeatPacerConfig{
		refresh: smearDuration,
		smear:   smearDuration / 10,
	}
	pacer := taskpacer.New(pacerConfig)
	pacer.StartTask(timeutil.Now())

	workLeft := len(nodeIDs)
	nodeIndex := 0

	for workLeft > 0 {
		todo, by := pacer.Pace(timeutil.Now(), workLeft)

		if todo <= 0 {
			c.metrics.PacerErrors.Inc(1)
			break
		}

		for j := 0; j < todo && nodeIndex < len(nodeIDs); j++ {
			nodeID := nodeIDs[nodeIndex]
			c.processNodeQueue(ctx, nodeID)
			nodeIndex++
		}

		workLeft -= todo

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
