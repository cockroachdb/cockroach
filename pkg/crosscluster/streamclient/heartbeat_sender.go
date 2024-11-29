// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// HeartbeatSender periodically sends a heartbeat for the given
// streamID to the given client.
type HeartbeatSender struct {
	client          Client
	streamID        streampb.StreamID
	frequencyGetter func() time.Duration

	lastSent time.Time
	frontier hlc.Timestamp
	// cg runs the HeartbeatSender thread.
	cg ctxgroup.Group
	// cancel stops heartbeat sender.
	cancel func()

	// HeartbeatSender closes this channel when it stops.
	StoppedChan     chan struct{}
	FrontierUpdates chan hlc.Timestamp
}

// NewHeartbeatSender creates a new HeartbeatSender.
func NewHeartbeatSender(
	ctx context.Context,
	client Client,
	streamID streampb.StreamID,
	frequencyGetter func() time.Duration,
) *HeartbeatSender {
	return &HeartbeatSender{
		client:          client,
		streamID:        streamID,
		frequencyGetter: frequencyGetter,
		cancel:          func() {},
		FrontierUpdates: make(chan hlc.Timestamp),
		StoppedChan:     make(chan struct{}),
	}
}

func (h *HeartbeatSender) Start(ctx context.Context, ts timeutil.TimeSource) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.cg = ctxgroup.WithContext(ctx)
	h.cg.GoCtx(func(ctx context.Context) error {
		sendHeartbeats := func() error {
			// The heartbeat thread send heartbeats when there is a frontier update,
			// and it has been a while since last time we sent it, or when we need
			// to heartbeat to keep the stream alive even if the frontier has no update.
			timer := ts.NewTimer()
			timer.Reset(h.frequencyGetter())
			defer timer.Stop()
			unknownStreamStatusRetryErr := log.Every(1 * time.Minute)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.Ch():
					timer.MarkRead()
					timer.Reset(h.frequencyGetter())
				case frontier := <-h.FrontierUpdates:
					h.frontier.Forward(frontier)
				}

				sent, streamStatus, err := h.maybeHeartbeat(ctx, ts, h.frontier, h.frequencyGetter())
				if err != nil {
					log.Errorf(ctx, "replication stream %d received an error from the producer job: %v", h.streamID, err)
					continue
				}

				if !sent || streamStatus.StreamStatus == streampb.StreamReplicationStatus_STREAM_ACTIVE {
					continue
				}

				if streamStatus.StreamStatus == streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
					if unknownStreamStatusRetryErr.ShouldLog() {
						log.Warningf(ctx, "replication stream %d has unknown stream status error and will retry later", h.streamID)
					}
					continue
				}
				// The replication stream is either paused or inactive.
				return crosscluster.NewStreamStatusErr(h.streamID, streamStatus.StreamStatus)
			}
		}
		err := sendHeartbeats()
		close(h.StoppedChan)
		return err
	})
}

func (h *HeartbeatSender) maybeHeartbeat(
	ctx context.Context,
	ts timeutil.TimeSource,
	frontier hlc.Timestamp,
	heartbeatFrequency time.Duration,
) (bool, streampb.StreamReplicationStatus, error) {
	if h.lastSent.Add(heartbeatFrequency).After(ts.Now()) {
		return false, streampb.StreamReplicationStatus{}, nil
	}
	h.lastSent = ts.Now()
	s, err := h.client.Heartbeat(ctx, h.streamID, frontier)
	return true, s, err
}

// Stop the heartbeat loop and returns any error at time of HeartbeatSender's exit.
// Can be called multiple times.
func (h *HeartbeatSender) Stop() error {
	h.cancel()
	return h.Wait()
}

// Wait for HeartbeatSender to be stopped and returns any error.
func (h *HeartbeatSender) Wait() error {
	err := h.cg.Wait()
	// We expect to see context cancelled when shutting down.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
