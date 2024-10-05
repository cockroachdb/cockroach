// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

// mergedSubscription combines multiple subscriptions into a single
// merged stream of events.
type mergedSubscription struct {
	cg       ctxgroup.Group
	cgCancel context.CancelFunc
	eventCh  chan partitionEvent
}

func mergeSubscriptions(
	ctx context.Context, subscriptions map[string]streamclient.Subscription,
) *mergedSubscription {
	ctx, cancel := context.WithCancel(ctx)
	m := &mergedSubscription{
		cg:       ctxgroup.WithContext(ctx),
		cgCancel: cancel,
		eventCh:  make(chan partitionEvent),
	}
	for partition, sub := range subscriptions {
		partition := partition
		sub := sub
		m.cg.GoCtx(func(ctx context.Context) error {
			ctxDone := ctx.Done()
			for {
				select {
				case event, ok := <-sub.Events():
					if !ok {
						return sub.Err()
					}

					pe := partitionEvent{
						Event:     event,
						partition: partition,
					}

					select {
					case m.eventCh <- pe:
					case <-ctxDone:
						return ctx.Err()
					}
				case <-ctxDone:
					return ctx.Err()
				}
			}
		})
	}
	return m
}

// Run blocks until the merged stream is closed.
func (m *mergedSubscription) Run() error {
	err := m.cg.Wait()
	close(m.eventCh)
	return err
}

// Close stops the merged stream. Note that the underlying
// subscriptions are not closed.
func (m *mergedSubscription) Close() {
	m.cgCancel()
}

// Events returns the merged event channel.
func (m *mergedSubscription) Events() chan partitionEvent {
	return m.eventCh
}
