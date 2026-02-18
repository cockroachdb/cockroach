// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

func NewMuxSubscription(clients []streamclient.Subscription) streamclient.Subscription {
	return &muxSubscription{
		clients: clients,
		events:  make(chan crosscluster.Event),
	}
}

type muxSubscription struct {
	clients []streamclient.Subscription
	events  chan crosscluster.Event
	err     error
}

func (m *muxSubscription) Subscribe(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)

	var wg sync.WaitGroup
	wg.Add(len(m.clients))

	for _, client := range m.clients {
		c := client
		group.Go(func() error {
			return c.Subscribe(ctx)
		})
		group.Go(func() error {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ev, ok := <-c.Events():
					if !ok {
						return nil
					}
					// NOTE: we could force the client to drain the channel before
					// returning instead of checking for context cancellation here, but
					// the context cancellation indicates the proecess is shutting down
					// or restarting so its probably better to bail sooner and doesn't
					// depend on the consumer draining the channel.
					select {
					case <-ctx.Done():
						return ctx.Err()
					case m.events <- ev:
					}
				}
			}
		})
	}
	group.Go(func() error {
		wg.Wait()
		for _, client := range m.clients {
			m.err = errors.CombineErrors(m.err, client.Err())
		}
		close(m.events)
		return nil
	})
	return group.Wait()
}

func (m *muxSubscription) Events() <-chan crosscluster.Event {
	return m.events
}

func (m *muxSubscription) Err() error {
	return m.err
}
