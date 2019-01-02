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

package transport

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Server handles incoming closed timestamp update stream requests.
type Server struct {
	stopper *stop.Stopper
	p       closedts.Producer
	refresh closedts.RefreshFn
}

// NewServer sets up a Server which relays information from the given producer
// to incoming clients.
func NewServer(stopper *stop.Stopper, p closedts.Producer, refresh closedts.RefreshFn) *Server {
	return &Server{
		stopper: stopper,
		p:       p,
		refresh: refresh,
	}
}

var _ ctpb.Server = (*Server)(nil)

// Get handles incoming client connections.
func (s *Server) Get(client ctpb.InboundClient) error {
	// TODO(tschottdorf): the InboundClient API isn't great since it
	// is blocking. How can we eagerly terminate these connections when
	// the server shuts down? I think we need to inject a cancellation
	// into the context, but grpc hands that to us.
	// This problem has likely been solved somewhere in our codebase.
	ctx := client.Context()
	ch := make(chan ctpb.Entry, 10)

	if log.V(1) {
		log.Infof(ctx, "closed timestamp server serving new inbound client connection")
	}

	// TODO(tschottdorf): make this, say, 2*closedts.CloseFraction*closedts.TargetInterval.
	const closedTimestampNoUpdateWarnThreshold = 10 * time.Second
	t := timeutil.NewTimer()

	// NB: We can't use Stopper.RunWorker because doing so would race with
	// calling Stopper.Stop.
	if err := s.stopper.RunAsyncTask(ctx, "closedts-subscription", func(ctx context.Context) {
		s.p.Subscribe(ctx, ch)
	}); err != nil {
		return err
	}
	for {
		reaction, err := client.Recv()
		if err != nil {
			return err
		}

		if len(reaction.Requested) != 0 {
			s.refresh(reaction.Requested...)
		}

		t.Reset(closedTimestampNoUpdateWarnThreshold)
		var entry ctpb.Entry
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopper.ShouldQuiesce():
			return errors.New("node is draining")
		case entry, ok = <-ch:
			if !ok {
				return errors.New("subscription dropped unexpectedly")
			}
		case <-t.C:
			t.Read = true
			// Send an empty entry to the client, which can use that to warn
			// about the absence of heartbeats. We don't log here since it
			// would log a message per incoming stream, which makes little
			// sense. It's the producer's job to warn on this node.
		}
		if err := client.Send(&entry); err != nil {
			return err
		}
	}
}
