// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transport

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
)

// Config holds the information necessary to create a client registry.
type Config struct {
	Settings *cluster.Settings
	Stopper  *stop.Stopper
	NodeID   roachpb.NodeID
	Dialer   closedts.Dialer
	Sink     closedts.Notifyee
}

// Clients manages clients receiving closed timestamp updates from
// peer nodes, along with facilities to request information about certain
// ranges. Received updates are relayed to a provided Notifyee.
type Clients struct {
	cfg Config

	// TODO(tschottdorf): remove unused clients. Perhaps expiring them after,
	// say, 24h is enough? There is no interruption when doing so; the only
	// price is that a full update is sent, but that is pretty cheap too.
	clients syncutil.IntMap
}

var _ closedts.ClientRegistry = (*Clients)(nil)

// NewClients sets up a client registry.
func NewClients(cfg Config) *Clients {
	return &Clients{cfg: cfg}
}

type client struct {
	mu struct {
		syncutil.Mutex
		requested map[roachpb.RangeID]struct{} // never nil
	}
}

// Request is called when serving a follower read has failed due to missing or
// insufficient information. By calling this method, the caller gives the
// instruction to connect to the given node (if it hasn't already) and ask it to
// send (or re-send) up-to-date information about the specified range. Having
// done so, the information should soon thereafter be available to the Sink and
// from there, further follower read attempts. Does not block.
func (pr *Clients) Request(nodeID roachpb.NodeID, rangeID roachpb.RangeID) {
	// If the new closed timestamps is enabled, this old one is disabled.
	if pr.cfg.Settings.Version.IsActive(context.TODO(), clusterversion.ClosedTimestampsRaftTransport) {
		return
	}

	if nodeID == pr.cfg.NodeID {
		return
	}
	if cl := pr.getOrCreateClient(nodeID); cl != nil {
		cl.mu.Lock()
		cl.mu.requested[rangeID] = struct{}{}
		cl.mu.Unlock()
	}
}

// EnsureClient makes sure that updates from the given nodes are pulled in, if
// they aren't already. This call does not block (and is cheap).
func (pr *Clients) EnsureClient(nodeID roachpb.NodeID) {
	if nodeID == pr.cfg.NodeID {
		return
	}
	pr.getOrCreateClient(nodeID)
}

func (pr *Clients) getOrCreateClient(nodeID roachpb.NodeID) *client {
	// Fast path to check for existing client without an allocation.
	p, found := pr.clients.Load(int64(nodeID))
	cl := (*client)(p)
	if found {
		return cl
	}
	if !pr.cfg.Dialer.Ready(nodeID) {
		return nil
	}

	if nodeID == pr.cfg.NodeID {
		panic("must not create client to local node")
	}

	// Slow path: create the client. Another inserter might race us to it.

	// This allocates, so only do it when necessary.
	ctx := logtags.AddTag(context.Background(), "ct-client", "")

	cl = &client{}
	cl.mu.requested = map[roachpb.RangeID]struct{}{}

	if firstClient, loaded := pr.clients.LoadOrStore(int64(nodeID), unsafe.Pointer(cl)); loaded {
		return (*client)(firstClient)
	}

	// If our client made it into the map, start it. The point in inserting
	// before starting is to be able to collect RangeIDs immediately while never
	// blocking callers.
	if err := pr.cfg.Stopper.RunAsyncTask(ctx, "ct-client", func(ctx context.Context) {
		defer pr.clients.Delete(int64(nodeID))

		c, err := pr.cfg.Dialer.Dial(ctx, nodeID)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "error opening closed timestamp stream to n%d: %+v", nodeID, err)
			}
			return
		}
		defer func() {
			_ = c.CloseSend()
		}()

		ctx = c.Context()

		ch := pr.cfg.Sink.Notify(nodeID)
		defer close(ch)

		reaction := &ctpb.Reaction{}
		for {
			if err := c.Send(reaction); err != nil {
				return
			}
			entry, err := c.Recv()
			if err != nil {
				return
			}

			select {
			case ch <- *entry:
			case <-ctx.Done():
				return
			case <-pr.cfg.Stopper.ShouldQuiesce():
				return
			}

			var requested map[roachpb.RangeID]struct{}
			cl.mu.Lock()
			requested, cl.mu.requested = cl.mu.requested, map[roachpb.RangeID]struct{}{}
			cl.mu.Unlock()

			slice := make([]roachpb.RangeID, 0, len(requested))
			for rangeID := range requested {
				slice = append(slice, rangeID)
			}
			reaction = &ctpb.Reaction{
				Requested: slice,
			}
		}
	}); err != nil {
		pr.clients.Delete(int64(nodeID))
	}

	return cl
}
