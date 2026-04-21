// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// testTrackerClient wraps per-applier DistDepResolverClients and
// TrackerServers, simulating the DistSQL dep resolver processor's event
// routing in-process. Each applier's outCh events are routed to the correct
// TrackerServer; resulting updates are delivered through loopback channels
// to RunBackchannelForwarder goroutines, which handle local delivery and
// remote forwarding -- mirroring the production data path exactly.
type testTrackerClient struct {
	clients   map[ldrdecoder.ApplierID]*DistDepResolverClient
	servers   map[ldrdecoder.ApplierID]*TrackerServer
	loopbacks map[ldrdecoder.ApplierID]chan DependencyUpdate
}

// NewTestDependencyTrackerClient creates a DependencyResolverClient backed by
// per-applier DistDepResolverClients wired to TrackerServers via in-process
// routing goroutines. The caller must cancel ctx to stop the background
// goroutines and then call the returned cleanup function to wait for them to
// exit and check for errors.
func NewTestDependencyTrackerClient(
	ctx context.Context, appliers []ldrdecoder.ApplierID,
) (DependencyResolverClient, func() error) {
	clients := make(map[ldrdecoder.ApplierID]*DistDepResolverClient, len(appliers))
	servers := make(map[ldrdecoder.ApplierID]*TrackerServer, len(appliers))
	loopbacks := make(map[ldrdecoder.ApplierID]chan DependencyUpdate, len(appliers))

	for _, id := range appliers {
		clients[id] = NewDistDepResolverClient(id)
		servers[id] = NewTrackerServer()
		loopbacks[id] = make(chan DependencyUpdate, 1000)
	}

	t := &testTrackerClient{clients: clients, servers: servers, loopbacks: loopbacks}

	grp := ctxgroup.WithContext(ctx)
	for _, id := range appliers {
		grp.GoCtx(func(ctx context.Context) error {
			return clients[id].RunBackchannelForwarder(ctx, loopbacks[id])
		})
		grp.GoCtx(func(ctx context.Context) error {
			return t.runRouter(ctx, clients[id])
		})
	}

	return t, grp.Wait
}

// runRouter drains events from a single client's OutCh and dispatches them to
// the correct TrackerServer, delivering resulting updates to the co-located
// applier's loopback channel.
func (t *testTrackerClient) runRouter(ctx context.Context, client *DistDepResolverClient) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-client.OutCh():
			if !ok {
				return nil
			}
			if err := t.processEvent(ctx, ev); err != nil {
				return err
			}
		}
	}
}

// processEvent handles a single DepResolverEvent by dispatching it to the
// correct TrackerServer and delivering resulting updates to the co-located
// applier's loopback channel. The RunBackchannelForwarder goroutine then
// handles local delivery or remote forwarding.
func (t *testTrackerClient) processEvent(ctx context.Context, ev DepResolverEvent) error {
	sendToLoopback := func(
		colocatedApplierID ldrdecoder.ApplierID, update DependencyUpdate,
	) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t.loopbacks[colocatedApplierID] <- update:
			return nil
		}
	}
	switch ev.Event.Type {
	case txnpb.DEP_RESOLVER_EVENT_WAIT:
		server := t.servers[ev.TargetApplierID]
		for _, txn := range ev.Event.Wait.WaitTxns {
			resolved, resolvedTime := server.MaybeAddWaiter(txn, ev.Event.Wait.WaitingID)
			if resolved {
				if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
					TxnID:           txn,
					ResolvedTime:    resolvedTime,
					TargetApplierID: ev.Event.Wait.WaitingID,
				}); err != nil {
					return err
				}
			}
		}
	case txnpb.DEP_RESOLVER_EVENT_WAIT_HORIZON:
		server := t.servers[ev.TargetApplierID]
		resolved, resolvedTime := server.WaitHorizon(
			ev.Event.WaitHorizon.WaitingID, ev.Event.WaitHorizon.TxnHorizon,
		)
		if resolved {
			if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
				TxnID:           ldrdecoder.TxnID{ApplierID: ev.Event.WaitHorizon.DependID},
				ResolvedTime:    resolvedTime,
				TargetApplierID: ev.Event.WaitHorizon.WaitingID,
			}); err != nil {
				return err
			}
		}
	case txnpb.DEP_RESOLVER_EVENT_READY:
		server := t.servers[ev.TargetApplierID]
		updates := server.Ready(ev.Event.Ready.ReadyTxn, ev.Event.Ready.ResolvedTime)
		for _, update := range updates {
			if err := sendToLoopback(ev.TargetApplierID, update); err != nil {
				return err
			}
		}
	case txnpb.DEP_RESOLVER_EVENT_FORWARD_UPDATE:
		if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
			TxnID:           ev.Event.ForwardUpdate.TxnID,
			ResolvedTime:    ev.Event.ForwardUpdate.ResolvedTime,
			TargetApplierID: ev.TargetApplierID,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Wait implements DependencyResolverClient.
func (t *testTrackerClient) Wait(waiter ldrdecoder.ApplierID, txns []ldrdecoder.TxnID) {
	t.clients[waiter].Wait(waiter, txns)
}

// WaitHorizon implements DependencyResolverClient.
func (t *testTrackerClient) WaitHorizon(
	applierID, dependID ldrdecoder.ApplierID, txnHorizon hlc.Timestamp,
) {
	t.clients[applierID].WaitHorizon(applierID, dependID, txnHorizon)
}

// Ready implements DependencyResolverClient.
func (t *testTrackerClient) Ready(txn ldrdecoder.TxnID, resolvedTime hlc.Timestamp) {
	t.clients[txn.ApplierID].Ready(txn, resolvedTime)
}

// Receive implements DependencyResolverClient.
func (t *testTrackerClient) Receive(applier ldrdecoder.ApplierID) <-chan DependencyUpdate {
	return t.clients[applier].Receive(applier)
}
