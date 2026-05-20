// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/util/container/nudge"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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
		loopbacks[id] = make(chan DependencyUpdate, 1)
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
		case <-client.OutCh():
			ev, ok := client.PopOutEvent()
			if !ok {
				continue
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
	switch e := ev.Event.Event.(type) {
	case *txnpb.LDRDepResolverEvent_Wait:
		server := t.servers[ev.TargetApplierID]
		for _, txn := range e.Wait.WaitTxns {
			resolved, resolvedTime := server.MaybeAddWaiter(txn, e.Wait.WaitingID)
			if resolved {
				if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
					TxnID:           txn,
					ResolvedTime:    resolvedTime,
					TargetApplierID: e.Wait.WaitingID,
				}); err != nil {
					return err
				}
			}
		}
	case *txnpb.LDRDepResolverEvent_WaitHorizon:
		server := t.servers[ev.TargetApplierID]
		resolved, resolvedTime := server.WaitHorizon(
			e.WaitHorizon.WaitingID, e.WaitHorizon.TxnHorizon,
		)
		if resolved {
			if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
				TxnID:           ldrdecoder.TxnID{ApplierID: e.WaitHorizon.DependID},
				ResolvedTime:    resolvedTime,
				TargetApplierID: e.WaitHorizon.WaitingID,
			}); err != nil {
				return err
			}
		}
	case *txnpb.LDRDepResolverEvent_Ready:
		server := t.servers[ev.TargetApplierID]
		updates := server.Ready(e.Ready.ReadyTxn, e.Ready.ResolvedTime)
		for _, update := range updates {
			if err := sendToLoopback(ev.TargetApplierID, update); err != nil {
				return err
			}
		}
	case *txnpb.LDRDepResolverEvent_ForwardUpdate:
		if err := sendToLoopback(ev.TargetApplierID, DependencyUpdate{
			TxnID:           e.ForwardUpdate.TxnID,
			ResolvedTime:    e.ForwardUpdate.ResolvedTime,
			TargetApplierID: ev.TargetApplierID,
		}); err != nil {
			return err
		}
	default:
		return errors.AssertionFailedf("unknown dep resolver event type: %T",
			ev.Event.Event)
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
func (t *testTrackerClient) Receive(applier ldrdecoder.ApplierID) *nudge.Buffer[DependencyUpdate] {
	return t.clients[applier].Receive(applier)
}
