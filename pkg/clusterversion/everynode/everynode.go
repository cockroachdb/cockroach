// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package everynode provides a mechanism for running an idempotent closure on
// each engine in the cluster during upgrades. This is written as a general
// mechanism but primarily intended for use in pkg/clusterversion/migration
// hooks.
//
// The primary external interface to this package is RunHookOnEveryNode. This
// method ensures that the requested hook (and all previous hooks) are run on
// the engines of every node in the cluster. In this context, "every node" means
// all nodes that have ever joined the cluster and haven't finished being
// decommissioned. This means that RunHookOnEveryNode will block if there is any
// node unavailibility, it is the responsibility of the cluster operator to
// ensure that all nodes are available during an upgrade.
//
// Before running the hooks on each node, this new low-water mark for required
// hooks is written to replicated kv. Then a synchronous RPC (`HookService`
// implemented in this package by `*Server`) is used to distribute the work.
// After an individual node runs the hook but before the RPC returns, the new
// hook high-water is persisted on each engine. When a node starts (is added,
// restarts, or is recommissioned), it looks up the required low-water. Then for
// each engine, the high-water is read and any necessary hooks are run before
// the node is allowed to join the cluster. (The per-engine high-water is an
// optimization, the hooks must be idempotent, so it would be correct but
// unecessary to run them every time a node started.)
package everynode

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// HookRunFn is an idempotent function.
type HookRunFn func(context.Context, engine.Engine) error

// RunHookOnEveryNode executes the requested hook closure on each node in the
// cluster and each node added to the cluster in the future. If no error is
// returned, the caller is guaranteed that no node will ever again connect to
// the cluster without first having run this hook.
//
// This is used during cluster version upgrades and requires that each node in
// the cluster be available. It blocks until each node has been contacted via
// RPC and returned a successful response.
//
// WIP It's currently "legal" for a node to be decommissioned and later
// re-added. If we wanted to prevent this, we could add a NodeID graveyard
// against which incoming Connect calls are checked. Probably better to instead
// have these node do the right thing on startup (get the current list of
// required hooks from Connect and run anything that hasn't been run).
func RunHookOnEveryNode(
	ctx context.Context,
	db *client.DB,
	gossip *gossip.Gossip,
	rpcCtx *rpc.Context,
	key HookKey,
	progressFn func(string),
) error {
	if err := updateClusterMinimum(ctx, db, key); err != nil {
		return err
	}

	// WIP: Get every non-decommissioned node. Can we do this transactionally with
	// the updateClusterMinimum call above?
	var nodeIDs []roachpb.NodeID

	var finishedAtomic int64
	g := ctxgroup.WithContext(ctx)
	for i := range nodeIDs {
		nodeID := nodeIDs[i]
		g.GoCtx(func(ctx context.Context) error {
			defer func() {
				finished := atomic.AddInt64(&finishedAtomic, 1)
				// WIP there should be some sort of rate limiting here
				progressFn(fmt.Sprintf(`finished hook on %d of %d nodes`, finished, len(nodeIDs)))
			}()

			// WIP: How should retries work?
			//
			// WIP: If something gets stuck, we need to be able to push the status
			// back up to the job so the user can see what's blocking the upgrade.
			address, err := gossip.GetNodeIDAddress(nodeID)
			if err != nil {
				return err
			}
			conn, err := rpcCtx.GRPCDialNode(address.String(), nodeID, rpc.SystemClass).Connect(ctx)
			if err != nil {
				return err
			}
			client := NewHookClient(conn)
			// WIP: What sort of cleanup is needed for conn and client?
			res, err := client.RunHook(ctx, &RunHookRequest{Key: key})
			if err != nil {
				return err
			}
			if res.Err != nil {
				return errors.DecodeError(ctx, *res.Err)
			}
			return nil
		})
	}
	return g.Wait()
}

func updateClusterMinimum(ctx context.Context, db *client.DB, key HookKey) error {
	err := db.Put(ctx, keys.EveryNodeRequiredMinimum, int(key))
	return errors.Wrapf(err, "persisting new cluster-level everynode required minimum: %s", key)
}

// RunHooksOnThisNode executes every hook up to and including the requested one.
func RunHooksOnThisNode(ctx context.Context, engs []engine.Engine, key HookKey) error {
	g := ctxgroup.WithContext(ctx)
	for i := range engs {
		eng := engs[i]
		g.GoCtx(func(ctx context.Context) error {
			return runHooksOnThisEngine(ctx, eng, key)
		})
	}
	return g.Wait()
}

func runHooksOnThisEngine(ctx context.Context, eng engine.Engine, target HookKey) error {
	finishedKey := keys.MakeStoreKey(keys.LocalStoreFinishedEveryNodeSuffix, nil)
	finishedVal, _, err := engine.MVCCGet(
		ctx, eng, finishedKey, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return err
	}
	finished, err := finishedVal.GetInt()
	if err != nil {
		return err
	}

	current := HookKey(finished)
	if current > target {
		// Fast path, skip writing our progress.
		return nil
	}
	for ; current < target; current++ {
		registry.mu.Lock()
		hookRunFn, ok := registry.mu.hooks[current]
		registry.mu.Unlock()
		if !ok {
			return errors.Errorf(`could not find hook for key: %s`, current)
		}
		if err := hookRunFn(ctx, eng); err != nil {
			return errors.Wrapf(err, `running hook: %s`, current)
		}
	}

	// Save the progress on this engine so we don't run the hooks needlessly.
	finishedVal.ClearChecksum()
	finishedVal.SetInt(int64(target))
	if err := engine.MVCCPut(
		ctx, eng, nil /* ms */, finishedKey, hlc.Timestamp{}, *finishedVal, nil, /* txn */
	); err != nil {
		return err
	}
	return nil
}

var registry struct {
	mu struct {
		syncutil.Mutex
		hooks map[HookKey]HookRunFn
	}
}

// Register is called in an init func to add a hook implementation for use with
// RunHookOnEveryNode.
func Register(key HookKey, fn HookRunFn) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.mu.hooks == nil {
		registry.mu.hooks = make(map[HookKey]HookRunFn)
	}
	if _, ok := registry.mu.hooks[key]; ok {
		panic(errors.AssertionFailedf(`multiple hooks registerd for key: %s`, key))
	}
	registry.mu.hooks[key] = fn
}

type Server struct {
	engs []engine.Engine
}

var _ HookServer = (*Server)(nil)

func NewServer(engs []engine.Engine) *Server {
	return &Server{engs: engs}
}

func (s *Server) RunHook(
	ctx context.Context, req *RunHookRequest,
) (*RunHookResponse, error) {
	err := RunHooksOnThisNode(ctx, s.engs, req.Key)
	if err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &RunHookResponse{Err: &resErr}, nil
	}
	return &RunHookResponse{}, nil
}
