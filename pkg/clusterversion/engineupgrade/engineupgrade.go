// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package engineupgrade provides a mechanism for running an idempotent closure on
// each engine in the cluster during upgrades. This is written as a general
// mechanism, but intended to be used within clusterversion migrations.
//
// Each upgrade should register themselves to a version using the `Upgrade` function.
// TODO(#long-running-migrations): document mitigation for import weirdness.
//
// Each engine has an upgrade "EngineState", with two fields:
// * CurrentVersion: this means all upgrades <= a given version have been applied.
// * TargetVersion: this is the version this node should aim to hit.
// The invariant here is that CurrentVersion <= TargetVersion.
//
// On startup, the `OnStartup` function is expected to be called.
// It will attempt to upgrade all engines to the TargetVersion.
// This means nodes will guarantee to start up with CurrentVersion = TargetVersion
// unless it is a brand new node (in which case, no upgrade should be necessary).
//
// To make sure engines are not behind, when a node joins a cluster, the Connect
// RPC will return an error if any nodes in the cluster have a higher TargetVersion
// than what is available locally. In that case, we upgrade the nodes to the
// TargetVersion accordingly and try reconnect.
//
// The main entrypoint for callers is expected to be `UpgradeCluster`, which
// propagates the new TargetVersion to all nodes that can be seen.
// It will only be complete when all available nodes have the desired TargetVersion,
// meaning any subsequent attempts to connect must also have TargetVersion set
// on the node's local engines.
package engineupgrade

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Fn is a function that performs an upgrade on a particular engine,
// corresponding to a particular version.
// These upgrades must be idempotent as they may be run multiple times.
// Only one Fn can operate on a given engine at any point in time,
// with the guarantee that previous versions have already run.
// TODO(#long-running-migrations): consider introducing a progress function.
type Fn func(context.Context, engine.Engine) error

// fnRegistry contains the mapping for versions to the
// function that runs to upgrade to that version.
// TODO(#long-running-migrations): use cockroach_versions instead.
type fnRegistry struct {
	mu struct {
		syncutil.RWMutex
		versionToFn map[Version]Fn
	}
}

// registry is the global registry. Use Register to add to the registry.
var defaultFnRegistry fnRegistry

// Register is called in an init func to add a upgrade implementation.
func Register(v Version, fn Fn) {
	defaultFnRegistry.mu.Lock()
	defer defaultFnRegistry.mu.Unlock()
	if defaultFnRegistry.mu.versionToFn == nil {
		defaultFnRegistry.mu.versionToFn = make(map[Version]Fn)
	}
	if _, ok := defaultFnRegistry.mu.versionToFn[v]; ok {
		panic(errors.AssertionFailedf(`multiple functions registered for version %s`, v))
	}
	defaultFnRegistry.mu.versionToFn[v] = fn
}

// Temporary variable until we use register.
// TODO(#long-running-migrations): remove when we use this.
var _ = Register

// UpgradeCluster calls every node to initialize an upgrade on every engine.
// This returns only after each engine in the cluster has successfully upgraded
// to the desired version, which prevents new nodes from starting up with
// a newer version.
func UpgradeCluster(ctx context.Context) error {
	// TODO(#long-running-migrations): implement this.

	// First, we RPC to every node on gossip for upgrade, and block until this is complete.

	// Next, we fetch gossip again, and double check each node has the desired target version.
	// This is to prevent a race where new nodes join the cluster whilst TargetVersion
	// may be switching. If any node does not have the desired TargetVersion, repeat
	// the first step.

	// Once we return here.
	return nil
}

// Temporary variable until we use it.
// TODO(#long-running-migrations): remove when we use this.
var _ = UpgradeCluster

// OnStartup is the hook called on node startup, but before connecting to the cluster.
// It checks that each engine on the given node is upgraded to the highest target version,
// upgrading engine if necessary.
// If the engine has never been initialized, it will initialize the state with zero values.
// It will be up to gossip.Connext to inform the engine to what to upgrade to later.
func OnStartup(ctx context.Context, engs []engine.Engine) error {
	// TODO(#long-running-migrations): implement this.
	return nil
}

// Temporary variable until we use it.
// TODO(#long-running-migrations): remove when we use this.
var _ = OnStartup

// getEngineState returns the entry from the local engine pertaining
// to the status of of the upgrade.
func getEngineState(ctx context.Context, eng engine.Engine) (*EngineState, error) {
	val, _, err := engine.MVCCGet(
		ctx,
		eng,
		keys.StoreUpgradeEngineStateKey(),
		hlc.Timestamp{},
		engine.MVCCGetOptions{},
	)
	if err != nil || val == nil {
		return nil, err
	}
	var ret EngineState
	err = val.GetProto(&ret)
	return &ret, err
}

// setEngineState sets the engine to the given state.
func setEngineState(ctx context.Context, eng engine.Engine, state *EngineState) error {
	var val roachpb.Value
	err := val.SetProto(state)
	if err != nil {
		return err
	}
	return engine.MVCCPut(
		ctx,
		eng,
		nil, /* stats */
		keys.StoreUpgradeEngineStateKey(),
		hlc.Timestamp{},
		val,
		nil, /* txn */
	)
}

// upgradeEngineToTarget executes every upgrade up to and including the requested one
// on the given engine.
func upgradeEngineToTarget(
	ctx context.Context, eng engine.Engine, registry *fnRegistry, targetVersion Version,
) error {
	state, err := getEngineState(ctx, eng)
	if err != nil {
		return err
	}

	// If the state does not exist, assume we've never been upgraded before.
	// As such, "do the dumb thing" and apply all upgrades.
	if state == nil {
		state = &EngineState{TargetVersion: targetVersion}
	}

	// Defensive check against bad state before running an upgrade.
	if state.CurrentVersion > state.TargetVersion {
		return errors.AssertionFailedf(
			"current version %s > target version %s",
			state.CurrentVersion,
			state.TargetVersion,
		)
	}

	// Defensive check against downgrades.
	if state.TargetVersion > targetVersion {
		return errors.AssertionFailedf(
			"attempted downgrade: target version %s > target version %s",
			state.TargetVersion,
			targetVersion,
		)
	}

	if targetVersion == state.CurrentVersion {
		// We're already done; don't bother with the rest.
		return nil
	}

	// Set desired engine target, and persist. From this point onwards, the engine
	// should connect to the cluster only after this version is hit.
	if targetVersion > state.TargetVersion {
		state.TargetVersion = targetVersion
		if err := setEngineState(ctx, eng, state); err != nil {
			return err
		}
	}

	// Perform all upgrades in order.
	for current := state.CurrentVersion + 1; current <= state.TargetVersion; current++ {
		registry.mu.RLock()
		upgradeFn, ok := registry.mu.versionToFn[current]
		registry.mu.RUnlock()
		// TODO(#long-running-migrations): if this is not required at a later stage
		// (if we decide to use cockroach_versions.go), we should not error here.
		// However, if an upgrade may be missing to import weirdness, we should
		// introduce an "expected upgrades" mapping and error out if it
		// is missing.
		if !ok {
			return errors.Errorf(`could not find upgrade for key: %s`, current)
		}
		if err := upgradeFn(ctx, eng); err != nil {
			return errors.Wrapf(err, `running upgrade: %s`, current)
		}

		// Update the version as we go.
		state.CurrentVersion = current
		if err := setEngineState(ctx, eng, state); err != nil {
			return err
		}
	}

	return nil
}

// runOnEngines runs the given closure on all the engines.
func runOnEngines(
	ctx context.Context, engs []engine.Engine, fn func(context.Context, engine.Engine) error,
) error {
	g := ctxgroup.WithContext(ctx)
	for i := range engs {
		eng := engs[i]
		g.GoCtx(func(ctx context.Context) error {
			return fn(ctx, eng)
		})
	}
	return g.Wait()
}

// nodeUpgrader is an implementation of NodeUpgraderServer.
type nodeUpgrader struct {
	engs         []engine.Engine
	fnRegistry   *fnRegistry
	upgradeMutex syncutil.Mutex
}

var _ NodeUpgraderServer = &nodeUpgrader{}

// UpgradeEngines is the default implementation of NodeUpgraderServer.
func (n *nodeUpgrader) UpgradeEngines(
	ctx context.Context, req *UpgradeEnginesRequest,
) (*UpgradeEnginesResponse, error) {
	// Only allow one upgrade to happen at any one point.
	n.upgradeMutex.Lock()
	defer n.upgradeMutex.Unlock()

	if err := runOnEngines(ctx, n.engs, func(ctx context.Context, eng engine.Engine) error {
		return upgradeEngineToTarget(ctx, eng, n.fnRegistry, req.TargetVersion)
	}); err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &UpgradeEnginesResponse{Err: &resErr}, nil
	}
	return &UpgradeEnginesResponse{}, nil
}

// Status is the implementation of the NodeUpgraderServer interface.
func (n *nodeUpgrader) Status(ctx context.Context, req *StatusRequest) (*StatusResponse, error) {
	resp := &StatusResponse{
		EngineStates: make([]*EngineState, len(n.engs)),
	}
	for i, eng := range n.engs {
		state, err := getEngineState(ctx, eng)
		if err != nil {
			return nil, err
		}
		resp.EngineStates[i] = state
	}
	return resp, nil
}

// NewNodeUpgraderServer returns the default NodeUpgraderServer
// implementation.
func NewNodeUpgraderServer(engs []engine.Engine) NodeUpgraderServer {
	return &nodeUpgrader{engs: engs, fnRegistry: &defaultFnRegistry}
}

// TODO(#long-running-migrations): when this is used, remove this.
var _ = NewNodeUpgraderServer
