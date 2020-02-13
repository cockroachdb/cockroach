// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nodeupgrade provides a mechanism for running an idempotent closure on
// each engine in the cluster during upgrades. This is written as a general
// mechanism, but intended to be used within clusterversion migrations.
//
// TODO(otan): write final protocol as we sort out the startup story.
package nodeupgrade

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

// UpgradeFn is an idempotent function that performs an upgrade on
// a particular node.
type UpgradeFn func(context.Context, engine.Engine) error

// registry contains
type upgradeRegistry struct {
	mu struct {
		syncutil.RWMutex
		versionToUpgradeFn map[Version]UpgradeFn
	}
}

var registry upgradeRegistry

// Register is called in an init func to add a upgrade implementation.
func Register(v Version, fn UpgradeFn) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.mu.versionToUpgradeFn == nil {
		registry.mu.versionToUpgradeFn = make(map[Version]UpgradeFn)
	}
	if _, ok := registry.mu.versionToUpgradeFn[v]; ok {
		panic(errors.AssertionFailedf(`multiple hooks registerd for key: %s`, v))
	}
	registry.mu.versionToUpgradeFn[v] = fn
}

// Temporary variable until we use register.
// TODO(otan): remove.
var _ = Register

// engineStatus contains the current and pending versions
// from the engine local storage.
type engineStatus struct {
	currentValue   *roachpb.Value
	currentVersion Version
	pendingValue   *roachpb.Value
	pendingVersion Version
}

// setTargetVersionOnEngine modifies the engine to have a new pending version
// to be the same as the target version. If the pending version is greater than
// the target, it will be a no-op.
func setTargetVersionOnEngine(ctx context.Context, eng engine.Engine, target Version) error {
	status, err := getStatusFromEngine(ctx, eng)
	if err != nil {
		return err
	}

	// Defensive check against bad state.
	// Note: other nodes may still be able to progress, as this is called
	// from a goroutine.
	if status.currentVersion > status.pendingVersion {
		return errors.AssertionFailedf(
			"current version %s > pending version %s",
			status.currentVersion,
			status.pendingVersion,
		)
	}

	if status.pendingVersion > target {
		// We're already at a higher pending version, continue.
		return nil
	}

	// We save updates to the current version until the end.
	// Since hooks are idempotent, this is ok.
	val := status.pendingValue
	val.ClearChecksum()
	val.SetInt(int64(target))
	return engine.MVCCPut(
		ctx,
		eng,
		nil, /* stats */
		keys.StoreNodeUpgradePendingVersionKey(),
		hlc.Timestamp{},
		*val,
		nil, /* txn */
	)
}

// getStatusResponseFromEngine returns the entry from the local engine pertaining
// to the status of of the upgrade.
func getStatusFromEngine(ctx context.Context, eng engine.Engine) (engineStatus, error) {
	currentVal, _, err := engine.MVCCGet(
		ctx,
		eng,
		keys.StoreNodeUpgradeCurrentVersionKey(),
		hlc.Timestamp{},
		engine.MVCCGetOptions{},
	)
	if err != nil {
		return engineStatus{}, err
	}
	currentVersion, err := currentVal.GetInt()
	if err != nil {
		return engineStatus{}, err
	}
	pendingVal, _, err := engine.MVCCGet(
		ctx,
		eng,
		keys.StoreNodeUpgradePendingVersionKey(),
		currentVal.Timestamp,
		engine.MVCCGetOptions{},
	)
	if err != nil {
		return engineStatus{}, err
	}
	pendingVersion, err := pendingVal.GetInt()
	if err != nil {
		return engineStatus{}, err
	}
	return engineStatus{
		currentValue:   currentVal,
		currentVersion: Version(currentVersion),
		pendingValue:   pendingVal,
		pendingVersion: Version(pendingVersion),
	}, err
}

// runPendingUpgradesOnEngine executes every upgrade up to and including the requested one
// on the given engine.
func runPendingUpgradesOnEngine(
	ctx context.Context, eng engine.Engine, registry *upgradeRegistry,
) error {
	status, err := getStatusFromEngine(ctx, eng)
	if err != nil {
		return err
	}

	// Defensive check against bad state before running an upgrade.
	if status.currentVersion > status.pendingVersion {
		return errors.AssertionFailedf(
			"current version %s > pending version %s",
			status.currentVersion,
			status.pendingVersion,
		)
	}

	if status.pendingVersion == status.currentVersion {
		// We're already done; skip any required upgrades.
		return nil
	}

	// Perform all upgrades in order.
	for current := status.currentVersion + 1; current <= status.pendingVersion; current++ {
		registry.mu.RLock()
		upgradeFn, ok := registry.mu.versionToUpgradeFn[current]
		registry.mu.RUnlock()
		if !ok {
			return errors.Errorf(`could not find upgrade for key: %s`, current)
		}
		if err := upgradeFn(ctx, eng); err != nil {
			return errors.Wrapf(err, `running upgrade: %s`, current)
		}
	}

	// We save updates to the current version until the end.
	// Since hooks are idempotent, this is ok.
	val := status.currentValue
	val.ClearChecksum()
	val.SetInt(int64(status.pendingVersion))
	return engine.MVCCPut(
		ctx,
		eng,
		nil, /* stats */
		keys.StoreNodeUpgradeCurrentVersionKey(),
		hlc.Timestamp{},
		*val,
		nil, /* txn */
	)
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
	engs     []engine.Engine
	registry *upgradeRegistry
}

var _ NodeUpgraderServer = &nodeUpgrader{}

// SetTargetVersion is the implementation of the NodeUpgraderServer interface.
func (n *nodeUpgrader) SetTargetVersion(
	ctx context.Context, req *SetTargetVersionRequest,
) (*SetTargetVersionResponse, error) {
	if err := runOnEngines(ctx, n.engs, func(ctx context.Context, eng engine.Engine) error {
		return setTargetVersionOnEngine(ctx, eng, req.TargetVersion)
	}); err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &SetTargetVersionResponse{Err: &resErr}, nil
	}
	return &SetTargetVersionResponse{}, nil
}

// RunPendingUpgrades is the implementation of the NodeUpgraderServer interface.
func (n *nodeUpgrader) RunPendingUpgrades(
	ctx context.Context, req *RunPendingUpgradesRequest,
) (*RunPendingUpgradesResponse, error) {
	if err := runOnEngines(ctx, n.engs, func(ctx context.Context, eng engine.Engine) error {
		return runPendingUpgradesOnEngine(ctx, eng, n.registry)
	}); err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &RunPendingUpgradesResponse{Err: &resErr}, nil
	}
	return &RunPendingUpgradesResponse{}, nil
}

// Status is the implementation of the NodeUpgraderServer interface.
func (n *nodeUpgrader) Status(ctx context.Context, req *StatusRequest) (*StatusResponse, error) {
	resp := &StatusResponse{
		Engines: make([]*StatusResponse_Engine, len(n.engs)),
	}
	for i, eng := range n.engs {
		status, err := getStatusFromEngine(ctx, eng)
		if err != nil {
			resErr := errors.EncodeError(ctx, err)
			return &StatusResponse{Err: &resErr}, nil
		}
		resp.Engines[i] = &StatusResponse_Engine{
			CurrentVersion: status.currentVersion,
			PendingVersion: status.pendingVersion,
		}
	}
	return resp, nil
}
