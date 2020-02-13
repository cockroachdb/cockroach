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
// TODO(otan): write final protocol as we sort out the startup story.
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

// Fn is an idempotent function that performs an upgrade on
// a particular engine.
type Fn func(context.Context, engine.Engine) error

// upgradeRegistry contains the mapping for versions to the
// function that runs to upgrade to that version.
type upgradeRegistry struct {
	mu struct {
		syncutil.RWMutex
		versionToFn map[Version]Fn
	}
}

// registry is the global registry. Use Register to add to the registry.
var registry upgradeRegistry

// Register is called in an init func to add a upgrade implementation.
func Register(v Version, fn Fn) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.mu.versionToFn == nil {
		registry.mu.versionToFn = make(map[Version]Fn)
	}
	if _, ok := registry.mu.versionToFn[v]; ok {
		panic(errors.AssertionFailedf(`multiple functions registered for version %s`, v))
	}
	registry.mu.versionToFn[v] = fn
}

// Temporary variable until we use register.
// TODO(otan): remove.
var _ = Register

// engineStatus contains the current and pending versions
// from the engine local storage.
type engineStatus struct {
	currentVersion Version
	pendingVersion Version
}

// getStatusResponseFromEngine returns the entry from the local engine pertaining
// to the status of of the upgrade.
func getStatusFromEngine(ctx context.Context, eng engine.Engine) (engineStatus, error) {
	currentVal, _, err := engine.MVCCGet(
		ctx,
		eng,
		keys.StoreCurrentVersionKey(),
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
		keys.StorePendingVersionKey(),
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
		currentVersion: Version(currentVersion),
		pendingVersion: Version(pendingVersion),
	}, err
}

// runPendingUpgradesOnEngine executes every upgrade up to and including the requested one
// on the given engine.
func runPendingUpgradesOnEngine(
	ctx context.Context, eng engine.Engine, registry *upgradeRegistry, targetVersion Version,
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

	if targetVersion == status.currentVersion {
		// We're already done; skip any required upgrades.
		return nil
	}

	// We set ourselves to be even higher then the current pendingVersion.
	if targetVersion > status.pendingVersion {
		var val roachpb.Value
		val.SetInt(int64(targetVersion))
		if err := engine.MVCCPut(
			ctx,
			eng,
			nil, /* stats */
			keys.StorePendingVersionKey(),
			hlc.Timestamp{},
			val,
			nil, /* txn */
		); err != nil {
			return err
		}
	}

	// Perform all upgrades in order.
	for current := status.currentVersion + 1; current <= targetVersion; current++ {
		registry.mu.RLock()
		upgradeFn, ok := registry.mu.versionToFn[current]
		registry.mu.RUnlock()
		if !ok {
			return errors.Errorf(`could not find upgrade for key: %s`, current)
		}
		if err := upgradeFn(ctx, eng); err != nil {
			return errors.Wrapf(err, `running upgrade: %s`, current)
		}

		// Update the version as we go.
		var val roachpb.Value
		val.SetInt(int64(current))
		if err := engine.MVCCPut(
			ctx,
			eng,
			nil, /* stats */
			keys.StoreCurrentVersionKey(),
			hlc.Timestamp{},
			val,
			nil, /* txn */
		); err != nil {
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
	engs     []engine.Engine
	registry *upgradeRegistry
}

var _ NodeUpgraderServer = &nodeUpgrader{}

// RunUpgrades is the implementation of the NodeUpgraderServer interface.
func (n *nodeUpgrader) RunUpgrades(
	ctx context.Context, req *RunUpgradesRequest,
) (*RunUpgradesResponse, error) {
	if err := runOnEngines(ctx, n.engs, func(ctx context.Context, eng engine.Engine) error {
		return runPendingUpgradesOnEngine(ctx, eng, n.registry, req.TargetVersion)
	}); err != nil {
		resErr := errors.EncodeError(ctx, err)
		return &RunUpgradesResponse{Err: &resErr}, nil
	}
	return &RunUpgradesResponse{}, nil
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
