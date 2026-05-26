// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// VersionGuard is a utility for checking the cluster version in a transaction.
// VersionGuard is optimized to avoid the extra kv read overhead once the
// cluster is finalized.
//
// Example Usage:
//
//	guard, err := watcher.MakeVersionGuard(ctx, txn, version.MaxVersionGateToCheck)
//	if err != nil {
//	   return err // unable to read version
//	}
//	if guard.IsActive(version.SomeVersionLessThanMax) {
//	  ...
//	} else if guard.IsActive(version.MaxVersionGateToCheck) {
//	  ...
//	}
type VersionGuard struct {
	activeVersion clusterversion.ClusterVersion
}

// MakeVersionGuard constructs a version guard for the transaction.
func (s *SettingsWatcher) MakeVersionGuard(
	ctx context.Context, txn *kv.Txn, maxGate clusterversion.Key,
) (VersionGuard, error) {
	activeVersion := s.settings.Version.ActiveVersion(ctx)
	if activeVersion.IsActive(maxGate) {
		return VersionGuard{activeVersion: activeVersion}, nil
	}

	txnVersion, err := s.GetClusterVersionFromStorage(ctx, txn)
	if errors.Is(err, errVersionSettingNotFound) {
		// The version setting is set via the upgrade job. Since there are some
		// permanent upgrades that run unconditionally when a cluster is
		// created, the version setting is populated during the cluster boot
		// strap process.
		//
		// The case where a setting is old and the version is missing is
		// uncommon and mostly shows up during tests. Usually clusters are
		// bootstrapped at the binary version, so a new cluster will hit the
		// fast path of the version guard since the active version is the most
		// recent version gate.
		//
		// However, during testing the sql server may be bootstrapped at an old
		// cluster version and hits the slow path because the cluster version
		// is behind the maxGate version. In this case we treat the in-memory
		// version as the active setting.
		//
		// Using the in-memory version is safe from race conditions because the
		// transaction did read the missing value from the system.settings
		// table and will get retried if the setting changes.
		log.Ops.Warningf(ctx, "the 'version' setting was not found in the system.setting table using in-memory settings %v", activeVersion)
		return VersionGuard{
			activeVersion: activeVersion,
		}, nil
	}
	if err != nil {
		return VersionGuard{}, err
	}

	return VersionGuard{
		activeVersion: txnVersion,
	}, nil
}

// IsActive returns true if the transaction should treat the version guard as
// active.
func (v *VersionGuard) IsActive(version clusterversion.Key) bool {
	return v.activeVersion.IsActive(version)
}

// TestMakeVersionGuard initializes a version guard at specific version.
func TestMakeVersionGuard(activeVersion clusterversion.ClusterVersion) VersionGuard {
	return VersionGuard{activeVersion: activeVersion}
}
