package settingswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
)

// VersionGuard is a utility for checking the cluster version in a transaction.
// VersionGuard is optimized to avoid the extra kv read overhead once the
// cluster is finalized.
//
// Example Usage:
// guard, err := watcher.MakeVersionGuard(ctx, txn, version.MaxVersionGateToCheck)
//
//	if err != nil {
//	   return err // unable to read version
//	}
//
//	if interlock.IsActive(version.SomeVersionLessThanMax) {
//	  ...
//	} else if interlock.IsActive(version.MaxVersionGateToCheck) {
//
//	  ...
//	}
type VersionGuard struct {
	maxGateIsActive bool
	txnVersion      clusterversion.ClusterVersion
}

// MakVersionGuard constructs a version guard for the transaction.
func (s *SettingsWatcher) MakeVersionGuard(
	ctx context.Context, txn *kv.Txn, maxGate clusterversion.Key,
) (VersionGuard, error) {
	if s.settings.Version.IsActive(ctx, maxGate) {
		return VersionGuard{
			maxGateIsActive: true,
		}, nil
	}
	txnVersion, err := s.GetClusterVersionFromStorage(ctx, txn)
	return VersionGuard{
		txnVersion: txnVersion,
	}, err
}

// IsActive returns true if the transaction should treat the version guard as
// active.
func (v *VersionGuard) IsActive(version clusterversion.Key) bool {
	if v.maxGateIsActive {
		return true
	}
	return v.txnVersion.IsActive(version)
}
