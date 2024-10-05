// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// startTenantAutoUpgradeLoop checks for changes in storage cluster version
// every 10 seconds and triggers an upgrade attempt if needed. Other than
// that, it also starts an upgrade attempt 10 seconds after a new sql server
// starts. This is to cover cases where upgrade becomes possible due to
// an upgrade to the tenant binary version.
func (s *SQLServer) startTenantAutoUpgradeLoop(ctx context.Context) error {
	storageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version
	return s.stopper.RunAsyncTask(ctx, "tenant-auto-upgrade-checker", func(ctx context.Context) {
		firstAttempt := true
		var allowUpgradeOnInternalVersionChanges bool
		if k := s.cfg.TestingKnobs.Server; k != nil {
			allowUpgradeOnInternalVersionChanges = k.(*TestingKnobs).AllowTenantAutoUpgradeOnInternalVersionChanges
		}
		for {
			select {
			case <-s.stopper.ShouldQuiesce():
				return
			// Check for changes every 10 seconds to avoid triggering an upgrade
			// on every change to the internal version of storage cluster version
			// within a short time period.
			case <-time.After(time.Second * 10):
				latestStorageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version
				// Only run upgrade if this is the first attempt (i.e. on server startup) or if the
				// the storage cluster version changed and is at an Internal version of 0 which implies that
				// that storage is at the "final" version for some release. First case ensures that if an upgrade is
				// possible due to a change in a sql instance binary version, it happens. Second
				// cases ensures that if an upgrade is possible due to a change in the storage
				// cluster version, it happens.
				// We may run an attempt when the change is only to the Internal version if a testing knob
				// is passed.
				storageClusterVersionChanged := storageClusterVersion != latestStorageClusterVersion
				if firstAttempt ||
					(storageClusterVersionChanged && (storageClusterVersion.Internal == 0 || allowUpgradeOnInternalVersionChanges)) {
					firstAttempt = false
					storageClusterVersion = latestStorageClusterVersion
					if err := s.startAttemptTenantUpgrade(ctx, allowUpgradeOnInternalVersionChanges); err != nil {
						log.Errorf(ctx, "failed to start an upgrade attempt: %v", err)
					}
				}
			}
		}
	})
}

// startAttemptTenantUpgrade attempts to upgrade cluster version.
func (s *SQLServer) startAttemptTenantUpgrade(
	ctx context.Context, allowUpgradeOnInternalVersionChanges bool,
) error {
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	// Check if auto upgrade is disabled for test purposes.
	if k := s.cfg.TestingKnobs.Server; k != nil {
		upgradeTestingKnobs := k.(*TestingKnobs)
		if disableCh := upgradeTestingKnobs.DisableAutomaticVersionUpgrade; disableCh != nil {
			log.Infof(ctx, "auto upgrade disabled by testing")
			select {
			case <-disableCh:
				log.Infof(ctx, "auto upgrade no longer disabled by testing")
			case <-s.stopper.ShouldQuiesce():
				return nil
			}
		}
	}

	var tenantAutoUpgradeInfoCh chan struct {
		Status    int
		UpgradeTo roachpb.Version
	}
	// Get testing knobs if set.
	if k := s.cfg.TestingKnobs.Server; k != nil {
		upgradeTestingKnobs := k.(*TestingKnobs)
		tenantAutoUpgradeInfoCh = upgradeTestingKnobs.TenantAutoUpgradeInfo
	}

	var tenantClusterVersion clusterversion.ClusterVersion
	if err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		tenantClusterVersion, err = s.settingsWatcher.GetClusterVersionFromStorage(ctx, txn)
		return err
	}); err != nil {
		return errors.Wrap(err, "unable to retrieve tenant cluster version")
	}

	// Check if we should upgrade cluster version.
	status, upgradeToVersion, err := s.tenantUpgradeStatus(ctx, tenantClusterVersion.Version, allowUpgradeOnInternalVersionChanges)

	// Let test code know the status of an upgrade if needed.
	if tenantAutoUpgradeInfoCh != nil {
		tenantAutoUpgradeInfoCh <- struct {
			Status    int
			UpgradeTo roachpb.Version
		}{int(status), upgradeToVersion}
	}

	switch status {
	case UpgradeBlockedDueToError:
		return err
	case UpgradeDisabledByConfiguration:
		log.Infof(ctx, "auto upgrade is disabled for current version (preserve_downgrade_option): %s", redact.Safe(tenantClusterVersion.Version))
		return nil
	case UpgradeAlreadyCompleted:
		log.Info(ctx, "no need to upgrade, instance already at the newest version")
		return nil
	case UpgradeBlockedDueToLowStorageClusterVersion:
		log.Info(ctx, "upgrade blocked because storage binary version doesn't support upgrading to minimum tenant binary version")
		return nil
	case UpgradeAllowed:
		// Fall out of the select below.
	default:
		return errors.AssertionFailedf("unhandled case: %d", status)
	}

	upgradeRetryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2,
		Closer:         s.stopper.ShouldQuiesce(),
	}

	// Run the set cluster setting version statement in a transaction
	// until success.
	for ur := retry.StartWithCtx(ctx, upgradeRetryOpts); ur.Next(); {
		if _, err := s.internalExecutor.ExecEx(
			ctx, "set-version", nil, /* txn */
			sessiondata.RootUserSessionDataOverride,
			"SET CLUSTER SETTING version = $1;", upgradeToVersion.String(),
		); err != nil {
			return errors.Wrap(err, "error when finalizing tenant cluster version upgrade")
		} else {
			log.Infof(ctx, "successfully upgraded tenant cluster version to %v", upgradeToVersion)
			return nil
		}
	}
	return nil
}

// tenantUpgradeStatus lets the main checking loop know if we should upgrade.
func (s *SQLServer) tenantUpgradeStatus(
	ctx context.Context,
	currentClusterVersion roachpb.Version,
	allowUpgradeOnInternalVersionChanges bool,
) (st upgradeStatus, upgradeToVersion roachpb.Version, err error) {
	storageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version

	if autoUpgradeEnabled := s.settingsWatcher.GetAutoUpgradeEnabledSettingValue(); !autoUpgradeEnabled {
		// Automatic upgrade is not enabled.
		return UpgradeDisabledByConfiguration, roachpb.Version{}, nil
	}

	instances, err := s.sqlInstanceReader.GetAllInstances(ctx)
	if err != nil {
		return UpgradeBlockedDueToError, roachpb.Version{}, err
	}
	if len(instances) == 0 {
		return UpgradeBlockedDueToError, roachpb.Version{}, errors.Errorf("no live instances found")
	}
	log.Infof(ctx, "found %d instances", len(instances))

	findMinBinaryVersion := func(instances []sqlinstance.InstanceInfo) roachpb.Version {
		minVersion := instances[0].BinaryVersion
		for _, instance := range instances {
			if instance.BinaryVersion.Less(minVersion) {
				minVersion = instance.BinaryVersion
			}
		}
		if !allowUpgradeOnInternalVersionChanges {
			// Unless a testing knob was passed, we are only interested in major and minor versions, not Internal ones.
			minVersion.Internal = 0
		}
		return minVersion
	}

	// For all cases below, return upgradeBlockedDueToLowStorageClusterVersion and
	// do not upgrade if storage logical version is less than the upgradeTo version.
	//
	// Upgrade Rules:
	// 1. Upgrade completed if `Tenant Logical Version == min(instancesBinaryVersions...)`
	// 2. Upgrade to Storage Logical Version (SLV) if min(instancesBinaryVersions...) supports upgrading to SLV
	// 3. Upgrade to min(instancesBinaryVersions...)

	minInstanceBinaryVersion := findMinBinaryVersion(instances)
	if currentClusterVersion == minInstanceBinaryVersion {
		return UpgradeAlreadyCompleted, roachpb.Version{}, nil
	} else if storageClusterVersion.LessEq(minInstanceBinaryVersion) {
		// minInstanceBinaryVersion supports storageClusterVersion so upgrade to storageClusterVersion.
		upgradeToVersion = storageClusterVersion
	} else {
		// minInstanceBinaryVersion doesn't support storageClusterVersion but we can upgrade
		// cluster version to minInstanceBinaryVersion.
		upgradeToVersion = minInstanceBinaryVersion
	}

	if storageClusterVersion.Less(upgradeToVersion) {
		return UpgradeBlockedDueToLowStorageClusterVersion, roachpb.Version{}, nil
	}
	return UpgradeAllowed, upgradeToVersion, nil
}
