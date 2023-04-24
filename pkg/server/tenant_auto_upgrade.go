// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// startAutoUpgradeLoop checks for changes in storage cluster version
// every 10 seconds and triggers an upgrade attempt if needed. Other than
// that, it also starts an upgrade attempt 10 seconds after a new sql server
// starts. This is to cover cases where upgrade becomes possible due to
// an upgrade to the tenant binary version.
func (s *SQLServer) startAutoUpgradeLoop(ctx context.Context) error {
	storageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version
	return s.stopper.RunAsyncTask(ctx, "tenant-auto-upgrade-checker", func(ctx context.Context) {
		firstAttempt := true
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
				// the storage cluster version changed. First case ensures that if an upgrade is
				// possible due to a change in a sql instance binary version, it happens. Second
				// cases ensures that if an upgrade is possible due to a change in the storage
				// cluster version, it happens.
				if storageClusterVersion != latestStorageClusterVersion || firstAttempt {
					firstAttempt = false
					storageClusterVersion = latestStorageClusterVersion
					if err := s.startAttemptUpgrade(ctx); err != nil {
						log.Errorf(ctx, "failed to start an upgrade attempt: %v", err)
					}
				}
			}
		}
	})
}

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *SQLServer) startAttemptUpgrade(ctx context.Context) error {
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

	var tenantClusterVersion clusterversion.ClusterVersion
	if err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		tenantClusterVersion, err = s.settingsWatcher.GetClusterVersionFromStorage(ctx, txn)
		return err
	}); err != nil {
		return errors.Wrap(err, "unable to retrieve tenant cluster version")
	}

	// Check if we should upgrade cluster version.
	status, upgradeToVersion, err := s.upgradeStatus(ctx, tenantClusterVersion.Version)
	switch status {
	case upgradeBlockedDueToError:
		return err
	case upgradeDisabledByConfiguration:
		log.Infof(ctx, "auto upgrade is disabled for current version (preserve_downgrade_option): %s", redact.Safe(tenantClusterVersion.Version))
		return nil
	case upgradeAlreadyCompleted:
		log.Info(ctx, "no need to upgrade, instance already at the newest version")
		return nil
	case upgradeBlockedDueToLowStorageClusterVersion:
		log.Info(ctx, "upgrade blocked because storage binary version doesn't support upgrading to minimum tenant binary version")
		return nil
	case upgradeAllowed:
		// Fall out of the select below.
	default:
		return errors.Newf("unhandled case: %d", status)
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

// upgradeStatus lets the main checking loop know if we should upgrade.
func (s *SQLServer) upgradeStatus(
	ctx context.Context, currentClusterVersion roachpb.Version,
) (st upgradeStatus, upgradeToVersion roachpb.Version, err error) {
	storageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version

	// Check if auto upgrade is enabled at current version.
	row, err := s.internalExecutor.QueryRowEx(
		ctx, "read-downgrade", nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		"SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';",
	)
	if err != nil {
		return upgradeBlockedDueToError, roachpb.Version{}, err
	}

	if row != nil {
		downgradeVersion, err := roachpb.ParseVersion(string(tree.MustBeDString(row[0])))
		if err != nil {
			return upgradeBlockedDueToError, roachpb.Version{}, err
		}
		if currentClusterVersion == downgradeVersion {
			return upgradeDisabledByConfiguration, roachpb.Version{}, nil
		}
	}

	instances, err := s.sqlInstanceReader.GetAllInstances(ctx)
	if err != nil {
		return upgradeBlockedDueToError, roachpb.Version{}, err
	}
	if len(instances) == 0 {
		return upgradeBlockedDueToError, roachpb.Version{}, errors.Errorf("no live instances found")
	}

	findMinBinaryVersion := func(instances []sqlinstance.InstanceInfo) roachpb.Version {
		minVersion := instances[0].BinaryVersion
		for _, instance := range instances {
			if instance.BinaryVersion.Less(minVersion) {
				minVersion = instance.BinaryVersion
			}
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
		return upgradeAlreadyCompleted, roachpb.Version{}, nil
	} else if storageClusterVersion.Less(minInstanceBinaryVersion) || storageClusterVersion == minInstanceBinaryVersion {
		// minInstanceBinaryVersion supports storageClusterVersion so upgrade to storageClusterVersion.
		upgradeToVersion = storageClusterVersion
	} else {
		// minInstanceBinaryVersion doesn't support storageClusterVersion but we can upgrade
		// cluster version to minInstanceBinaryVersion.
		upgradeToVersion = minInstanceBinaryVersion
	}

	if storageClusterVersion.Less(upgradeToVersion) {
		return upgradeBlockedDueToLowStorageClusterVersion, roachpb.Version{}, nil
	}
	return upgradeAllowed, upgradeToVersion, nil
}
