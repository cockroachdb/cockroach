// Copyright 2018 The Cockroach Authors.
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

// startAutoUpgradeOnStorageUpgrade checks for changes in storage cluster version
// every 30 seconds and triggers an upgrade attempt if needed.
func (s *SQLServer) startAutoUpgradeOnStorageUpgrade(ctx context.Context) error {
	storageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version
	return s.stopper.RunAsyncTask(ctx, "tenant-auto-upgrade-checker", func(ctx context.Context) {
		for {
			select {
			case <-s.stopper.ShouldQuiesce():
				return
			// Check for changes every 30 seconds to avoid triggering an upgrade
			// on every change to the internal version of storage cluster version
			// within a short time period.
			case <-time.After(time.Second * 30):
				latestStorageClusterVersion := s.settingsWatcher.GetStorageClusterActiveVersion().Version
				if storageClusterVersion != latestStorageClusterVersion {
					storageClusterVersion = latestStorageClusterVersion
					if err := s.startAttemptUpgrade(ctx); err != nil {
						log.Errorf(ctx, "failed to start an upgrade attempt")
					}
				}
			}
		}
	})
}

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *SQLServer) startAttemptUpgrade(ctx context.Context) error {
	return s.stopper.RunAsyncTask(ctx, "tenant-auto-upgrade", func(ctx context.Context) {
		ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		retryOpts := retry.Options{
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			Multiplier:     2,
			Closer:         s.stopper.ShouldQuiesce(),
		}

		// Check if auto upgrade is disabled for test purposes.
		if k := s.cfg.TestingKnobs.Server; k != nil {
			upgradeTestingKnobs := k.(*TestingKnobs)
			if disableCh := upgradeTestingKnobs.DisableAutomaticVersionUpgrade; disableCh != nil {
				log.Infof(ctx, "auto upgrade disabled by testing")
				select {
				case <-disableCh:
					log.Infof(ctx, "auto upgrade no longer disabled by testing")
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		}

		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			var tenantClusterVersion clusterversion.ClusterVersion
			if err := s.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				tenantClusterVersion, err = s.settingsWatcher.GetClusterVersionFromStorage(ctx, txn)
				return err
			}); err != nil {
				log.Errorf(ctx, "unable to retrieve cluster version: %v", err)
				continue
			}

			// Check if we should upgrade cluster version, keep checking upgrade
			// status, or stop attempting upgrade.
			status, upgradeToVersion, err := s.upgradeStatus(ctx, tenantClusterVersion.Version)
			switch status {
			case upgradeBlockedDueToError:
				log.Errorf(ctx, "failed attempt to upgrade cluster version, error: %v", err)
				continue
			case upgradeBlockedDueToMixedVersions:
				log.Infof(ctx, "failed attempt to upgrade cluster version: %v", err)
				continue
			case upgradeDisabledByConfiguration:
				log.Infof(ctx, "auto upgrade is disabled for current version (preserve_downgrade_option): %s", redact.Safe(tenantClusterVersion.Version))
				// Note: we do 'continue' here (and not 'return') so that the
				// auto-upgrade gets a chance to continue/complete if the
				// operator resets `preserve_downgrade_option` after the node
				// has started up already.
				continue
			case upgradeAlreadyCompleted:
				log.Info(ctx, "no need to upgrade, instance already at the newest version")
				return
			case upgradeBlockedDueToLowBinaryVersion:
				log.Info(ctx, "upgrade blocked because tenant binary version doesn't support upgrading to storage cluster version")
			case upgradeAllowed:
				// Fall out of the select below.
			default:
				panic(errors.AssertionFailedf("unhandled case: %d", status))
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
					log.Errorf(ctx, "error when finalizing cluster version upgrade: %v", err)
				} else {
					log.Info(ctx, "successfully upgraded cluster version")
					return
				}
			}
		}
	})
}

// upgradeStatus lets the main checking loop know if we should do upgrade,
// keep checking upgrade status, or stop attempting upgrade.
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

	if storageClusterVersion == currentClusterVersion {
		return upgradeAlreadyCompleted, roachpb.Version{}, nil
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

	minInstanceBinaryVersion := findMinBinaryVersion(instances)
	if storageClusterVersion.Less(minInstanceBinaryVersion) || storageClusterVersion == minInstanceBinaryVersion {
		// minInstanceBinaryVersion supports storageClusterVersion so upgrade to storageClusterVersion.
		upgradeToVersion = storageClusterVersion
	} else if currentClusterVersion.Less(minInstanceBinaryVersion) {
		// minInstanceBinaryVersion doesn't support storageClusterVersion but we can upgrade
		// cluster version to minInstanceBinaryVersion.
		upgradeToVersion = minInstanceBinaryVersion
	} else {
		return upgradeBlockedDueToLowBinaryVersion, roachpb.Version{}, nil
	}
	return upgradeAllowed, upgradeToVersion, nil
}
