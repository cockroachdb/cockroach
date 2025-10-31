// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *topLevelServer) startAttemptUpgrade(ctx context.Context) error {
	return s.stopper.RunAsyncTask(ctx, "auto-upgrade", func(ctx context.Context) {
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
			clusterVersion, err := s.clusterVersion(ctx)
			if err != nil {
				log.Errorf(ctx, "unable to retrieve cluster version: %v", err)
				continue
			}

			// Check if we should upgrade cluster version, keep checking upgrade
			// status, or stop attempting upgrade.
			status, err := s.upgradeStatus(ctx, clusterVersion)
			switch status {
			case UpgradeBlockedDueToError:
				log.Errorf(ctx, "failed attempt to upgrade cluster version, error: %v", err)
				continue
			case UpgradeBlockedDueToMixedVersions:
				log.Infof(ctx, "failed attempt to upgrade cluster version: %v", err)
				continue
			case UpgradeDisabledByConfigurationToPreserveDowngrade:
				log.Infof(ctx, "auto upgrade is disabled for current version (preserve_downgrade_option): %s", redact.Safe(clusterVersion))
				// Note: we do 'continue' here (and not 'return') so that the
				// auto-upgrade gets a chance to continue/complete if the
				// operator resets `preserve_downgrade_option` after the node
				// has started up already.
				continue
			case UpgradeDisabledByConfiguration:
				log.Infof(ctx, "auto upgrade is disabled by (cluster.auto_upgrade.enabled)")
				// Note: we do 'continue' here (and not 'return') so that the
				// auto-upgrade gets a chance to continue/complete if the
				// operator resets `auto_upgrade.enabled` after the node
				// has started up already.
				continue
			case UpgradeAlreadyCompleted:
				log.Info(ctx, "no need to upgrade, cluster already at the newest version")
				return
			case UpgradeAllowed:
				// Fall out of the select below.
			default:
				panic(errors.AssertionFailedf("unhandled case: %d", status))
			}

			if clusterversion.AutoUpgradeSystemClusterFromMeta1Leaseholder.Get(&s.ClusterSettings().SV) {
				isMeta1LH, err := s.sqlServer.isMeta1Leaseholder(ctx, s.clock.NowAsClockTimestamp())
				if err != nil || !isMeta1LH {
					log.Ops.VInfof(ctx, 2, "not upgrading since we are not the Meta1 leaseholder; err=%v", err)
					continue
				}
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
				if _, err := s.sqlServer.internalExecutor.ExecEx(
					ctx, "set-version", nil, /* txn */
					sessiondata.NodeUserSessionDataOverride,
					"SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
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

type upgradeStatus int8

const (
	UpgradeAllowed upgradeStatus = iota
	UpgradeAlreadyCompleted
	UpgradeDisabledByConfiguration
	UpgradeDisabledByConfigurationToPreserveDowngrade
	UpgradeBlockedDueToError
	UpgradeBlockedDueToMixedVersions
	UpgradeBlockedDueToLowStorageClusterVersion
)

// isAutoUpgradeEnabled consults `cluster.auto_upgrade.enabled` and
// `cluster.preserve_downgrade_option` settings to decide if automatic
// upgrade is enabled. The later setting will be retired in a future
// release.
func (s *topLevelServer) isAutoUpgradeEnabled(currentClusterVersion string) upgradeStatus {
	if autoUpgradeEnabled := clusterversion.AutoUpgradeEnabled.Get(&s.ClusterSettings().SV); !autoUpgradeEnabled {
		// Automatic upgrade is not enabled.
		return UpgradeDisabledByConfiguration
	}
	if downgradeVersion := clusterversion.PreserveDowngradeVersion.Get(&s.ClusterSettings().SV); downgradeVersion != "" {
		if currentClusterVersion == downgradeVersion {
			return UpgradeDisabledByConfigurationToPreserveDowngrade
		}
	}
	return UpgradeAllowed
}

// upgradeStatus lets the main checking loop know if we should do upgrade,
// keep checking upgrade status, or stop attempting upgrade.
func (s *topLevelServer) upgradeStatus(
	ctx context.Context, clusterVersion string,
) (st upgradeStatus, err error) {
	nodes, err := s.status.ListNodesInternal(ctx, nil)
	if err != nil {
		return UpgradeBlockedDueToError, err
	}
	vitalities, err := s.nodeLiveness.ScanNodeVitalityFromKV(ctx)
	if err != nil {
		return UpgradeBlockedDueToError, err
	}

	var newVersion string
	var notRunningErr error
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		v := vitalities[nodeID]

		// Skip over removed nodes.
		if v.IsDecommissioned() {
			continue
		}

		// TODO(baptist): This does not allow upgrades if any nodes are draining.
		// This may be an overly strict check as the operator may want to leave the
		// node in a draining state until post upgrade.
		if !v.IsLive(livenesspb.Upgrade) {
			// We definitely won't be able to upgrade, but defer this error as
			// we may find out that we are already at the latest version (the
			// cluster may be up-to-date, but a node is down).
			if notRunningErr == nil {
				notRunningErr = errors.Errorf("node %d not running (%d), cannot determine version", nodeID, st)
			}
			// However, we don't want to exit the auto upgrade process if we can't validate
			// our own version. Consider the case where the meta1 leaseholder is the first
			// node to restart to the new version but has transient liveness issues. If we skip
			// the check here, we will see all other nodes at the same (old) version and exit
			// the auto upgrade process with UpgradeAlreadyCompleted. Since the meta1 leaseholder
			// is the only node that can perform the upgrade, this indefinitely blocks the upgrade.
			if s.node.Descriptor.NodeID == nodeID {
				return UpgradeBlockedDueToError, errors.Errorf("node %d not running (%d), cannot determine version", nodeID, st)
			}
			continue
		}

		version := node.Desc.ServerVersion.String()
		if newVersion == "" {
			newVersion = version
		} else if version != newVersion {
			return UpgradeBlockedDueToMixedVersions, errors.Newf(
				"not all nodes are running the latest version yet (saw %s and %s)",
				redact.Safe(newVersion), redact.Safe(version))
		}
	}

	if newVersion == "" {
		return UpgradeBlockedDueToError, errors.Errorf("no live nodes found")
	}

	// Check if we really need to upgrade cluster version.
	if newVersion == clusterVersion {
		return UpgradeAlreadyCompleted, nil
	}

	if notRunningErr != nil {
		return UpgradeBlockedDueToError, notRunningErr
	}

	return s.isAutoUpgradeEnabled(clusterVersion), nil
}

// clusterVersion returns the current cluster version from the SQL subsystem
// (which returns the version from the KV store as opposed to the possibly
// lagging settings subsystem).
func (s *topLevelServer) clusterVersion(ctx context.Context) (string, error) {
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx, "show-version", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SHOW CLUSTER SETTING version;",
	)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", errors.New("cluster version is not set")
	}
	clusterVersion := string(tree.MustBeDString(row[0]))

	return clusterVersion, nil
}
