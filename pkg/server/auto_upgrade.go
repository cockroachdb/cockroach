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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *Server) startAttemptUpgrade(ctx context.Context) error {
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
			case upgradeBlockedDueToError:
				log.Errorf(ctx, "failed attempt to upgrade cluster version, error: %v", err)
				continue
			case upgradeBlockedDueToMixedVersions:
				log.Infof(ctx, "failed attempt to upgrade cluster version: %v", err)
				continue
			case upgradeDisabledByConfiguration:
				log.Infof(ctx, "auto upgrade is disabled for current version (preserve_downgrade_option): %s", redact.Safe(clusterVersion))
				// Note: we do 'continue' here (and not 'return') so that the
				// auto-upgrade gets a chance to continue/complete if the
				// operator resets `preserve_downgrade_option` after the node
				// has started up already.
				continue
			case upgradeAlreadyCompleted:
				log.Info(ctx, "no need to upgrade, cluster already at the newest version")
				return
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
				if _, err := s.sqlServer.internalExecutor.ExecEx(
					ctx, "set-version", nil, /* txn */
					sessiondata.RootUserSessionDataOverride,
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
	upgradeAllowed upgradeStatus = iota
	upgradeAlreadyCompleted
	upgradeDisabledByConfiguration
	upgradeBlockedDueToError
	upgradeBlockedDueToMixedVersions
)

// upgradeStatus lets the main checking loop know if we should do upgrade,
// keep checking upgrade status, or stop attempting upgrade.
func (s *Server) upgradeStatus(
	ctx context.Context, clusterVersion string,
) (st upgradeStatus, err error) {
	nodes, err := s.status.ListNodesInternal(ctx, nil)
	if err != nil {
		return upgradeBlockedDueToError, err
	}
	clock := s.admin.server.clock
	statusMap, err := getLivenessStatusMap(ctx, s.nodeLiveness, clock.Now().GoTime(), s.st)
	if err != nil {
		return upgradeBlockedDueToError, err
	}

	var newVersion string
	var notRunningErr error
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		st := statusMap[nodeID]

		// Skip over removed nodes.
		if st == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			continue
		}

		if st != livenesspb.NodeLivenessStatus_LIVE &&
			st != livenesspb.NodeLivenessStatus_DECOMMISSIONING {
			// We definitely won't be able to upgrade, but defer this error as
			// we may find out that we are already at the latest version (the
			// cluster may be up-to-date, but a node is down).
			if notRunningErr == nil {
				notRunningErr = errors.Errorf("node %d not running (%s), cannot determine version", nodeID, st)
			}
			continue
		}

		version := node.Desc.ServerVersion.String()
		if newVersion == "" {
			newVersion = version
		} else if version != newVersion {
			return upgradeBlockedDueToMixedVersions, errors.Newf(
				"not all nodes are running the latest version yet (saw %s and %s)",
				redact.Safe(newVersion), redact.Safe(version))
		}
	}

	if newVersion == "" {
		return upgradeBlockedDueToError, errors.Errorf("no live nodes found")
	}

	// Check if we really need to upgrade cluster version.
	if newVersion == clusterVersion {
		return upgradeAlreadyCompleted, nil
	}

	if notRunningErr != nil {
		return upgradeBlockedDueToError, notRunningErr
	}

	// Check if auto upgrade is enabled at current version. This is read from
	// the KV store so that it's in effect on all nodes immediately following a
	// SET CLUSTER SETTING.
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx, "read-downgrade", nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		"SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';",
	)
	if err != nil {
		return upgradeBlockedDueToError, err
	}

	if row != nil {
		downgradeVersion := string(tree.MustBeDString(row[0]))

		if clusterVersion == downgradeVersion {
			return upgradeDisabledByConfiguration, nil
		}
	}

	return upgradeAllowed, nil
}

// clusterVersion returns the current cluster version from the SQL subsystem
// (which returns the version from the KV store as opposed to the possibly
// lagging settings subsystem).
func (s *Server) clusterVersion(ctx context.Context) (string, error) {
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx, "show-version", nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
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
