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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *Server) startAttemptUpgrade(ctx context.Context) {
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	if err := s.stopper.RunAsyncTask(ctx, "auto-upgrade", func(ctx context.Context) {
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
			// Check if we should upgrade cluster version, keep checking upgrade
			// status, or stop attempting upgrade.
			if quit, err := s.upgradeStatus(ctx); err != nil {
				log.Infof(ctx, "failed attempt to upgrade cluster version, error: %s", err)
				continue
			} else if quit {
				log.Info(ctx, "no need to upgrade, cluster already at the newest version")
				return
			}

			upgradeRetryOpts := retry.Options{
				InitialBackoff: 5 * time.Second,
				MaxBackoff:     10 * time.Second,
				Multiplier:     2,
				Closer:         s.stopper.ShouldQuiesce(),
			}

			// Run the set cluster setting version statement and reset cluster setting
			// `cluster.preserve_downgrade_option` statement in a transaction until
			// success.
			for ur := retry.StartWithCtx(ctx, upgradeRetryOpts); ur.Next(); {
				if _, err := s.sqlServer.internalExecutor.ExecEx(
					ctx, "set-version", nil, /* txn */
					sessiondata.InternalExecutorOverride{User: security.RootUserName()},
					"SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
				); err != nil {
					log.Infof(ctx, "error when finalizing cluster version upgrade: %s", err)
				} else {
					log.Info(ctx, "successfully upgraded cluster version")
					return
				}
			}
		}
	}); err != nil {
		cancel()
		log.Infof(ctx, "failed attempt to upgrade cluster version, error: %s", err)
	}
}

// upgradeStatus lets the main checking loop know if we should do upgrade,
// keep checking upgrade status, or stop attempting upgrade.
// Return (true, nil) to indicate we want to stop attempting upgrade.
// Return (false, nil) to indicate we want to do the upgrade.
// Return (false, err) to indicate we want to keep checking upgrade status.
func (s *Server) upgradeStatus(ctx context.Context) (bool, error) {
	// Check if all nodes are running at the newest version.
	clusterVersion, err := s.clusterVersion(ctx)
	if err != nil {
		return false, err
	}

	nodesWithLiveness, err := s.status.nodesStatusWithLiveness(ctx)
	if err != nil {
		return false, err
	}

	var newVersion string
	var notRunningErr error
	for nodeID, st := range nodesWithLiveness {
		if st.livenessStatus != livenesspb.NodeLivenessStatus_LIVE &&
			st.livenessStatus != livenesspb.NodeLivenessStatus_DECOMMISSIONING {
			// We definitely won't be able to upgrade, but defer this error as
			// we may find out that we are already at the latest version (the
			// cluster may be up to date, but a node is down).
			if notRunningErr == nil {
				notRunningErr = errors.Errorf("node %d not running (%s), cannot determine version", nodeID, st.livenessStatus)
			}
			continue
		}

		version := st.NodeStatus.Desc.ServerVersion.String()
		if newVersion == "" {
			newVersion = version
		} else if version != newVersion {
			return false, errors.Newf("not all nodes are running the latest version yet (saw %s and %s)", newVersion, version)
		}
	}

	if newVersion == "" {
		return false, errors.Errorf("no live nodes found")
	}

	// Check if we really need to upgrade cluster version.
	if newVersion == clusterVersion {
		return true, nil
	}

	if notRunningErr != nil {
		return false, notRunningErr
	}

	// Check if auto upgrade is enabled at current version. This is read from
	// the KV store so that it's in effect on all nodes immediately following a
	// SET CLUSTER SETTING.
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx, "read-downgrade", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';",
	)
	if err != nil {
		return false, err
	}

	if row != nil {
		downgradeVersion := string(tree.MustBeDString(row[0]))

		if clusterVersion == downgradeVersion {
			return false, errors.Errorf("auto upgrade is disabled for current version: %s", clusterVersion)
		}
	}

	return false, nil
}

// clusterVersion returns the current cluster version from the SQL subsystem
// (which returns the version from the KV store as opposed to the possibly
// lagging settings subsystem).
func (s *Server) clusterVersion(ctx context.Context) (string, error) {
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx, "show-version", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
