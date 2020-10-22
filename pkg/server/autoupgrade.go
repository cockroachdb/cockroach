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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// startAutoUpgrade attempts to auto upgrade cluster version to the binary's
// current version.
func (s *Server) startAutoUpgrade(ctx context.Context) {
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	if err := s.stopper.RunAsyncTask(ctx, "auto-upgrade", func(ctx context.Context) {
		defer cancel()
		retryOpts := retry.Options{
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			Multiplier:     2,
			Closer:         s.stopper.ShouldQuiesce(),
		}

		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Check if auto upgrade is disabled for test purposes.
			if k := s.cfg.TestingKnobs.Server; k != nil {
				upgradeTestingKnobs := k.(*TestingKnobs)
				if disable := atomic.LoadInt32(&upgradeTestingKnobs.DisableAutomaticVersionUpgrade); disable == 1 {
					log.Infof(ctx, "auto upgrade disabled by testing")
					continue
				}
			}

			// Check if we should upgrade cluster version, keep checking upgrade
			// status, or stop attempting upgrade.
			shouldUpgrade, err := s.shouldUpgrade(ctx)
			if err != nil {
				log.Infof(ctx, "failed attempt to upgrade cluster version, error: %s", err)
				continue
			}
			if !shouldUpgrade {
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
					sessiondata.InternalExecutorOverride{User: security.RootUser},
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

// shouldUpgrade lets the autoupgrade loop know whether or not we should
// upgrade, or retry after if if we can't yet tell.
//  - (true, nil) indicates that we should upgrade
//  - (false, nil) indicates that we shouldn't upgrade
//  - (    , err) indicate we don't know one way or another, and should retry
func (s *Server) shouldUpgrade(ctx context.Context) (should bool, _ error) {
	livenesses, err := s.nodeLiveness.GetLivenessesFromKV(ctx)
	if err != nil {
		return false, err
	}

	if len(livenesses) == 0 {
		return false, errors.AssertionFailedf("didn't find any liveness records")
	}

	for _, liveness := range livenesses {
		// For nodes that have expired and haven't been decommissioned, stall
		// auto-upgrade.
		expiration := hlc.Timestamp(liveness.Expiration)
		if expiration.Less(s.clock.Now()) && !liveness.Membership.Decommissioned() {
			return false, errors.Errorf("node %d not live (since %s), and not decommissioned", liveness.NodeID, expiration.String())
		}
	}

	resp, err := s.status.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return false, err
	}

	if len(resp.Nodes) != len(livenesses) {
		return false, errors.AssertionFailedf("mismatched number of node statuses, expected %d got %d", len(livenesses), len(resp.Nodes))
	}

	firstVersion := resp.Nodes[0].Desc.ServerVersion
	for _, status := range resp.Nodes[1:] {
		// Check version of all nodes.
		if status.Desc.ServerVersion != firstVersion {
			return false, errors.Newf("mismatched server versions between nodes (saw %s and %s)", firstVersion, status.Desc.ServerVersion)
		}
	}

	// Check if all nodes are running at the newest version.
	clusterVersion, err := s.clusterVersion(ctx)
	if err != nil {
		return false, err
	}

	// Check if we really need to upgrade cluster version.
	if firstVersion.String() == clusterVersion {
		return false, nil
	}

	// Check if auto upgrade is enabled at current version. This is read from
	// the KV store so that it's in effect on all nodes immediately following a
	// SET CLUSTER SETTING.
	datums, err := s.sqlServer.internalExecutor.QueryEx(
		ctx, "read-downgrade", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUser},
		"SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';",
	)
	if err != nil {
		return false, err
	}

	if len(datums) != 0 {
		downgradeVersion := string(tree.MustBeDString(datums[0][0]))
		if clusterVersion == downgradeVersion {
			return false, errors.Errorf("auto upgrade is disabled for current version: %s", clusterVersion)
		}
	}
	return true, nil
}

// clusterVersion returns the current cluster version from the SQL subsystem
// (which returns the version from the KV store as opposed to the possibly
// lagging settings subsystem).
func (s *Server) clusterVersion(ctx context.Context) (string, error) {
	datums, err := s.sqlServer.internalExecutor.QueryEx(
		ctx, "show-version", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUser},
		"SHOW CLUSTER SETTING version;",
	)
	if err != nil {
		return "", err
	}
	if len(datums) == 0 {
		return "", errors.New("cluster version is not set")
	}
	row := datums[0]
	clusterVersion := string(tree.MustBeDString(row[0]))
	return clusterVersion, nil
}
