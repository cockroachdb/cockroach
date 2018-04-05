// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// UpgradeTestingKnobs is a part of the context used to control whether cluster
// version upgrade should happen automatically or not.
type UpgradeTestingKnobs struct {
	DisableUpgrade *int32
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*UpgradeTestingKnobs) ModuleTestingKnobs() {}

// startAttemptUpgrade attempts to upgrade cluster version.
func (s *Server) startAttemptUpgrade(ctx context.Context) {
	if err := s.stopper.RunAsyncTask(s.stopper.WithCancel(ctx), "auto-upgrade", func(ctx context.Context) {
		retryOpts := retry.Options{
			InitialBackoff: time.Second,
			MaxBackoff:     30 * time.Second,
			Multiplier:     2,
			Closer:         s.stopper.ShouldQuiesce(),
		}

		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			args := sql.SessionArgs{User: security.RootUser}
			session := sql.NewSession(ctx, args, s.sqlExecutor, &s.sqlMemMetrics, nil /* conn */)
			session.StartMonitor(&s.admin.memMonitor, mon.BoundAccount{})
			defer session.Finish(s.sqlExecutor)

			// Check if auto upgrade is disabled for test purposes.
			if k := s.cfg.TestingKnobs.Upgrade; k != nil {
				upgradeTestingKnobs := k.(*UpgradeTestingKnobs)
				if disable := atomic.LoadInt32(upgradeTestingKnobs.DisableUpgrade); disable == 1 {
					log.Infof(ctx, "auto upgrade disabled by testing")
					continue
				}
			}
			// Check if we should upgrade cluster version, keep checking upgrade
			// status, or stop attempting upgrade.
			if quit, err := s.upgradeStatus(ctx, session); err != nil {
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
				if res, err := s.sqlExecutor.ExecuteStatementsBuffered(
					session,
					"SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
					nil, 1,
				); err != nil {
					log.Infof(ctx, "error when finalizing cluster version upgrade: %s", err)
				} else {
					log.Info(ctx, "successfully upgraded cluster version")
					res.Close(ctx)
					return
				}
			}
		}
	}); err != nil {
		log.Infof(ctx, "failed attempt to upgrade cluster version, error: %s", err)
	}
}

// upgradeStatus lets the main checking loop know if we should do upgrade,
// keep checking upgrade status, or stop attempting upgrade.
// Return (true, nil) to indicate we want to stop attempting upgrade.
// Return (false, nil) to indicate we want to do the upgrade.
// Return (false, err) to indicate we want to keep checking upgrade status.
func (s *Server) upgradeStatus(ctx context.Context, session *sql.Session) (bool, error) {
	// Check if all nodes are running at the newest version.
	clusterVersion, err := s.clusterVersion(ctx, session)
	if err != nil {
		return false, err
	}

	const gossipStmt = "SELECT node_id, server_version FROM crdb_internal.gossip_nodes;"
	gr, err := s.sqlExecutor.ExecuteStatementsBuffered(session, gossipStmt, nil, 1)
	if err != nil {
		return false, err
	}
	defer gr.Close(ctx)

	statusMap := s.nodeLiveness.GetLivenessStatusMap()
	var newVersion string
	for i, nRows := 0, gr.ResultList[0].Rows.Len(); i < nRows; i++ {
		row := gr.ResultList[0].Rows.At(i)
		id := roachpb.NodeID(int32(tree.MustBeDInt(row[0])))
		version := string(tree.MustBeDString(row[1]))

		if statusMap[id] == storage.NodeLivenessStatus_LIVE {
			if newVersion == "" {
				newVersion = version
			} else if version != newVersion {
				return false, errors.New("not all nodes are running the latest version yet")
			}
		}
	}

	// Check if we really need to upgrade cluster version.
	if newVersion == clusterVersion {
		return true, nil
	}

	// Check if auto upgrade is enabled at current version.
	const downgradeStmt = `SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';`
	r, err := s.sqlExecutor.ExecuteStatementsBuffered(session, downgradeStmt, nil, 1)
	if err != nil {
		return false, err
	}
	defer r.Close(ctx)
	if r.ResultList[0].Rows.Len() != 0 {
		row := r.ResultList[0].Rows.At(0)
		downgradeVersion := string(tree.MustBeDString(row[0]))

		if clusterVersion == downgradeVersion {
			return false, errors.Errorf("auto upgrade is disabled for current version: %s", clusterVersion)
		}
	}

	// Check if all non-decommissioned nodes are alive.
	for id, status := range statusMap {
		if status != storage.NodeLivenessStatus_DECOMMISSIONED && status != storage.NodeLivenessStatus_LIVE {
			return false, errors.Errorf("node %d is not decommissioned but not alive, node status: %s.",
				id, storage.NodeLivenessStatus_name[int32(status)])
		}
	}
	return false, nil
}

// clusterVersion returns the current cluster version from the SQL subsystem
// (which returns the version from the KV store as opposed to the possibly
// lagging settings subsystem)
func (s *Server) clusterVersion(ctx context.Context, session *sql.Session) (string, error) {
	const clusterVersionStmt = "SHOW CLUSTER SETTING version;"
	r, err := s.sqlExecutor.ExecuteStatementsBuffered(session, clusterVersionStmt, nil, 1)
	if err != nil {
		return "", err
	}
	defer r.Close(ctx)
	if r.ResultList[0].Rows.Len() == 0 {
		return "", errors.New("cluster version is not set")
	}
	row := r.ResultList[0].Rows.At(0)
	clusterVersion := string(tree.MustBeDString(row[0]))

	return clusterVersion, nil
}
