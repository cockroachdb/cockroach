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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/pkg/errors"
)

// UpgradeTestingKnobs is a part of the context used to control whether cluster
// version upgrade should happen automatically or not.
type UpgradeTestingKnobs struct {
	DisableUpgrade int32 // accessed atomically
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*UpgradeTestingKnobs) ModuleTestingKnobs() {}

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

		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Check if auto upgrade is disabled for test purposes.
			if k := s.cfg.TestingKnobs.Upgrade; k != nil {
				upgradeTestingKnobs := k.(*UpgradeTestingKnobs)
				if disable := atomic.LoadInt32(&upgradeTestingKnobs.DisableUpgrade); disable == 1 {
					log.Infof(ctx, "auto upgrade disabled by testing")
					continue
				}
			}

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
				if _, err := s.internalExecutor.Exec(
					ctx, "set-version", nil /* txn */, "SET CLUSTER SETTING version = crdb_internal.node_executable_version();",
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

	nodesWithLiveness, err := s.status.NodesWithLiveness(ctx)
	if err != nil {
		return false, err
	}

	var newVersion string
	for nodeID, st := range nodesWithLiveness {
		if st.LivenessStatus != storagepb.NodeLivenessStatus_LIVE &&
			st.LivenessStatus != storagepb.NodeLivenessStatus_DECOMMISSIONING {
			return false, errors.Errorf("node %d not running (%s), cannot determine version",
				nodeID, st.LivenessStatus)
		}

		version := st.Desc.ServerVersion.String()
		if newVersion == "" {
			newVersion = version
		} else if version != newVersion {
			return false, errors.New("not all nodes are running the latest version yet")
		}
	}

	// Check if we really need to upgrade cluster version.
	if newVersion == clusterVersion {
		return true, nil
	}

	// Check if auto upgrade is enabled at current version.
	datums, _, err := s.internalExecutor.Query(
		ctx, "read-downgrade", nil, /* txn */
		"SELECT value FROM system.settings WHERE name = 'cluster.preserve_downgrade_option';",
	)
	if err != nil {
		return false, err
	}

	if len(datums) != 0 {
		row := datums[0]
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
	datums, _, err := s.internalExecutor.Query(
		ctx, "show-version", nil, /* txn */
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
