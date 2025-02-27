// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RunInitialSQL concerns itself with running "initial SQL" code when
// a cluster is started for the first time.
//
// The "startSingleNode" argument is true for `start-single-node`,
// and `cockroach demo` with 2 nodes or fewer.
// If adminUser is non-empty, an admin user with that name is
// created upon initialization. Its password is then also returned.
func (s *topLevelServer) RunInitialSQL(
	ctx context.Context, startSingleNode bool, adminUser, adminPassword string,
) error {
	if startSingleNode {
		s.sqlServer.disableLicenseEnforcement(ctx)
	}

	newCluster := s.InitialStart() && s.NodeID() == kvstorage.FirstNodeID
	if !newCluster || s.cfg.DisableSQLServer {
		// The initial SQL code only runs the first time the cluster is initialized.
		return nil
	}

	if adminUser != "" && !s.Insecure() {
		if err := s.createAdminUser(ctx, adminUser, adminPassword); err != nil {
			return err
		}
	}

	return nil
}

// RunInitialSQL implements cli.serverStartupInterface.
func (s *SQLServerWrapper) RunInitialSQL(context.Context, bool, string, string) error {
	return nil
}

// createAdminUser creates an admin user with the given name.
func (s *topLevelServer) createAdminUser(
	ctx context.Context, adminUser, adminPassword string,
) error {
	ie := s.sqlServer.internalExecutor
	_, err := ie.Exec(
		ctx, "admin-user", nil,
		fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", adminUser),
		adminPassword,
	)
	if err != nil {
		return err
	}
	// TODO(knz): Demote the admin user to an operator privilege with fewer options.
	_, err = ie.Exec(ctx, "admin-user", nil, fmt.Sprintf("GRANT admin TO %s", tree.Name(adminUser)))
	return err
}
