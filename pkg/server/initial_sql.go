// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RunInitialSQL concerns itself with running "initial SQL" code when
// a cluster is started for the first time.
//
// The "startSingleNode" argument is true for `start-single-node`,
// and `cockroach demo` with 2 nodes or fewer.
// If adminUser is non-empty, an admin user with that name is
// created upon initialization. Its password is then also returned.
func (s *Server) RunInitialSQL(
	ctx context.Context, startSingleNode, createAppTenant bool, adminUser, adminPassword string,
) error {
	newCluster := s.InitialStart() && s.NodeID() == kvserver.FirstNodeID
	if !newCluster {
		// The initial SQL code only runs the first time the cluster is initialized.
		return nil
	}

	if startSingleNode {
		// For start-single-node, set the default replication factor to
		// 1 so as to avoid warning messages and unnecessary rebalance
		// churn.
		if err := s.disableReplication(ctx); err != nil {
			log.Ops.Errorf(ctx, "could not disable replication: %v", err)
			return err
		}
		log.Ops.Infof(ctx, "Replication was disabled for this cluster.\n"+
			"When/if adding nodes in the future, update zone configurations to increase the replication factor.")
	}

	if adminUser != "" && !s.Insecure() {
		if err := s.createAdminUser(ctx, adminUser, adminPassword); err != nil {
			return err
		}
	}

	if createAppTenant {
		if err := s.createAppTenant(ctx); err != nil {
			return err
		}
	}

	return nil
}

// RunInitialSQL implements cli.serverStartupInterface.
func (s *SQLServerWrapper) RunInitialSQL(context.Context, bool, bool, string, string) error {
	return nil
}

// createAppTenant creates an application tenant if it does
// not exist yet.
// TODO(knz): Replace this by a long-running migration.
// See: https://github.com/cockroachdb/cockroach/issues/91051
func (s *Server) createAppTenant(ctx context.Context) error {
	ie := s.sqlServer.internalExecutor
	_, err := ie.Exec(
		ctx, "create-app-tenant", nil, /* txn */
		// This query picks up the first available ID automatically.
		`
WITH tid AS (
  SELECT id+1 AS newid
    FROM (VALUES (1) UNION ALL SELECT id FROM system.tenants) AS u(id)
   WHERE NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.id=u.id+1)
     AND NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.name='app')
   LIMIT 1
) SELECT "".crdb_internal.create_tenant(newid, 'app') FROM tid
`)
	return err
}

// createAdminUser creates an admin user with the given name.
func (s *Server) createAdminUser(ctx context.Context, adminUser, adminPassword string) error {
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

// disableReplication changes the replication factor on
// all defined zones to become 1. This is used by start-single-node
// and demo to define single-node clusters, so as to avoid
// churn in the log files.
//
// The change is effected using the internal SQL interface of the
// given server object.
func (s *Server) disableReplication(ctx context.Context) (retErr error) {
	ie := s.sqlServer.internalExecutor

	it, err := ie.QueryIterator(ctx, "get-zones", nil,
		"SELECT target FROM crdb_internal.zones")
	if err != nil {
		return err
	}
	// We have to make sure to close the iterator since we might return
	// from the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		zone := string(*it.Cur()[0].(*tree.DString))
		if _, err := ie.Exec(ctx, "set-zone", nil,
			fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1", zone)); err != nil {
			return err
		}
	}
	return err
}
