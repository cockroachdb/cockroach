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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

	if startSingleNode && (*s.cfg.DefaultSystemZoneConfig.NumReplicas != 1 || *s.cfg.DefaultZoneConfig.NumReplicas != 1) {
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

// disableReplication changes the replication factor on
// all defined zones to become 1. This is used by start-single-node
// and demo to define single-node clusters, so as to avoid
// churn in the log files.
//
// The change is effected using the internal SQL interface of the
// given server object.
func (s *topLevelServer) disableReplication(ctx context.Context) (retErr error) {
	ie := s.sqlServer.internalExecutor

	it, err := ie.QueryIteratorEx(ctx, "get-zones", nil, sessiondata.NodeUserSessionDataOverride,
		"SELECT target FROM crdb_internal.zones")
	if err != nil {
		return err
	}

	// We have to make sure to close the iterator since we might return
	// from the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// TODO(#125882): For now, we need to cache the zones before we can
	// modify them. This is because the iterator will open a transaction that
	// holds a lease to the system database, which will block the ALTER DATABASE
	// system schema change in declarative-schema-changer-land.
	var ok bool
	var zones []string
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		zone := string(*it.Cur()[0].(*tree.DString))
		zones = append(zones, zone)
	}
	if err != nil {
		return err
	}

	for _, zone := range zones {
		if _, err := ie.Exec(ctx, "set-zone", nil,
			fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1", zone)); err != nil {
			return err
		}
	}

	return nil
}
