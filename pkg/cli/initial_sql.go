// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// runInitialSQL concerns itself with running "initial SQL" code when
// a cluster is started for the first time.
//
// The "startSingleNode" argument is true for `start-single-node`,
// and `cockroach demo` with 2 nodes or fewer.
// If adminUser is non-empty, an admin user with that name is
// created upon initialization. Its password is then also returned.
func runInitialSQL(
	ctx context.Context, s *server.Server, startSingleNode bool, adminUser string,
) (adminPassword string, err error) {
	newCluster := s.InitialStart() && s.NodeID() == server.FirstNodeID
	if !newCluster {
		// The initial SQL code only runs the first time the cluster is initialized.
		return "", nil
	}

	if startSingleNode {
		// For start-single-node, set the default replication factor to
		// 1 so as to avoid warning messages and unnecessary rebalance
		// churn.
		if err := cliDisableReplication(ctx, s); err != nil {
			log.Errorf(ctx, "could not disable replication: %v", err)
			return "", err
		}
		log.Infof(ctx, "Replication was disabled for this cluster.\n"+
			"When/if adding nodes in the future, update zone configurations to increase the replication factor.")
	}

	if adminUser != "" && !s.Insecure() {
		adminPassword, err = createAdminUser(ctx, s, adminUser)
		if err != nil {
			return "", err
		}
	}

	return adminPassword, nil
}

// createAdminUser creates an admin user with the given name.
func createAdminUser(
	ctx context.Context, s *server.Server, adminUser string,
) (adminPassword string, err error) {
	adminPassword, err = security.GenerateRandomPassword()
	if err != nil {
		return "", err
	}
	if err := s.RunLocalSQL(ctx,
		func(ctx context.Context, ie *sql.InternalExecutor) error {
			_, err := ie.Exec(ctx, "admin-user", nil, "CREATE USER $1 WITH PASSWORD $2", adminUser, adminPassword)
			if err != nil {
				return err
			}
			// TODO(knz): Demote the admin user to an operator privilege with fewer options.
			_, err = ie.Exec(ctx, "admin-user", nil, fmt.Sprintf("GRANT admin TO %s", tree.Name(adminUser)))
			return err
		}); err != nil {
		return "", err
	}
	return adminPassword, nil
}

// cliDisableReplication changes the replication factor on
// all defined zones to become 1. This is used by start-single-node
// and demo to define single-node clusters, so as to avoid
// churn in the log files.
//
// The change is effected using the internal SQL interface of the
// given server object.
func cliDisableReplication(ctx context.Context, s *server.Server) error {
	return s.RunLocalSQL(ctx,
		func(ctx context.Context, ie *sql.InternalExecutor) error {
			rows, err := ie.Query(ctx, "get-zones", nil,
				"SELECT target FROM crdb_internal.zones")
			if err != nil {
				return err
			}

			for _, row := range rows {
				zone := string(*row[0].(*tree.DString))
				if _, err := ie.Exec(ctx, "set-zone", nil,
					fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1", zone)); err != nil {
					return err
				}
			}

			return nil
		})
}
