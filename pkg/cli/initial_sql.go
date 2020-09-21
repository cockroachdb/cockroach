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
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	if s.NodeID() != server.FirstNodeID {
		return "", waitUntilInitialSQLDone(ctx, s)
	}

	// We're on the first done. Has the initial SQL already run?
	isDone, err := isInitialSQLDone(ctx, s)
	if err != nil {
		return "", err
	}
	if isDone {
		log.Infof(ctx, "initial SQL has already run")
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

	// We completed the SQL initialization. Tell all the other nodes we're done.
	log.Infof(ctx, "initial SQL completed execution")
	if err := markInitialSQLDone(ctx, s); err != nil {
		return "", err
	}

	return adminPassword, nil
}

// markInitialSQLDone marks the initial SQL as having been executed.
// This unlocks the client acceptor in every node in the cluster.
func markInitialSQLDone(ctx context.Context, s *server.Server) error {
	return s.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		_, err := ie.Exec(ctx, "mark-sql-init", nil,
			`INSERT INTO system.eventlog(timestamp, "eventType", "targetID", "reportingID")
          VALUES (now(), $1, 0, $2)`,
			sql.EventLogInitialSQLExecuted, int32(s.NodeID()))
		return err
	})
}

// isInitialSQLDone returns true iff the initial SQL has been executed
// already.
func isInitialSQLDone(ctx context.Context, s *server.Server) (bool, error) {
	isDone := false
	err := s.RunLocalSQL(ctx, func(ctx context.Context, ie *sql.InternalExecutor) error {
		vals, err := ie.QueryRow(ctx, "wait-for-sql-init", nil,
			"SELECT count(*)=1 FROM system.eventlog WHERE \"eventType\" = $1",
			sql.EventLogInitialSQLExecuted)
		if err != nil {
			return err
		}
		isDone = bool(*vals[0].(*tree.DBool))
		return nil
	})
	return isDone, err
}

// waitUntilInitialSQLDone makes the node wait until the initial SQL has
// been executed. This waits until a row with a particular event ID
// appears in system.eventlog.
func waitUntilInitialSQLDone(ctx context.Context, s *server.Server) error {
	timer := timeutil.NewTimer()
	defer timer.Stop()
	for {
		log.Infof(ctx, "waiting for initial SQL to complete execution")
		isDone, err := isInitialSQLDone(ctx, s)
		if err != nil {
			return err
		}
		if isDone {
			log.Infof(ctx, "initial SQL has completed")
			break
		}

		// Wait a little bit before the next iteration, or let the context
		// cancel the wait.
		timer.Reset(time.Second)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Read = true
		}
	}
	return nil
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
