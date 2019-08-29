// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

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
