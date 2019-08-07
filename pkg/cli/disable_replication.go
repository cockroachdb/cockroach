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
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func cliDisableReplication(ctx context.Context, pgURL string) error {
	conn := makeSQLConn(pgURL)
	defer conn.Close()
	rows, err := conn.Query("SELECT target FROM crdb_internal.zones", nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	var zones []string
	nextVals := make([]driver.Value, len(rows.Columns()))
	for {
		err := rows.Next(nextVals)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		zones = append(zones, nextVals[0].(string))
	}
	for _, zone := range zones {
		if err := conn.Exec(
			fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1", zone), nil,
		); err != nil {
			return err
		}
	}

	log.Shout(ctx, log.Severity_INFO,
		"Replication was disabled for this cluster.\n"+
			"When/if adding nodes in the future, update zone configurations to increase the replication factor.")

	return nil
}
