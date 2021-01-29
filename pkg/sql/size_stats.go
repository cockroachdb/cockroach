// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// AdminTableStats is an endpoint that returns disk usage and replication
// statistics for the specified table.
func (p *planner) AdminTableApproximateSize(
	ctx context.Context, databaseName string, schemaQualifiedTable string,
) (uint64, error) {
	resp, err := p.execCfg.SQLStatusServer.TableStats(ctx, &serverpb.TableStatsRequest{Database: databaseName,
		Table: schemaQualifiedTable})
	if err != nil || resp == nil {
		return 0, err
	}
	return resp.ApproximateDiskBytes, nil
}
