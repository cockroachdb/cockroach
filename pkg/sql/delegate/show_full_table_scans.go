// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowFullTableScans() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.FullTableScans)
	const query = `
  SELECT 
    key AS query, count, rows_read_avg, bytes_read_avg, service_lat_avg, contention_time_avg, max_mem_usage_avg, network_bytes_avg, max_retries
  FROM crdb_internal.node_statement_statistics WHERE full_scan = TRUE ORDER BY count DESC`
	return parse(query)
}
