// Copyright 2020 The Cockroach Authors.
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

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowRegions(n *tree.ShowRegions) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Regions)
	// TODO (storm): Change this so that it doesn't use hard-coded strings and is
	// more flexible for custom named sub-regions.
	query := `
SELECT
	region, zones
FROM
	(
		SELECT
			substring(locality, 'region=([^,]*)') AS region,
			array_remove(
				array_agg(
					COALESCE(
						substring(locality, 'az=([^,]*)'),
						substring(
							locality,
							'availability-zone=([^,]*)'),
						substring(
							locality,
							'zone=([^,]*)'
						)
					)
				),
				NULL
			)
				AS zones
		FROM
			crdb_internal.kv_node_status
		GROUP BY
			region
	)
WHERE
	region IS NOT NULL
ORDER BY
	region`

	return parse(query)
}
