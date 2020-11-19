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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowRegions(n *tree.ShowRegions) (tree.Statement, error) {
	if n.Database != "" {
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromDatabase)
		return nil, unimplemented.New(
			"show regions from database",
			"SHOW REGIONS FROM DATABASE not yet implemented",
		)
	}
	sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromCluster)

	regionKey := "region"
	zoneKeys := []string {"zone", "az", "availability_zone"}
	zoneKeysSubClause := ""

	// Build the zone sub-clause based on the zone keys that we're accepting above.
	for i, zk := range zoneKeys {
		zoneKeysSubClause += `
						substring(locality,'`
		zoneKeysSubClause += zk
		zoneKeysSubClause += `=([^,]*)')`

		// If we're not the last zone key, add a comma.
		if i != len(zoneKeys) - 1 {
			zoneKeysSubClause += `,`
		}
	}

	// Form the final zoneClause to include each of the substrings in zoneSubClause.
	zoneKeysClause := `
			array_remove(
				array_agg(
					COALESCE(` + zoneKeysSubClause + `
					)
				),
				NULL
			)`

	query := `
SELECT
	region, zones
FROM
	(
		SELECT
			substring(locality, '` + regionKey + `=([^,]*)') AS region,` +
			zoneKeysClause +
`			AS zones
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
