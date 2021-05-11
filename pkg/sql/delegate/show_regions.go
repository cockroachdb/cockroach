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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowRegions(n *tree.ShowRegions) (tree.Statement, error) {
	zonesClause := `
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
					ORDER BY locality
				),
				NULL
			)
				AS zones
		FROM
			crdb_internal.kv_node_status
		GROUP BY
			region
	`
	switch n.ShowRegionsFrom {
	case tree.ShowRegionsFromAllDatabases:
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromAllDatabases)
		return parse(`
SELECT
	name as database_name,
	regions,
	primary_region
FROM crdb_internal.databases
ORDER BY database_name
			`,
		)

	case tree.ShowRegionsFromDatabase:
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromDatabase)
		dbName := string(n.DatabaseName)
		if dbName == "" {
			dbName = d.evalCtx.SessionData.Database
		}
		// Note the LEFT JOIN here -- in the case where regions no longer exist on the cluster
		// but still exist on the database config, we want to still see this database region
		// with no zones attached in this query.
		query := fmt.Sprintf(
			`
WITH zones_table(region, zones) AS (%s)
SELECT
	r.name AS "database",
	r.region as "region",
	r.region = r.primary_region AS "primary",
	COALESCE(zones_table.zones, '{}'::string[])
AS
	zones
FROM [
	SELECT
		name,
		unnest(dbs.regions) AS region,
		dbs.primary_region AS primary_region
	FROM crdb_internal.databases dbs
	WHERE dbs.name = %s
] r
LEFT JOIN zones_table ON (r.region = zones_table.region)
ORDER BY "primary" DESC, "region"`,
			zonesClause,
			lex.EscapeSQLString(dbName),
		)
		return parse(query)

	case tree.ShowRegionsFromCluster:
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromCluster)

		query := fmt.Sprintf(
			`
SELECT
	region, zones
FROM
	(%s)
WHERE
	region IS NOT NULL
ORDER BY
	region`,
			zonesClause,
		)

		return parse(query)
	case tree.ShowRegionsFromDefault:
		sqltelemetry.IncrementShowCounter(sqltelemetry.Regions)

		query := fmt.Sprintf(
			`
WITH databases_by_region(region, database_names) AS (
	SELECT
		region,
		array_agg(name) as database_names
	FROM [
		SELECT
			name,
			unnest(regions) AS region
		FROM crdb_internal.databases
	] GROUP BY region
),
databases_by_primary_region(region, database_names) AS (
	SELECT
		primary_region,
		array_agg(name)
	FROM crdb_internal.databases
	GROUP BY primary_region
),
zones_table(region, zones) AS (%s)
SELECT
	zones_table.region,
	zones_table.zones,
	COALESCE(databases_by_region.database_names, '{}'::string[]) AS database_names,
	COALESCE(databases_by_primary_region.database_names, '{}'::string[]) AS primary_region_of
FROM zones_table
LEFT JOIN databases_by_region ON (zones_table.region = databases_by_region.region)
LEFT JOIN databases_by_primary_region ON (zones_table.region = databases_by_primary_region.region)
ORDER BY zones_table.region
`,
			zonesClause,
		)
		return parse(query)
	}
	return nil, errors.Newf("unhandled ShowRegionsFrom: %v", n.ShowRegionsFrom)
}
