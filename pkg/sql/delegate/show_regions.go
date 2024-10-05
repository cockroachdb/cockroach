// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowRegions(n *tree.ShowRegions) (tree.Statement, error) {
	zonesClause := `
		SELECT
			region, zones
		FROM crdb_internal.regions
		ORDER BY region
`
	switch n.ShowRegionsFrom {
	case tree.ShowRegionsFromAllDatabases:
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromAllDatabases)
		return d.parse(`
SELECT
	name as database_name,
	regions,
	primary_region,
	secondary_region
FROM crdb_internal.databases
ORDER BY database_name
			`,
		)

	case tree.ShowRegionsFromDatabase:
		sqltelemetry.IncrementShowCounter(sqltelemetry.RegionsFromDatabase)
		dbName := string(n.DatabaseName)
		if dbName == "" {
			dbName = d.evalCtx.SessionData().Database
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
	r.region = r.secondary_region AS "secondary",
	COALESCE(zones_table.zones, '{}'::string[])
AS
	zones
FROM [
	SELECT
		name,
		unnest(dbs.regions) AS region,
		dbs.primary_region AS primary_region,
		dbs.secondary_region AS secondary_region
	FROM crdb_internal.databases dbs
	WHERE dbs.name = %s
] r
LEFT JOIN zones_table ON (r.region = zones_table.region)
ORDER BY "primary" DESC, "secondary" DESC, "region"`,
			zonesClause,
			lexbase.EscapeSQLString(dbName),
		)

		return d.parse(query)

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

		return d.parse(query)
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
databases_by_secondary_region(region, database_names) AS (
	SELECT
		secondary_region,
		array_agg(name)
	FROM crdb_internal.databases
	GROUP BY secondary_region
),
zones_table(region, zones) AS (%s)
SELECT
	zones_table.region,
	zones_table.zones,
	COALESCE(databases_by_region.database_names, '{}'::string[]) AS database_names,
	COALESCE(databases_by_primary_region.database_names, '{}'::string[]) AS primary_region_of,
	COALESCE(databases_by_secondary_region.database_names, '{}'::string[]) AS secondary_region_of
FROM zones_table
LEFT JOIN databases_by_region ON (zones_table.region = databases_by_region.region)
LEFT JOIN databases_by_primary_region ON (zones_table.region = databases_by_primary_region.region)
LEFT JOIN databases_by_secondary_region ON (zones_table.region = databases_by_secondary_region.region)
ORDER BY zones_table.region
`,
			zonesClause,
		)
		return d.parse(query)
	case tree.ShowSuperRegionsFromDatabase:
		sqltelemetry.IncrementShowCounter(sqltelemetry.SuperRegions)

		query := fmt.Sprintf(
			`
SELECT database_name, super_region_name, regions
  FROM crdb_internal.super_regions
 WHERE database_name = '%s'`, n.DatabaseName)

		return d.parse(query)
	}
	return nil, errors.Newf("unhandled ShowRegionsFrom: %v", n.ShowRegionsFrom)
}
