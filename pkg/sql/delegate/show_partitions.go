// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowPartitions(n *tree.ShowPartitions) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Partitions)
	if n.IsTable {
		flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
		tn := n.Table.ToTableName()

		dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
		if err != nil {
			return nil, err
		}
		if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
			return nil, err
		}

		// We use the raw_config_sql from the partition_lookup result to get the
		// official zone config for the partition, and use the full_config_sql from the zones table
		// which is the result of looking up the partition's inherited zone configuration.
		const showTablePartitionsQuery = `
		SELECT
			tables.database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value) as partition_value,
			replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
			replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
		FROM
			%[3]s.crdb_internal.partitions
			JOIN %[3]s.crdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN %[3]s.crdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN %[3]s.crdb_internal.zones ON
					partitions.zone_id = zones.zone_id
					AND partitions.subzone_id = zones.subzone_id
			LEFT JOIN %[3]s.crdb_internal.zones AS partition_lookup ON
				partition_lookup.database_name = tables.database_name
				AND partition_lookup.table_name = tables.name
				AND partition_lookup.index_name = table_indexes.index_name
				AND partition_lookup.partition_name = partitions.name
		WHERE
			tables.name = %[1]s AND tables.database_name = %[2]s
		ORDER BY
			1, 2, 3, 4, 5, 6, 7, 8, 9;
		`
		return parse(fmt.Sprintf(showTablePartitionsQuery,
			lex.EscapeSQLString(resName.Table()),
			lex.EscapeSQLString(resName.Catalog()),
			resName.CatalogName.String()))
	} else if n.IsDB {
		const showDatabasePartitionsQuery = `
		SELECT
			tables.database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value) as partition_value,
			replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
			replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
		FROM
			%[1]s.crdb_internal.partitions
			JOIN %[1]s.crdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN %[1]s.crdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN %[1]s.crdb_internal.zones ON
					partitions.zone_id = zones.zone_id
					AND partitions.subzone_id = zones.subzone_id
			LEFT JOIN %[1]s.crdb_internal.zones AS partition_lookup ON
				partition_lookup.database_name = tables.database_name
				AND partition_lookup.table_name = tables.name
				AND partition_lookup.index_name = table_indexes.index_name
				AND partition_lookup.partition_name = partitions.name
		WHERE
			tables.database_name = %[2]s
		ORDER BY
			tables.name, partitions.name, 1, 4, 5, 6, 7, 8, 9;
		`
		// Note: n.Database.String() != string(n.Database)
		return parse(fmt.Sprintf(showDatabasePartitionsQuery, n.Database.String(), lex.EscapeSQLString(string(n.Database))))
	}

	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Index.Table

	// Throw a more descriptive error if the user did not use the index hint syntax.
	if tn.ObjectName == "" {
		err := errors.New("no table specified")
		err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
		err = errors.WithHint(err, "Specify a table using the hint syntax of table@index.")
		return nil, err
	}

	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}

	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	// Force resolution of the index.
	_, _, err = cat.ResolveTableIndex(d.ctx, d.catalog, flags, &n.Index)
	if err != nil {
		return nil, err
	}

	const showIndexPartitionsQuery = `
	SELECT
		tables.database_name,
		tables.name AS table_name,
		partitions.name AS partition_name,
		partitions.parent_name AS parent_partition,
		partitions.column_names,
		concat(tables.name, '@', table_indexes.index_name) AS index_name,
		coalesce(partitions.list_value, partitions.range_value) as partition_value,
		replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
		replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
	FROM
		%[5]s.crdb_internal.partitions
		JOIN %[5]s.crdb_internal.table_indexes ON
				partitions.index_id = table_indexes.index_id
				AND partitions.table_id = table_indexes.descriptor_id
		JOIN %[5]s.crdb_internal.tables ON table_indexes.descriptor_id = tables.table_id
		LEFT JOIN %[5]s.crdb_internal.zones ON
			partitions.zone_id = zones.zone_id
			AND partitions.subzone_id = zones.subzone_id
		LEFT JOIN %[5]s.crdb_internal.zones AS partition_lookup ON
			partition_lookup.database_name = tables.database_name
			AND partition_lookup.table_name = tables.name
			AND partition_lookup.index_name = table_indexes.index_name
			AND partition_lookup.partition_name = partitions.name
	WHERE
		table_indexes.index_name = %[1]s AND tables.name = %[2]s
	ORDER BY
		1, 2, 3, 4, 5, 6, 7, 8, 9;
	`
	return parse(fmt.Sprintf(showIndexPartitionsQuery,
		lex.EscapeSQLString(n.Index.Index.String()),
		lex.EscapeSQLString(resName.Table()),
		resName.Table(),
		n.Index.Index.String(),
		// note: CatalogName.String() != Catalog()
		resName.CatalogName.String()))
}
