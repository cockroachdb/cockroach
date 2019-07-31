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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowPartitions(n *tree.ShowPartitions) (tree.Statement, error) {
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

		const showTablePartitionsQuery = `
		SELECT
			database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value) as partition_value,
			regexp_extract(config_yaml, e'constraints: (\\[.*\\])') AS zone_constraints
		FROM
			crdb_internal.partitions
			JOIN crdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN crdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN crdb_internal.zones ON
					zones.zone_name
					= concat(database_name, '.', tables.name, '.', partitions.name)
		WHERE
			tables.name = %[1]s AND database_name = %[2]s;
		`
		return parse(fmt.Sprintf(showTablePartitionsQuery, lex.EscapeSQLString(resName.Table()), lex.EscapeSQLString(resName.Catalog())))
	} else if n.IsDB {
		const showDatabasePartitionsQuery = `
		SELECT
			database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value) as partition_value,
			regexp_extract(config_yaml, e'constraints: (\\[.*\\])') AS zone_constraints
		FROM
			%[1]s.crdb_internal.partitions
			JOIN %[1]s.crdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN %[1]s.crdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN %[1]s.crdb_internal.zones ON
					zones.zone_name
					= concat(database_name, '.', tables.name, '.', partitions.name)
		WHERE
			database_name = %[2]s
		ORDER BY
			tables.name, partitions.name;
		`
		return parse(fmt.Sprintf(showDatabasePartitionsQuery, n.Object, lex.EscapeSQLString(n.Object)))
	}

	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Index.Table

	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	// TODO (rohany): The dummy query to force resolution of the index
	// is a dirty hack that needs to be fixed.
	const showIndexPartitionsQuery = `
	WITH
		dummy AS (SELECT * FROM %[3]s@%[4]s LIMIT 0)
	SELECT
		database_name,
		tables.name AS table_name,
		partitions.name AS partition_name,
		partitions.parent_name AS parent_partition,
		partitions.column_names,
		concat(tables.name, '@', table_indexes.index_name) AS index_name,
		coalesce(partitions.list_value, partitions.range_value) as partition_value,
		regexp_extract(config_yaml, e'constraints: (\\[.*\\])') AS zone_constraints
	FROM
		crdb_internal.partitions
		JOIN crdb_internal.table_indexes ON
				partitions.index_id = table_indexes.index_id
				AND partitions.table_id = table_indexes.descriptor_id
		JOIN crdb_internal.tables ON table_indexes.descriptor_id = tables.table_id
		LEFT JOIN crdb_internal.zones ON
				zones.zone_name
				= concat(database_name, '.', tables.name, '.', partitions.name)
	WHERE
		index_name = %[1]s AND tables.name = %[2]s;
	`
	return parse(fmt.Sprintf(showIndexPartitionsQuery,
		lex.EscapeSQLString(n.Index.Index.String()),
		lex.EscapeSQLString(resName.Table()),
		resName.Table(),
		n.Index.Index.String()))
}
