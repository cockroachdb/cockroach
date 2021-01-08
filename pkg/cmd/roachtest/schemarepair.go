// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

// registerSchemaRepair registers the roachtest to repair corrupted descriptors.
func registerSchemaRepair(r *testRegistry) {
	r.Add(testSpec{
		Name:  "schemarepair/drop-database-cascade-20.1",
		Owner: OwnerSQLSchema,
		Cluster: clusterSpec{
			NodeCount:   1,
			CPUs:        1,
			ReusePolicy: reusePolicyAny{},
		},
		Timeout: 20 * time.Minute,
		Run:     runSchemaRepair,
	})
}

func runSchemaRepair(ctx context.Context, t *test, c *cluster) {
	const versionWithCorruption = "v20.1.0"
	const cockroachWithCorruption = "cockroach-" + versionWithCorruption
	require.NoError(t, c.Stage(ctx, t.l, "release", versionWithCorruption, ""))
	c.Run(ctx, c.Nodes(1), "cp", "cockroach", cockroachWithCorruption)

	const versionForRepair = "v20.2.3"
	const cockroachForRepair = "cockroach-" + versionForRepair
	require.NoError(t, c.Stage(ctx, t.l, "release", versionForRepair, ""))
	c.Run(ctx, c.Nodes(1), "cp", "cockroach", cockroachForRepair)

	c.Start(ctx, t, c.Nodes(1), startArgs("--binary", cockroachWithCorruption))
	{
		db := c.Conn(ctx, 1)
		_, err := db.Exec(createCorruptionQuery)
		require.NoError(t, err)
	}
	c.Stop(ctx)
	c.Start(ctx, t, c.Nodes(1), startArgs("--binary", cockroachForRepair))
	out, err := c.RunWithBuffer(ctx, c.l, c.Nodes(1), "./"+cockroachForRepair, "debug", "doctor", "cluster", "--insecure")
	// We expect the error to be non-nil.
	require.NotNil(t, err)
	require.Regexp(t, `
   Table  ..: ParentID  .., ParentSchemaID .., Name 'foo': parentID .. does not exist
   Table  ..: ParentID  .., ParentSchemaID .., Name 'v': extra draining names found \[\{ParentID:.. ParentSchemaID:.. Name:v\}\]
   Table  ..: ParentID  .., ParentSchemaID .., Name 'foo': parentID .. does not exist`,
		string(out))

	{
		db := c.Conn(ctx, 1)

		// Ensure that the dry-run query works.
		rows, err := db.Query(dryRunQuery)
		require.NoError(t, err)
		rowStrs, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		require.Len(t, rowStrs, 3)

		rows, err = db.Query(repairQuery)
		require.NoError(t, err)
		repairRowStrs, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		require.Len(t, repairRowStrs, 3)
		for i := range repairRowStrs {
			require.Equal(t, repairRowStrs[i][0], rowStrs[i][0])
		}
	}

	c.Run(ctx, c.Nodes(1), "./"+cockroachForRepair, "debug", "doctor", "cluster", "--insecure")
	{
		db := c.Conn(ctx, 1)
		_, err := db.Exec("DROP DATABASE _crl_repair_to_delete CASCADE;")
		require.NoError(t, err)
	}
	c.Run(ctx, c.Nodes(1), "./"+cockroachForRepair, "debug", "doctor", "cluster", "--insecure")
}

const createCorruptionQuery = `
SET experimental_serial_normalization = 'sql_sequence';

CREATE DATABASE db1;
USE db1;
CREATE TABLE foo (i SERIAL PRIMARY KEY, j SERIAL);
CREATE TABLE parent(i INT PRIMARY KEY);
CREATE TABLE child (i INT PRIMARY KEY REFERENCES parent(i));
CREATE TABLE view_source (i INT PRIMARY KEY);
CREATE VIEW v AS (SELECT i FROM view_source);

CREATE DATABASE db2;
USE db2;
CREATE TABLE foo (i SERIAL PRIMARY KEY, j SERIAL);

USE defaultdb;
DROP DATABASE db1 CASCADE;
DROP DATABASE db2 CASCADE;
`

const repairQuery = `
CREATE DATABASE _crl_repair_to_delete;
WITH new_database_id AS (
                      SELECT id
                        FROM system.namespace
                       WHERE "parentID" = 0
                         AND "parentSchemaID" = 0
                         AND name = '_crl_repair_to_delete'
                     ),
     assert_new_database AS (
                          SELECT crdb_internal.force_error('XXUUU', 'no new database')
                            FROM (
                                  SELECT 1 WHERE NOT EXISTS(SELECT * FROM new_database_id)
                                 )
                         ),
     descs AS (
            SELECT id,
                   crdb_internal.pb_to_json(
                    'cockroach.sql.sqlbase.Descriptor',
                    descriptor
                   ) AS descriptor
              FROM system.descriptor
             WHERE NOT EXISTS(SELECT * FROM assert_new_database)
           ),
     not_orphaned AS (
                    SELECT descs.id AS id
                      FROM descs
                           INNER HASH JOIN system.descriptor AS d ON d.id
                                                                             = (
                                                                                descs.descriptor->'table'->>'parentId'
                                                                              )::INT8
                  ),
     orphaned_tables AS (
                      SELECT id, descriptor
                        FROM descs
                       WHERE descs.id NOT IN (SELECT id FROM not_orphaned)
                         AND descriptor->'table'->>'state' = 'PUBLIC'
                     ),
     orphaned_views AS (
                      SELECT id, descriptor
                        FROM descs
                       WHERE descs.id NOT IN (SELECT id FROM not_orphaned)
                         AND char_length(descriptor->'table'->>'viewQuery') > 0
                         AND id
                             NOT IN (
                                SELECT id::INT8
                                  FROM (
                                        SELECT json_array_elements_text(
                                                payload->'descriptorIds'
                                               ) AS id
                                          FROM (
                                                SELECT id,
                                                       status,
                                                       crdb_internal.pb_to_json(
                                                        'cockroach.sql.jobs.jobspb.Payload',
                                                        payload
                                                       ) AS payload
                                                  FROM system.jobs
                                               )
                                         WHERE (payload->'schemaChange') IS NOT NULL
                                           AND payload->>'description' LIKE 'DROP %'
                                           AND status
                                               IN (
                                                  'pending',
                                                  'running',
                                                  'paused',
                                                  'pause-requested'
                                                )
                                       )
                              )
                    ),
     cols_seqs_removed AS (
                        SELECT id,
                               col
                               || '
                                  {"ownsSequenceIds": [], "usesSequenceIds": []}
                                '::JSONB AS col
                          FROM (
                                SELECT id,
                                       jsonb_array_elements(
                                        descriptor->'table'->'columns'
                                       ) AS col
                                  FROM orphaned_tables
                               )
                       ),
     cols_nextval_replaced AS (
                            SELECT id,
                                   CASE
                                   WHEN col->>'defaultExpr' LIKE '%nextval%'
                                   THEN jsonb_set(
                                    col,
                                    ARRAY['defaultExpr'],
                                    to_json('unique_rowid()')
                                   )
                                   ELSE col
                                   END AS col
                              FROM cols_seqs_removed
                           ),
     updated_columns AS (
                        SELECT id, json_agg(col) AS cols
                          FROM cols_nextval_replaced
                      GROUP BY id
                     ),
     updated_tables AS (
                      SELECT orphaned_tables.id,
                             jsonb_set(
                              descriptor,
                              ARRAY['table'],
                              descriptor->'table'
                              || json_build_object(
                                  'columns',
                                  cols,
                                  'version',
                                  to_json((descriptor->'table'->>'version')::INT8 + 1),
                                  'modificationTime',
                                  json_build_object(),
                                  'parentId',
                                  ARRAY (SELECT id FROM new_database_id)[1],
                                  'name',
                                  'table' || (orphaned_tables.id)::STRING,
                                  'drainingNames',
                                  descriptor->'table'->'drainingNames'
                                  || json_build_object(
                                      'parentId',
                                      descriptor->'table'->'parentId',
                                      'parentSchemaId',
                                      descriptor->'table'->'unexposedParentSchemaId',
                                      'name',
                                      descriptor->'table'->'name'
                                    )
                                )
                             ) AS descriptor
                        FROM orphaned_tables
                        JOIN updated_columns ON orphaned_tables.id = updated_columns.id
                    ),
     upsert_table_descriptors AS (
                                SELECT id,
                                       crdb_internal.unsafe_upsert_descriptor(
                                        id,
                                        crdb_internal.json_to_pb(
                                          'cockroach.sql.sqlbase.Descriptor',
                                          descriptor
                                        )
                                       ) AS upsert_desc
                                  FROM updated_tables
                              ),
     upsert_table_namespace_entries AS (
                                      SELECT id,
                                             crdb_internal.unsafe_upsert_namespace_entry(
                                              (descriptor->'table'->>'parentId')::INT8,
                                              (
                                                descriptor->'table'->>'unexposedParentSchemaId'
                                              )::INT8,
                                              descriptor->'table'->>'name',
                                              id
                                             ) AS upsert_ns
                                        FROM updated_tables
                                    ),
     delete_view_descriptors AS (
                              SELECT id,
                                     crdb_internal.unsafe_delete_namespace_entry(
                                      (descriptor->'table'->>'parentId')::INT8,
                                      (
                                        descriptor->'table'->>'unexposedParentSchemaId'
                                      )::INT8,
                                      descriptor->'table'->>'name',
                                      id
                                     ) AS delete_ns
                                FROM orphaned_views
                             ),
     delete_view_namespace_entries AS (
                                    SELECT id,
                                           crdb_internal.unsafe_delete_descriptor(
                                            id
                                           ) AS delete_desc
                                      FROM orphaned_views
                                   )
     SELECT ud.id, upsert_desc AS desc, upsert_ns AS ns, false AS is_view
       FROM upsert_table_descriptors AS ud, upsert_table_namespace_entries AS uns
      WHERE ud.id = uns.id
     UNION ALL SELECT dd.id, delete_desc, delete_ns, true
                 FROM delete_view_descriptors AS dd, delete_view_namespace_entries AS dns
                WHERE dd.id = dns.id;


`

const dryRunQuery = `
    WITH descs AS (
            SELECT id,
                   crdb_internal.pb_to_json(
                    'cockroach.sql.sqlbase.Descriptor',
                    descriptor
                   ) AS descriptor
              FROM system.descriptor
           ),
     not_orphaned AS (
                    SELECT descs.id AS id
                      FROM descs
                           INNER HASH JOIN system.descriptor AS d ON d.id
                                                                             = (
                                                                                descs.descriptor->'table'->>'parentId'
                                                                              )::INT8
                  ),
     orphaned_tables AS (
                      SELECT id, descriptor
                        FROM descs
                       WHERE descs.id NOT IN (SELECT id FROM not_orphaned)
                         AND descriptor->'table'->>'state' = 'PUBLIC'
                     ),
     orphaned_views AS (
                      SELECT id, descriptor
                        FROM descs
                       WHERE descs.id NOT IN (SELECT id FROM not_orphaned)
                         AND char_length(descriptor->'table'->>'viewQuery') > 0
                         AND id
                             NOT IN (
                                SELECT id::INT8
                                  FROM (
                                        SELECT json_array_elements_text(
                                                payload->'descriptorIds'
                                               ) AS id
                                          FROM (
                                                SELECT id,
                                                       status,
                                                       crdb_internal.pb_to_json(
                                                        'cockroach.sql.jobs.jobspb.Payload',
                                                        payload
                                                       ) AS payload
                                                  FROM system.jobs
                                               )
                                         WHERE (
                                                payload->'schemaChange'
                                               ) IS NOT NULL
                                           AND payload->>'description'
                                               LIKE 'DROP %'
                                           AND status
                                               IN (
                                                  'pending',
                                                  'running',
                                                  'paused',
                                                  'pause-requested'
                                                )
                                       )
                              )
                    ),
    cols_seqs_removed AS (
                        SELECT id,
                               col
                               || '
                                  {"ownsSequenceIds": [], "usesSequenceIds": []}
                                '::JSONB AS col
                          FROM (
                                SELECT id,
                                       jsonb_array_elements(
                                        descriptor->'table'->'columns'
                                       ) AS col
                                  FROM orphaned_tables
                               )
                       ),
     cols_nextval_replaced AS (
                            SELECT id,
                                   CASE
                                   WHEN col->>'defaultExpr' LIKE '%nextval%'
                                   THEN jsonb_set(
                                    col,
                                    ARRAY['defaultExpr'],
                                    to_json('unique_rowid()')
                                   )
                                   ELSE col
                                   END AS col
                              FROM cols_seqs_removed
                           ),
     updated_columns AS (
                        SELECT id, json_agg(col) AS cols
                          FROM cols_nextval_replaced
                      GROUP BY id
                     ),
     updated_tables AS (
                      SELECT orphaned_tables.id,
                             jsonb_set(
                              descriptor,
                              ARRAY['table'],
                              descriptor->'table'
                              || json_build_object(
                                  'columns',
                                  cols,
                                  'version',
                                  to_json((descriptor->'table'->>'version')::INT8 + 1),
                                  'modificationTime',
                                  json_build_object(),
                                  'name',
                                  'table' || (orphaned_tables.id)::STRING,
                                  'drainingNames',
                                  descriptor->'table'->'drainingNames'
                                  || json_build_object(
                                      'parentId',
                                      descriptor->'table'->'parentId',
                                      'parentSchemaId',
                                      descriptor->'table'->'unexposedParentSchemaId',
                                      'name',
                                      descriptor->'table'->'name'
                                    )
                                )
                             ) AS descriptor
                        FROM orphaned_tables
                        JOIN updated_columns ON orphaned_tables.id = updated_columns.id
                    )
     SELECT id, false as view, encode(crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', descriptor), 'hex') FROM updated_tables 
      UNION ALL SELECT id, true, encode(crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', descriptor), 'hex') FROM orphaned_views;`
