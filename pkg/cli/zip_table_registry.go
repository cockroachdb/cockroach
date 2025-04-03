// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// NonSensitiveColumns is a string list used to define table columns that do
// not contain sensitive data.
type NonSensitiveColumns []string

// TableRegistryConfig is the unit of configuration used
// in the DebugZipTableRegistry, providing the option to
// define custom redacted/unredacted queries if necessary.
//
// It allows us to define how a table should be queried when the `--redact`
// flag is passed to `debug zip`, as well as how it should be queried when
// an unredacted debug zip is requested. The goal is to be selective in how
// we query tables in the `--redact` case, so that newly added columns containing
// sensitive information don't accidentally leak into debug zip outputs.
//
// We enforce that *some* explicit config is present for the redacted case.
// In the absence of an explicit configuration for the unredacted case, a
// `SELECT *` query is used.
//
// The following config combinations are acceptable:
// - nonSensitiveCols defined, nothing else.
// - customQueryRedacted defined, nothing else
// - customQueryUnredacted defined AND customQueryRedacted defined
// - customQueryUnredacted defined AND nonSensitiveCols defined
type TableRegistryConfig struct {
	// nonSensitiveCols are all the columns associated with the table that do
	// not contain sensitive data. This is required in the absence of
	// customQueryRedacted.
	nonSensitiveCols NonSensitiveColumns
	// customQueryUnredacted is the custom SQL query used to query the
	// table when redaction is not necessary.
	// If omitted, unredacted debug zips will run a simple `SELECT *` against
	// the table.
	customQueryUnredacted string
	// customQueryUnredacted is the custom SQL query used to query the
	// table when redaction is required. This is required in the absence of
	// nonSensitiveCols.
	//
	// customQueryRedacted should NOT be a `SELECT * FROM table` type query, as
	// this could leak newly added sensitive columns into the output.
	customQueryRedacted string

	// customQueryUnredactedFallback is an alternative query that will be
	// attempted if `customQueryUnredacted` does not return within the
	// timeout or fails. If empty it will be ignored.
	customQueryUnredactedFallback string
	// customQueryRedactedFallback is an alternative query that will be
	// attempted if `customQueryRedacted` does not return within the
	// timeout or fails. If empty it will be ignored.
	customQueryRedactedFallback string
}

// DebugZipTableRegistry is a registry of `crdb_internal` and `system` tables
// that we wish to include in `debug zip` bundles. The registry provides a way
// to clearly define what the redacted form of a table should look like when
// redacted `debug zip` bundles are requested. For now, this redacted query
// is produced by only querying the columns for that table which have been
// explicitly marked in the registry as "non-sensitive", or by using a custom
// redacted query (if defined).
//
// TODO(abarganier): Find a way to push this redaction responsibility into the
// table handlers themselves. Can we perhaps use a session setting to indicate
// to internal table handlers that we wish for the query to be redacted? This
// may be a way to avoid having to completely omit entire columns.
type DebugZipTableRegistry map[string]TableRegistryConfig

// TableQuery holds two sql query strings together that are used to
// dump tables when generating a debug zip. `query` is the primary
// query to run, and `fallback` is one to try if the primary fails or
// times out.
type TableQuery struct {
	query    string
	fallback string
}

// QueryForTable produces the appropriate query for `debug zip` for the given
// table to use, taking redaction into account. If the provided tableName does
// not exist in the registry, or no redacted config exists in the registry for
// the tableName, an error is returned.
func (r DebugZipTableRegistry) QueryForTable(tableName string, redact bool) (TableQuery, error) {
	tableConfig, ok := r[tableName]
	if !ok {
		return TableQuery{}, errors.Newf("no entry found in table registry for: %s", tableName)
	}
	if !redact {
		if tableConfig.customQueryUnredacted != "" {
			return TableQuery{tableConfig.customQueryUnredacted, tableConfig.customQueryUnredactedFallback}, nil
		}
		return TableQuery{fmt.Sprintf("TABLE %s", tableName), ""}, nil
	}
	if tableConfig.customQueryRedacted != "" {
		return TableQuery{tableConfig.customQueryRedacted, tableConfig.customQueryRedactedFallback}, nil
	}
	if len(tableConfig.nonSensitiveCols) == 0 {
		return TableQuery{}, errors.Newf("requested redacted query for table %s, but no non-sensitive columns defined", tableName)
	}
	var colsString strings.Builder
	for i, colName := range tableConfig.nonSensitiveCols {
		// Wrap column names in quotes to escape any identifiers
		colsString.WriteString(colName)
		if i != len(tableConfig.nonSensitiveCols)-1 {
			colsString.WriteString(", ")
		}
	}
	return TableQuery{fmt.Sprintf("SELECT %s FROM %s", colsString.String(), tableName), ""}, nil
}

// GetTables returns all the table names within the registry. Useful for
// iterating the registry in deterministic ways, as map key ordering is
// not guaranteed.
func (r DebugZipTableRegistry) GetTables() []string {
	tables := make([]string, 0, len(r))
	for table := range r {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

var zipInternalTablesPerCluster = DebugZipTableRegistry{
	"crdb_internal.cluster_contention_events": {
		// `key` column contains the contended key, which may contain sensitive
		// row-level data. So, we will only fetch if the table is under the system
		// schema.
		nonSensitiveCols: NonSensitiveColumns{
			"table_id",
			"index_id",
			"IF(crdb_internal.is_system_table_key(key), crdb_internal.pretty_key(key, 0) ,'redacted') as pretty_key",
			"num_contention_events",
			"cumulative_contention_time",
			"txn_id",
			"count",
		},
	},
	"crdb_internal.cluster_database_privileges": {
		nonSensitiveCols: NonSensitiveColumns{
			"database_name",
			"grantee",
			"privilege_type",
			"is_grantable",
		},
	},
	"crdb_internal.cluster_distsql_flows": {
		nonSensitiveCols: NonSensitiveColumns{
			"flow_id",
			"node_id",
			"since",
			"crdb_internal.hide_sql_constants(stmt) as stmt",
		},
	},
	"crdb_internal.cluster_locks": {
		// `lock_key` column contains the txn lock key, which may contain
		// sensitive row-level data.
		// `lock_key_pretty` column contains the pretty-printed txn lock key,
		// which may contain sensitive row-level data.
		nonSensitiveCols: NonSensitiveColumns{
			"range_id",
			"table_id",
			"database_name",
			"schema_name",
			"table_name",
			"index_name",
			"txn_id",
			"ts",
			"lock_strength",
			"durability",
			"granted",
			"contended",
			"duration",
			"isolation_level",
		},
	},
	"crdb_internal.cluster_queries": {
		// `client_address` contains unredacted client IP addresses.
		nonSensitiveCols: NonSensitiveColumns{
			"query_id",
			"txn_id",
			"node_id",
			"session_id",
			"user_name",
			"start",
			"application_name",
			"distributed",
			"phase",
			"full_scan",
			"crdb_internal.hide_sql_constants(query) as query",
		},
	},
	"crdb_internal.cluster_sessions": {
		// `client_address` contains unredacted client IP addresses.
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"session_id",
			"user_name",
			"application_name",
			"num_txns_executed",
			"session_start",
			"active_query_start",
			"kv_txn",
			"alloc_bytes",
			"max_alloc_bytes",
			"status",
			"session_end",
			"crdb_internal.hide_sql_constants(active_queries) as active_queries",
			"crdb_internal.hide_sql_constants(last_active_query) as last_active_query",
			"trace_id",
			"goroutine_id",
		},
	},
	"crdb_internal.cluster_settings": {
		customQueryRedacted: `SELECT
			variable,
			CASE WHEN type = 's' AND value != default_value THEN '<redacted>' ELSE value END value,
			type,
			public,
			description,
			default_value,
			origin
		FROM crdb_internal.cluster_settings`,
	},
	"crdb_internal.probe_ranges_1s_read_limit_100": {
		// At time of writing, it's considered very dangerous to use
		// `write` as the argument to crdb_internal.probe_ranges due to
		// this corruption bug:
		// https://github.com/cockroachdb/cockroach/issues/101549 Since
		// this fix is unevenly distributed in deployments it's not safe to
		// indiscriminately run it from the CLI client on an arbitrary
		// cluster.
		customQueryRedacted:   `SELECT * FROM crdb_internal.probe_ranges(INTERVAL '1000ms', 'read') WHERE error != '' ORDER BY end_to_end_latency_ms DESC LIMIT 100;`,
		customQueryUnredacted: `SELECT * FROM crdb_internal.probe_ranges(INTERVAL '1000ms', 'read') WHERE error != '' ORDER BY end_to_end_latency_ms DESC LIMIT 100;`,
	},
	"crdb_internal.cluster_transactions": {
		// `last_auto_retry_reason` contains error text that may contain
		// sensitive data.
		// `txn_string` contains the transaction key, which may contain
		// sensitive row-level data.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"node_id",
			"session_id",
			"start",
			"application_name",
			"num_stmts",
			"num_retries",
			"num_auto_retries",
			"isolation_level",
			"priority",
			"quality_of_service",
		},
	},
	`"".crdb_internal.create_function_statements`: {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_id",
			"function_id",
			"function_name",
			"crdb_internal.hide_sql_constants(create_statement) as create_statement",
		},
	},
	`"".crdb_internal.create_procedure_statements`: {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_id",
			"procedure_id",
			"procedure_name",
			"crdb_internal.hide_sql_constants(create_statement) as create_statement",
		},
	},
	// The synthetic SQL CREATE statements for all tables.
	// Note the "". to collect across all databases.
	`"".crdb_internal.create_schema_statements`: {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_name",
			"descriptor_id",
			"create_statement",
		},
	},
	`"".crdb_internal.create_statements`: {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_name",
			"descriptor_id",
			"descriptor_type",
			"state",
			"validate_statements",
			"has_partitions",
			"is_multi_region",
			"is_virtual",
			"is_temporary",
			"crdb_internal.hide_sql_constants(create_statement) as create_statement",
			"crdb_internal.hide_sql_constants(alter_statements) as alter_statements",
			"crdb_internal.hide_sql_constants(create_nofks) as create_nofks",
			"crdb_internal.redact(create_redactable) as create_redactable",
		},
	},
	// Ditto, for CREATE TYPE.
	`"".crdb_internal.create_type_statements`: {
		// `enum_members` column contains customer-supplied enum constants.
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_name",
			"descriptor_id",
			"descriptor_name",
			"crdb_internal.hide_sql_constants(create_statement) as create_statement",
		},
	},
	`"".crdb_internal.cluster_replication_spans`: {
		nonSensitiveCols: NonSensitiveColumns{
			"job_id",
			"resolved",
			"resolved_age",
		},
	},
	`"".crdb_internal.logical_replication_spans`: {
		nonSensitiveCols: NonSensitiveColumns{
			"job_id",
			"resolved",
			"resolved_age",
		},
	},
	"crdb_internal.default_privileges": {
		nonSensitiveCols: NonSensitiveColumns{
			"database_name",
			"schema_name",
			"role",
			"for_all_roles",
			"object_type",
			"grantee",
			"privilege_type",
			"is_grantable",
		},
	},
	"crdb_internal.index_usage_statistics": {
		nonSensitiveCols: NonSensitiveColumns{
			"table_id",
			"index_id",
			"total_reads",
			"last_read",
		},
	},
	"crdb_internal.invalid_objects": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"database_name",
			"schema_name",
			"obj_name",
			"crdb_internal.redact(error_redactable) as error_redactable",
		},
	},
	// `statement` column can contain customer URI params such as
	// AWS_ACCESS_KEY_ID.
	// `error`, `execution_errors`, and `execution_events` columns contain
	// error text that may contain sensitive data.
	"crdb_internal.jobs": {
		nonSensitiveCols: NonSensitiveColumns{
			"job_id",
			"job_type",
			"description",
			"user_name",
			"descriptor_ids",
			"status",
			"running_status",
			"created",
			"started",
			"finished",
			"modified",
			"fraction_completed",
			"high_water_timestamp",
			"coordinator_id",
			"trace_id",
		},
	},
	"crdb_internal.system_jobs": {
		// `payload` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		customQueryUnredacted: `SELECT *
			FROM crdb_internal.system_jobs`,
		customQueryRedacted: `SELECT 
			"id",
			"status",
			"created",
			'redacted' AS "payload",
			"progress",
			"created_by_type",
			"created_by_id",
			"claim_session_id",
			"claim_instance_id",
			"num_runs",
			"last_run"
			FROM crdb_internal.system_jobs`,
	},
	"crdb_internal.kv_system_privileges": {
		nonSensitiveCols: NonSensitiveColumns{
			"username",
			"path",
			"privileges",
			"grant_options",
			"user_id",
		},
	},
	"crdb_internal.kv_node_liveness": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"epoch",
			"expiration",
			"draining",
			"membership",
		},
	},
	"crdb_internal.kv_node_status": {
		// `env` column can contain sensitive node environment variable values,
		// such as AWS_ACCESS_KEY.
		// Some fields are marked as `<redacted>` because we want to redact hostname, ip address and other sensitive fields
		// in the db dump files contained in debugzip
		customQueryRedacted: `SELECT 
				"node_id",
				"network",
				'<redacted>' as address,
				"attrs",
				"locality",
				"server_version",
				"go_version",
				"tag",
				"time",
				"revision",
				"cgo_compiler",
				"platform",
				"distribution",
				"type",
				"dependencies",
				"started_at",
				"updated_at",
				"metrics",
				'<redacted>' as args,
				'<redacted>' as env,
				"activity"
			FROM crdb_internal.kv_node_status
		`,
	},
	"crdb_internal.kv_store_status": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"store_id",
			"attrs",
			"capacity",
			"available",
			"used",
			"logical_bytes",
			"range_count",
			"lease_count",
			"writes_per_second",
			"bytes_per_replica",
			"writes_per_replica",
			"metrics",
			"properties",
		},
	},
	"crdb_internal.partitions": {
		// `list_value` and `range_value` columns contain PARTITION BY
		// statements, which contain partition constants.
		nonSensitiveCols: NonSensitiveColumns{
			"table_id",
			"index_id",
			"parent_name",
			"name",
			"columns",
			"column_names",
			"zone_id",
			"subzone_id",
		},
	},
	"crdb_internal.regions": {
		nonSensitiveCols: NonSensitiveColumns{
			"region",
			"zones",
		},
	},
	"crdb_internal.schema_changes": {
		nonSensitiveCols: NonSensitiveColumns{
			"table_id",
			"parent_id",
			"name",
			"type",
			"target_id",
			"target_name",
			"state",
			"direction",
		},
	},
	"crdb_internal.super_regions": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"database_name",
			"super_region_name",
			"regions",
		},
	},
	"crdb_internal.kv_protected_ts_records": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"ts",
			"meta_type",
			"meta",
			"num_spans",
			"spans",
			"verified",
			"target",
			"decoded_meta",
			"decoded_target",
			// Internal meta may contain sensitive data such as usernames.
			// "internal_meta",
			"num_ranges",
			"last_updated",
		},
	},
	"crdb_internal.table_indexes": {
		nonSensitiveCols: NonSensitiveColumns{
			"descriptor_id",
			"descriptor_name",
			"index_id",
			"index_name",
			"index_type",
			"is_unique",
			"is_inverted",
			"is_sharded",
			"is_visible",
			"shard_bucket_count",
			"created_at",
		},
	},
	"crdb_internal.transaction_contention_events": {
		customQueryUnredacted: `
with fingerprint_queries as (
	SELECT distinct fingerprint_id, metadata->> 'query' as query  
	FROM system.statement_statistics
),
transaction_fingerprints as (
		SELECT distinct fingerprint_id, transaction_fingerprint_id
		FROM system.statement_statistics
), 
transaction_queries as (
		SELECT tf.transaction_fingerprint_id, array_agg(fq.query) as queries
		FROM fingerprint_queries fq
		JOIN transaction_fingerprints tf on tf.fingerprint_id = fq.fingerprint_id
		GROUP BY tf.transaction_fingerprint_id
)
SELECT collection_ts,
       contention_duration,
       waiting_txn_id,
       waiting_txn_fingerprint_id,
       waiting_stmt_fingerprint_id,
       fq.query                      AS waiting_stmt_query,
       blocking_txn_id,
       blocking_txn_fingerprint_id,
       tq.queries AS blocking_txn_queries_unordered,
       contending_pretty_key,
       index_name,
       table_name,
       database_name
FROM crdb_internal.transaction_contention_events
LEFT JOIN fingerprint_queries fq ON fq.fingerprint_id = waiting_stmt_fingerprint_id
LEFT JOIN transaction_queries tq ON tq.transaction_fingerprint_id = blocking_txn_fingerprint_id
WHERE fq.fingerprint_id != '\x0000000000000000' AND tq.transaction_fingerprint_id != '\x0000000000000000'
`,
		customQueryUnredactedFallback: `
SELECT collection_ts,
       contention_duration,
       waiting_txn_id,
       waiting_txn_fingerprint_id,
       waiting_stmt_fingerprint_id,
       blocking_txn_id,
       blocking_txn_fingerprint_id,
       contending_pretty_key,
       index_name,
       table_name,
       database_name
FROM crdb_internal.transaction_contention_events
`,
		// `contending_key` column contains the contended key, which may
		// contain sensitive row-level data. So, we will only fetch if the
		// table is under the system schema.
		nonSensitiveCols: NonSensitiveColumns{
			"collection_ts",
			"blocking_txn_id",
			"blocking_txn_fingerprint_id",
			"waiting_txn_id",
			"waiting_txn_fingerprint_id",
			"contention_duration",
			"IF(crdb_internal.is_system_table_key(contending_key), crdb_internal.pretty_key(contending_key, 0) ,'redacted') as contending_pretty_key",
			"contention_type",
		},
	},
	"crdb_internal.zones": {
		nonSensitiveCols: NonSensitiveColumns{
			"zone_id",
			"subzone_id",
			"target",
			"range_name",
			"database_name",
			"schema_name",
			"table_name",
			"index_name",
			"partition_name",
			"raw_config_yaml",
			"raw_config_sql",
			"raw_config_protobuf",
			"full_config_yaml",
			"full_config_sql",
		},
	},
}

var zipInternalTablesPerNode = DebugZipTableRegistry{
	"crdb_internal.active_range_feeds": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"tags",
			"start_after",
			"diff",
			"node_id",
			"range_id",
			"created",
			"range_start",
			"range_end",
			"resolved",
			"resolved_age",
			"last_event",
			"catchup",
		},
	},
	"crdb_internal.feature_usage": {
		nonSensitiveCols: NonSensitiveColumns{
			"feature_name",
			"usage_count",
		},
	},
	"crdb_internal.gossip_alerts": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"store_id",
			"category",
			"description",
			"value",
		},
	},
	"crdb_internal.gossip_liveness": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"epoch",
			"expiration",
			"draining",
			"decommissioning",
			"membership",
			"updated_at",
		},
	},
	"crdb_internal.gossip_nodes": {
		// `cluster_name` is hashed as we only care to see whether values are
		// identical across nodes.
		// Some fields are marked as `<redacted>` because we want to redact hostname, ip address and other sensitive fields
		// in the db dump files contained in debugzip
		customQueryRedacted: `SELECT 
				node_id, 
				network, 
				'<redacted>' as address, 
				'<redacted>' as advertise_address, 
				sql_network, 
				'<redacted>' as sql_address, 
				'<redacted>' as advertise_sql_address, 
				attrs, 
				'<redacted>' as locality, 
				fnv32(cluster_name) as cluster_name,
				server_version, 
				build_tag, 
				started_at, 
				is_live, 
				ranges,
				leases
			FROM crdb_internal.gossip_nodes`,
	},
	"crdb_internal.leases": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"table_id",
			"name",
			"parent_id",
			"expiration",
			"deleted",
		},
	},
	"crdb_internal.kv_session_based_leases": {
		nonSensitiveCols: NonSensitiveColumns{
			"desc_id",
			"version",
			"sql_instance_id",
			"session_id",
			"crdb_region",
		},
	},
	"crdb_internal.node_build_info": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"field",
			"value",
		},
	},
	"crdb_internal.node_contention_events": {
		// `key` column contains the contended key, which may contain sensitive
		// row-level data.
		nonSensitiveCols: NonSensitiveColumns{
			"table_id",
			"index_id",
			"num_contention_events",
			"cumulative_contention_time",
			"txn_id",
			"count",
		},
	},
	"crdb_internal.node_distsql_flows": {
		nonSensitiveCols: NonSensitiveColumns{
			"flow_id",
			"node_id",
			"stmt",
			"since",
			"crdb_internal.hide_sql_constants(stmt) as stmt",
		},
	},
	"crdb_internal.node_execution_insights": {
		// `last_retry_reason` column contains error text that may contain
		// sensitive data.
		nonSensitiveCols: NonSensitiveColumns{
			"session_id",
			"txn_id",
			"txn_fingerprint_id",
			"stmt_id",
			"stmt_fingerprint_id",
			"query",
			"status",
			"start_time",
			"end_time",
			"full_scan",
			"user_name",
			"app_name",
			"database_name",
			"plan_gist",
			"rows_read",
			"rows_written",
			"priority",
			"retries",
			"exec_node_ids",
			"kv_node_ids",
			"error_code",
			"crdb_internal.redact(last_error_redactable) as last_error_redactable",
		},
	},
	"crdb_internal.node_inflight_trace_spans": {
		customQueryUnredacted: `WITH spans AS (
			SELECT * FROM crdb_internal.node_inflight_trace_spans
			WHERE duration > INTERVAL '10' ORDER BY trace_id ASC, duration DESC
		) SELECT * FROM spans, LATERAL crdb_internal.payloads_for_span(span_id)`,
		// `payload_jsonb` column is an `Any` type, meaning it can contain
		// customer data and is high risk for new types leaking sensitive
		// data into the payload.
		customQueryRedacted: `WITH spans AS (                 
			SELECT * FROM crdb_internal.node_inflight_trace_spans
			WHERE duration > INTERVAL '10' ORDER BY trace_id ASC, duration DESC
		) SELECT trace_id, parent_span_id, span_id, goroutine_id, finished, start_time, duration, operation, payload_type 
		FROM spans, LATERAL crdb_internal.payloads_for_span(span_id)`,
	},
	"crdb_internal.node_memory_monitors": {
		nonSensitiveCols: NonSensitiveColumns{
			"level",
			"name",
			"id",
			"parent_id",
			"used",
			"reserved_used",
			"reserved_reserved",
			"stopped",
		},
	},
	"crdb_internal.node_metrics": {
		nonSensitiveCols: NonSensitiveColumns{
			"store_id",
			"name",
			"value",
		},
	},
	"crdb_internal.node_queries": {
		// `client_address` contains unredacted client IP addresses.
		nonSensitiveCols: NonSensitiveColumns{
			"query_id",
			"txn_id",
			"node_id",
			"session_id",
			"user_name",
			"start",
			"application_name",
			"distributed",
			"phase",
			"full_scan",
			"crdb_internal.hide_sql_constants(query) as query",
		},
	},
	"crdb_internal.node_runtime_info": {
		// Some fields are marked as `<redacted>` because we want to redact hostname, ip address and other sensitive fields
		// in the db dump files contained in debugzip
		customQueryRedacted: `SELECT * FROM (
			SELECT
				"node_id",
				"component",
				"field",
				"value"
			FROM crdb_internal.node_runtime_info 
      WHERE field NOT IN ('URL', 'Host', 'URI') UNION
    		SELECT
					"node_id",
					"component",
					"field",
					'<redacted>' AS value
				FROM crdb_internal.node_runtime_info
				WHERE field IN ('URL', 'Host', 'URI')
      ) ORDER BY node_id`,
	},
	"crdb_internal.node_sessions": {
		// `client_address` contains unredacted client IP addresses.
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"session_id",
			"user_name",
			"application_name",
			"num_txns_executed",
			"session_start",
			"active_query_start",
			"kv_txn",
			"alloc_bytes",
			"max_alloc_bytes",
			"status",
			"session_end",
			"crdb_internal.hide_sql_constants(active_queries) as active_queries",
			"crdb_internal.hide_sql_constants(last_active_query) as last_active_query",
			"trace_id",
			"goroutine_id",
		},
	},
	"crdb_internal.node_statement_statistics": {
		// `last_error` column contain error text that may contain sensitive
		// data.
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"application_name",
			"flags",
			"statement_id",
			"key",
			"anonymized",
			"count",
			"first_attempt_count",
			"max_retries",
			"rows_avg",
			"rows_var",
			"parse_lat_avg",
			"parse_lat_var",
			"run_lat_avg",
			"run_lat_var",
			"service_lat_avg",
			"service_lat_var",
			"overhead_lat_avg",
			"overhead_lat_var",
			"bytes_read_avg",
			"bytes_read_var",
			"rows_read_avg",
			"rows_read_var",
			"network_bytes_avg",
			"network_bytes_var",
			"network_msgs_avg",
			"network_msgs_var",
			"max_mem_usage_avg",
			"max_mem_usage_var",
			"max_disk_usage_avg",
			"max_disk_usage_var",
			"contention_time_avg",
			"contention_time_var",
			"cpu_sql_nanos_avg",
			"cpu_sql_nanos_var",
			"mvcc_step_avg",
			"mvcc_step_var",
			"mvcc_step_internal_avg",
			"mvcc_step_internal_var",
			"mvcc_seek_avg",
			"mvcc_seek_var",
			"mvcc_seek_internal_avg",
			"mvcc_seek_internal_var",
			"mvcc_block_bytes_avg",
			"mvcc_block_bytes_var",
			"mvcc_block_bytes_in_cache_avg",
			"mvcc_block_bytes_in_cache_var",
			"mvcc_key_bytes_avg",
			"mvcc_key_bytes_var",
			"mvcc_value_bytes_avg",
			"mvcc_value_bytes_var",
			"mvcc_point_count_avg",
			"mvcc_point_count_var",
			"mvcc_points_covered_by_range_tombstones_avg",
			"mvcc_points_covered_by_range_tombstones_var",
			"mvcc_range_key_count_avg",
			"mvcc_range_key_count_var",
			"mvcc_range_key_contained_points_avg",
			"mvcc_range_key_contained_points_var",
			"mvcc_range_key_skipped_points_avg",
			"mvcc_range_key_skipped_points_var",
			"implicit_txn",
			"full_scan",
			"sample_plan",
			"database_name",
			"exec_node_ids",
			"kv_node_ids",
			"used_follower_read",
			"txn_fingerprint_id",
			"index_recommendations",
			"latency_seconds_min",
			"latency_seconds_max",
		},
	},
	"crdb_internal.node_transaction_statistics": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"application_name",
			"key",
			"statement_ids",
			"count",
			"max_retries",
			"service_lat_avg",
			"service_lat_var",
			"retry_lat_avg",
			"retry_lat_var",
			"commit_lat_avg",
			"commit_lat_var",
			"rows_read_avg",
			"rows_read_var",
			"network_bytes_avg",
			"network_bytes_var",
			"network_msgs_avg",
			"network_msgs_var",
			"max_mem_usage_avg",
			"max_mem_usage_var",
			"max_disk_usage_avg",
			"max_disk_usage_var",
			"contention_time_avg",
			"contention_time_var",
			"cpu_sql_nanos_avg",
			"cpu_sql_nanos_var",
			"mvcc_step_avg",
			"mvcc_step_var",
			"mvcc_step_internal_avg",
			"mvcc_step_internal_var",
			"mvcc_seek_avg",
			"mvcc_seek_var",
			"mvcc_seek_internal_avg",
			"mvcc_seek_internal_var",
			"mvcc_block_bytes_avg",
			"mvcc_block_bytes_var",
			"mvcc_block_bytes_in_cache_avg",
			"mvcc_block_bytes_in_cache_var",
			"mvcc_key_bytes_avg",
			"mvcc_key_bytes_var",
			"mvcc_value_bytes_avg",
			"mvcc_value_bytes_var",
			"mvcc_point_count_avg",
			"mvcc_point_count_var",
			"mvcc_points_covered_by_range_tombstones_avg",
			"mvcc_points_covered_by_range_tombstones_var",
			"mvcc_range_key_count_avg",
			"mvcc_range_key_count_var",
			"mvcc_range_key_contained_points_avg",
			"mvcc_range_key_contained_points_var",
			"mvcc_range_key_skipped_points_avg",
			"mvcc_range_key_skipped_points_var",
		},
	},
	"crdb_internal.node_transactions": {
		// `last_auto_retry_reason` column contains error text that may contain
		// sensitive data.
		// `txn_string` contains the transaction key, which may contain
		// sensitive row-level data.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"node_id",
			"session_id",
			"start",
			"application_name",
			"num_stmts",
			"num_retries",
			"num_auto_retries",
		},
	},
	"crdb_internal.node_txn_execution_insights": {
		// `last_retry_reason` column contains error text that may contain
		// sensitive data.
		nonSensitiveCols: NonSensitiveColumns{
			"txn_id",
			"txn_fingerprint_id",
			"query",
			"implicit_txn",
			"session_id",
			"start_time",
			"end_time",
			"user_name",
			"app_name",
			"rows_read",
			"rows_written",
			"priority",
			"retries",
			"contention",
			"problems",
			"causes",
			"stmt_execution_ids",
			"last_error_code",
			"crdb_internal.redact(last_error_redactable) as last_error_redactable",
		},
	},
	"crdb_internal.node_txn_stats": {
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"application_name",
			"txn_count",
			"txn_time_avg_sec",
			"txn_time_var_sec",
			"committed_count",
			"implicit_count",
		},
	},
	"crdb_internal.node_tenant_capabilities_cache": {
		nonSensitiveCols: NonSensitiveColumns{
			"tenant_id",
			"capability_name",
			"capability_value",
		},
	},
	"crdb_internal.cluster_replication_node_streams": {
		nonSensitiveCols: NonSensitiveColumns{
			"stream_id",
			"consumer",
			"spans",
			"state",
			"read",
			"emit",
			"last_read_ms",
			"last_emit_ms",
			"seq",
			"chkpts",
			"last_chkpt",
			"batches",
			"megabytes",
			"last_kb",
			"rf_chk",
			"rf_adv",
			"rf_last_adv",
			"resolved_age",
		},
	},
	"crdb_internal.cluster_replication_node_stream_spans": {
		nonSensitiveCols: NonSensitiveColumns{
			"stream_id",
			"consumer",
		},
	},
	"crdb_internal.cluster_replication_node_stream_checkpoints": {
		nonSensitiveCols: NonSensitiveColumns{
			"stream_id",
			"consumer",
			"resolved",
			"resolved_age",
		},
	},
	"crdb_internal.logical_replication_node_processors": {
		nonSensitiveCols: NonSensitiveColumns{
			"stream_id",
			"consumer",
			"state",
			"recv_time",
			"last_recv_time",
			"ingest_time",
			"flush_time",
			"flush_count",
			"flush_kvs",
			"flush_bytes",
			"flush_batches",
			"last_flush_time",
			"chunks_running",
			"chunks_done",
			"last_kvs_done",
			"last_kvs_todo",
			"last_batches",
			"last_slowest",
			"checkpoints",
			"retry_size",
			"resolved_age",
		},
	},
}

// NB: The following system tables explicitly forbidden:
//   - system.users: avoid downloading passwords.
//   - system.web_sessions: avoid downloading active session tokens.
//   - system.join_tokens: avoid downloading secret join keys.
//   - system.comments: avoid downloading noise from SQL schema.
//   - system.ui: avoid downloading noise from UI customizations.
//   - system.zones: the contents of crdb_internal.zones is easier to use.
//   - system.statement_bundle_chunks: avoid downloading a large table that's
//     hard to interpret currently.
//   - system.statement_statistics: historical data, usually too much to
//     download.
//   - system.transaction_statistics: ditto
//   - system.statement_activity: ditto
//   - system.transaction_activity: ditto
//
// A test makes this assertion in pkg/cli/zip_table_registry.go:TestNoForbiddenSystemTablesInDebugZip
var zipSystemTables = DebugZipTableRegistry{
	"system.database_role_settings": {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"role_name",
			"settings",
		},
	},
	"system.descriptor": {
		customQueryUnredacted: `SELECT
				id,
				descriptor
			FROM system.descriptor`,
		customQueryRedacted: `SELECT
				id,
				crdb_internal.redact_descriptor(descriptor) AS descriptor
			FROM system.descriptor`,
	},
	"system.eventlog": {
		nonSensitiveCols: NonSensitiveColumns{
			"timestamp",
			`"eventType"`,
			`"targetID"`,
			`"reportingID"`,
			`"uniqueID"`,
		},
	},
	"system.external_connections": {
		// `connection_details` column may contain customer infra IP addresses,
		// URI params containing access keys, etc.
		nonSensitiveCols: NonSensitiveColumns{
			"connection_name",
			"created",
			"updated",
			"connection_type",
		},
	},
	"system.jobs": {
		// NB: `payload` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		customQueryUnredacted: `SELECT * 
			FROM system.jobs`,
		customQueryRedacted: `SELECT id,
			status,
			created,
			created_by_type,
			created_by_id,
			claim_session_id,
			claim_instance_id,
			num_runs,
			last_run
			FROM system.jobs`,
	},
	"system.job_info": {
		// `value` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		customQueryUnredacted: `SELECT *
			FROM system.job_info`,
		customQueryRedacted: `SELECT job_id,
			info_key,
			written,
			'redacted' AS value
			FROM system.job_info`,
	},
	"system.job_progress": {
		nonSensitiveCols: NonSensitiveColumns{"job_id", "written", "fraction", "resolved"},
	},
	"system.job_progress_history": {
		nonSensitiveCols: NonSensitiveColumns{"job_id", "written", "fraction", "resolved"},
	},
	"system.job_status": {
		nonSensitiveCols: NonSensitiveColumns{"job_id", "written", "status"},
	},
	"system.job_message": {
		nonSensitiveCols: NonSensitiveColumns{"job_id", "written", "kind", "message"},
	},
	"system.lease": {
		nonSensitiveCols: NonSensitiveColumns{
			"desc_id",
			"version",
			"sql_instance_id",
			"session_id",
			"crdb_region",
		},
	},
	"system.locations": {
		nonSensitiveCols: NonSensitiveColumns{
			`"localityKey"`,
			`"localityValue"`,
			"latitude",
			"longitude",
		},
	},
	"system.migrations": {
		nonSensitiveCols: NonSensitiveColumns{
			"major",
			"minor",
			"patch",
			"internal",
			"completed_at",
		},
	},
	"system.namespace": {
		nonSensitiveCols: NonSensitiveColumns{
			`"parentID"`,
			`"parentSchemaID"`,
			"name",
			"id",
		},
	},
	"system.privileges": {
		nonSensitiveCols: NonSensitiveColumns{
			"username",
			"path",
			"privileges",
			"grant_options",
		},
	},
	"system.protected_ts_meta": {
		nonSensitiveCols: NonSensitiveColumns{
			"singleton",
			"version",
			"num_records",
			"num_spans",
			"total_bytes",
		},
	},
	"system.protected_ts_records": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"ts",
			"meta_type",
			"meta",
			"num_spans",
			"spans",
			"verified",
			"target",
		},
	},
	"system.rangelog": {
		nonSensitiveCols: NonSensitiveColumns{
			"timestamp",
			`"rangeID"`,
			`"storeID"`,
			`"eventType"`,
			`"otherRangeID"`,
			"info",
			`"uniqueID"`,
		},
	},
	"system.replication_constraint_stats": {
		nonSensitiveCols: NonSensitiveColumns{
			"zone_id",
			"subzone_id",
			"type",
			"config",
			"report_id",
			"violation_start",
			"violating_ranges",
		},
	},
	"system.replication_critical_localities": {
		nonSensitiveCols: NonSensitiveColumns{
			"zone_id",
			"subzone_id",
			"locality",
			"report_id",
			"at_risk_ranges",
		},
	},
	"system.replication_stats": {
		nonSensitiveCols: NonSensitiveColumns{
			"zone_id",
			"subzone_id",
			"report_id",
			"total_ranges",
			"unavailable_ranges",
			"under_replicated_ranges",
			"over_replicated_ranges",
		},
	},
	"system.reports_meta": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"generated",
		},
	},
	"system.role_id_seq": {
		nonSensitiveCols: NonSensitiveColumns{
			"last_value",
			"log_cnt",
			"is_called",
		},
	},
	"system.role_members": {
		nonSensitiveCols: NonSensitiveColumns{
			"role",
			"member",
			`"isAdmin"`,
		},
	},
	"system.role_options": {
		nonSensitiveCols: NonSensitiveColumns{
			"username",
			"option",
			"value",
		},
	},
	"system.scheduled_jobs": {
		// `execution_args` column contains BACKUP statements which can contain
		// sensitive URI params, such as AWS keys.
		nonSensitiveCols: NonSensitiveColumns{
			"schedule_id",
			"schedule_name",
			"created",
			"owner",
			"next_run",
			"schedule_state",
			"schedule_expr",
			"schedule_details",
			"executor_type",
		},
	},
	"system.settings": {
		customQueryUnredacted: `SELECT * FROM system.settings`,
		customQueryRedacted: `SELECT * FROM (
				SELECT *
				FROM system.settings
				WHERE "valueType" <> 's'
    	) UNION (
				SELECT name, '<redacted>' as value,
				"lastUpdated",
				"valueType"
				FROM system.settings
				WHERE "valueType"  = 's'
    	)`,
	},
	"system.span_configurations": {
		nonSensitiveCols: NonSensitiveColumns{
			"config",
			// Boundary keys for span configs, which are derived from zone configs, are typically on
			// metadata object boundaries (database, table, or index), and not arbitrary range boundaries
			// and therefore do not contain sensitive information. Therefore they can remain unredacted.
			"start_key",
			// Boundary keys for span configs, which are derived from zone configs, are typically on
			// metadata object boundaries (database, table, or index), and not arbitrary range boundaries
			// and therefore do not contain sensitive information. Therefore they can remain unredacted.
			"end_key",
		},
	},
	"system.sql_instances": {
		// Some fields are marked as `<redacted>` because we want to redact hostname, ip address and other sensitive fields
		// in the db dump files contained in debugzip
		customQueryRedacted: `SELECT 
			"id",
			'<redacted>' as addr,
			"session_id",
			'<redacted>' as locality,
			'<redacted>' as sql_addr
			FROM system.sql_instances
		`,
	},
	// system.sql_stats_cardinality shows row counts for all of the system tables related to the SQL Stats
	// system, grouped by aggregated timestamp. None of this information is sensitive. It aids in escalations
	// involving the SQL Stats system.
	"system.sql_stats_cardinality": func() TableRegistryConfig {
		query := `
			SELECT table_name, aggregated_ts, row_count
			FROM (
					SELECT 'system.statement_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_statistics' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_statistics
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.statement_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.statement_activity
					GROUP BY aggregated_ts
				UNION
					SELECT 'system.transaction_activity' AS table_name, aggregated_ts, count(*) AS row_count
					FROM system.transaction_activity
					GROUP BY aggregated_ts
			)
			ORDER BY table_name, aggregated_ts DESC;`
		return TableRegistryConfig{
			customQueryUnredacted: query,
			customQueryRedacted:   query,
		}
	}(),
	"system.sqlliveness": {
		nonSensitiveCols: NonSensitiveColumns{
			"session_id",
			"expiration",
		},
	},
	"system.statement_diagnostics": {
		// `bundle_chunks` column contains diagnostic bundle bytes, which
		// contain unredacted information such as SQL arguments and
		// unredacted trace logs.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"statement_fingerprint",
			"collected_at",
			"statement",
			"error",
		},
	},
	"system.statement_diagnostics_requests": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"completed",
			"statement_fingerprint",
			"statement_diagnostics_id",
			"requested_at",
			"min_execution_latency",
			"expires_at",
			"sampling_probability",
			"plan_gist",
			"anti_plan_gist",
			"redacted",
		},
	},
	// statement_statistics can have over 100k rows in just the last hour.
	// Select all the statements that are part of a transaction where one of
	// the statements is in the 100 by cpu usage, and all the transaction
	// fingerprints from the transaction_contention_events table to keep the
	// number of rows limited.
	"system.statement_statistics_limit_5000": {
		customQueryRedacted: `SELECT max(ss.aggregated_ts),
       ss.fingerprint_id,
       ss.transaction_fingerprint_id,
       ss.plan_hash,
       ss.app_name,
       ss.agg_interval,
       merge_stats_metadata(ss.metadata)    AS metadata,
       merge_statement_stats(ss.statistics) AS statistics,
       ss.plan,
       ss.index_recommendations
     FROM system.public.statement_statistics ss
     WHERE aggregated_ts > (now() - INTERVAL '1 hour') AND
 ( transaction_fingerprint_id in (SELECT DISTINCT(blocking_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      union
                                      SELECT DISTINCT(waiting_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      )
    OR transaction_fingerprint_id in (SELECT ss_cpu.transaction_fingerprint_id
                                      FROM system.public.statement_statistics ss_cpu
                                      group by ss_cpu.transaction_fingerprint_id, ss_cpu.cpu_sql_nanos
                                      ORDER BY ss_cpu.cpu_sql_nanos desc limit 100))
GROUP BY ss.aggregated_ts,
         ss.app_name,
         ss.fingerprint_id,
         ss.transaction_fingerprint_id,
         ss.plan_hash,
         ss.agg_interval,
         ss.plan,
         ss.index_recommendations
limit 5000;`,
		customQueryUnredacted: `SELECT max(ss.aggregated_ts),
       ss.fingerprint_id,
       ss.transaction_fingerprint_id,
       ss.plan_hash,
       ss.app_name,
       ss.agg_interval,
       merge_stats_metadata(ss.metadata)    AS metadata,
       merge_statement_stats(ss.statistics) AS statistics,
       ss.plan,
       ss.index_recommendations
     FROM system.public.statement_statistics ss
     WHERE aggregated_ts > (now() - INTERVAL '1 hour') AND
 ( transaction_fingerprint_id in (SELECT DISTINCT(blocking_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      union
                                      SELECT DISTINCT(waiting_txn_fingerprint_id)
                                      FROM crdb_internal.transaction_contention_events
                                      WHERE blocking_txn_fingerprint_id != '\x0000000000000000'
                                      )
    OR transaction_fingerprint_id in (SELECT ss_cpu.transaction_fingerprint_id
                                      FROM system.public.statement_statistics ss_cpu
                                      group by ss_cpu.transaction_fingerprint_id, ss_cpu.cpu_sql_nanos
                                      ORDER BY ss_cpu.cpu_sql_nanos desc limit 100))
GROUP BY ss.aggregated_ts,
         ss.app_name,
         ss.fingerprint_id,
         ss.transaction_fingerprint_id,
         ss.plan_hash,
         ss.agg_interval,
         ss.plan,
         ss.index_recommendations
limit 5000;`,
	},
	"system.table_statistics": {
		// `histogram` may contain sensitive information, such as keys and non-key column data.
		nonSensitiveCols: NonSensitiveColumns{
			`"tableID"`,
			`"statisticID"`,
			"name",
			`"columnIDs"`,
			`"createdAt"`,
			`"rowCount"`,
			`"distinctCount"`,
			`"nullCount"`,
			`"avgSize"`,
		},
	},
	"system.task_payloads": {
		// `value` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		// `description` is user-defined and may contain PII.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"created",
			"owner",
			"owner_id",
			"min_version",
			"type",
		},
	},
	"system.tenant_tasks": {
		nonSensitiveCols: NonSensitiveColumns{
			"tenant_id",
			"issuer",
			"task_id",
			"created",
			"payload_id",
			"owner",
			"owner_id",
		},
	},
	"system.tenant_settings": {
		customQueryRedacted: `SELECT * FROM (
			SELECT *
			FROM system.tenant_settings
			WHERE value_type <> 's'
    	) UNION (
			SELECT tenant_id, name, '<redacted>' as value, last_updated, value_type, reason
			FROM system.tenant_settings
			WHERE value_type = 's'
    	)`,
	},
	"system.tenant_usage": {
		nonSensitiveCols: NonSensitiveColumns{
			"tenant_id",
			"instance_id",
			"next_instance_id",
			"last_update",
			"ru_burst_limit",
			"ru_refill_rate",
			"ru_current",
			"current_share_sum",
			"total_consumption",
			"instance_seq",
			"instance_shares",
			"instance_lease",
		},
	},
	"system.tenants": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"active",
			"info",
		},
	},
}
