// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
// If nonSensitiveCols is provided, you don't need to define
// customQueryRedacted in the presence of customQueryRedacted.
// In the absence of customQueryRedacted, nonSensitiveCols will
// be used.
type TableRegistryConfig struct {
	// nonSensitiveCols are all the columns associated with the table that do
	// not contain sensitive data.
	// NB: these are required in the absence of customQueryRedacted.
	// NB: the ordering of the columns should match the unredacted column
	// ordering as much as possible, to provide a consistent experience across
	// redacted/unredacted table dumps.
	nonSensitiveCols NonSensitiveColumns
	// customQueryUnredacted is the custom SQL query used to query the
	// table when redaction is not necessary. NB: optional.
	customQueryUnredacted string
	// customQueryUnredacted is the custom SQL query used to query the
	// table when redaction is required.
	// NB: this field is optional, and takes precedence over nonSensitiveCols.
	customQueryRedacted string
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

// QueryForTable produces the appropriate query for `debug zip` for the given
// table to use, taking redaction into account. If the provided tableName does
// not exist in the registry, or no redacted config exists in the registry for
// the tableName, an error is returned.
func (r DebugZipTableRegistry) QueryForTable(tableName string, redact bool) (string, error) {
	tableConfig, ok := r[tableName]
	if !ok {
		return "", errors.Newf("no entry found in table registry for: %s", tableName)
	}
	if !redact {
		if tableConfig.customQueryUnredacted != "" {
			return tableConfig.customQueryUnredacted, nil
		}
		return fmt.Sprintf("TABLE %s", tableName), nil
	}
	if tableConfig.customQueryRedacted != "" {
		return tableConfig.customQueryRedacted, nil
	}
	if len(tableConfig.nonSensitiveCols) == 0 {
		return "", errors.Newf("requested redacted query for table %s, but no non-sensitive columns defined", tableName)
	}
	var colsString strings.Builder
	for i, colName := range tableConfig.nonSensitiveCols {
		// Wrap column names in quotes to escape any identifiers
		colsString.WriteString(colName)
		if i != len(tableConfig.nonSensitiveCols)-1 {
			colsString.WriteString(", ")
		}
	}
	return fmt.Sprintf("SELECT %s FROM %s", colsString.String(), tableName), nil
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
			"status",
			"crdb_internal.hide_sql_constants(stmt) as stmt",
		},
	},
	"crdb_internal.cluster_execution_insights": {
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
			"contention",
			"index_recommendations",
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
		},
	},
	"crdb_internal.cluster_settings": {
		customQueryRedacted: `SELECT * FROM (
    		SELECT * 
    		FROM crdb_internal.cluster_settings
    		WHERE type <> 's'
		) UNION (
		    SELECT variable, '<redacted>' as value, type, public, description
			FROM crdb_internal.cluster_settings g
			WHERE type = 's'
		)`,
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
		},
	},
	"crdb_internal.cluster_txn_execution_insights": {
		// `last_retry_reason` column contains error text that may contain
		// sensitive data.
		nonSensitiveCols: NonSensitiveColumns{
			"txn_id",
			"txn_fingerprint_id",
			"query",
			"implicit_txn",
			"session_id,",
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
		},
	},
	// Ditto, for CREATE TYPE.
	`"".crdb_internal.create_type_statements`: {
		// `create_statement` column contains unredacted SQL statement strings
		// containing customer-supplied enum constants.
		// `enum_members` column contains customer-supplied enum constants.
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"database_name",
			"schema_name",
			"descriptor_id",
			"descriptor_name",
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
			"error",
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
			"last_run",
			"next_run",
			"num_runs",
		},
	},
	"crdb_internal.system_jobs": {
		// `payload` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"status",
			"created",
			"progress",
			"created_by_type",
			"created_by_id",
			"claim_session_id",
			"claim_instance_id",
			"num_runs",
			"last_run",
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
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"network",
			"address",
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
			"args",
			"activity",
		},
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
		// `contending_key` column contains the contended key, which may
		// contain sensitive row-level data.
		nonSensitiveCols: NonSensitiveColumns{
			"collection_ts",
			"blocking_txn_id",
			"blocking_txn_fingerprint_id",
			"waiting_txn_id",
			"waiting_txn_fingerprint_id",
			"contention_duration",
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
			"startts",
			"diff",
			"node_id",
			"range_id",
			"created",
			"range_start",
			"range_end",
			"resolved",
			"last_event_utc",
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
		customQueryRedacted: `SELECT 
			node_id, network, address, advertise_address, sql_network, sql_address, 
			advertise_sql_address, attrs, locality, fnv32(cluster_name) as cluster_name,
			server_version, build_tag, started_at, is_live, ranges, leases
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
			"status",
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
		nonSensitiveCols: NonSensitiveColumns{
			"node_id",
			"component",
			"field",
			"value",
		},
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
			"implicit_txn",
			"full_scan",
			"sample_plan",
			"database_name",
			"exec_node_ids",
			"txn_fingerprint_id",
			"index_recommendations",
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
			"session_id,",
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
}

/**
 * NB: The following system tables explicitly forbidden:
 * 	- system.users: avoid downloading passwords.
 * 	- system.web_sessions: avoid downloading active session tokens.
 * 	- system.join_tokens: avoid downloading secret join keys.
 * 	- system.comments: avoid downloading noise from SQL schema.
 * 	- system.ui: avoid downloading noise from UI customizations.
 * 	- system.zones: the contents of crdb_internal.zones is easier to use.
 * 	- system.statement_bundle_chunks: avoid downloading a large table that's
 *    hard to interpret currently.
 * 	- system.statement_statistics: historical data, usually too much to
 *    download.
 * 	- system.transaction_statistics: ditto
 *
 * A test makes this assertion in pkg/cli/zip_table_registry.go:TestNoForbiddenSystemTablesInDebugZip
 */
var zipSystemTables = DebugZipTableRegistry{
	"system.database_role_settings": {
		nonSensitiveCols: NonSensitiveColumns{
			"database_id",
			"role_name",
			"settings",
		},
	},
	"system.descriptor": {
		// `descriptor` column can contain customer-supplied default values
		// for columns, e.g. `column_name STRING DEFAULT 'some value'`
		nonSensitiveCols: NonSensitiveColumns{"id"},
		customQueryUnredacted: `SELECT
				id,
				descriptor,
				to_hex(descriptor) AS hex_descriptor
			FROM system.descriptor`,
		customQueryRedacted: `SELECT
				id,
				crdb_internal.redact_descriptor(descriptor) AS descriptor,
				to_hex(crdb_internal.redact_descriptor(descriptor)) AS hex_descriptor
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
		// `payload` column may contain customer info, such as URI params
		// containing access keys, encryption salts, etc.
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"status",
			"created",
			"progress",
			"created_by_type",
			"created_by_id",
			"claim_session_id",
			"claim_instance_id",
			"num_runs",
			"last_run",
		},
		customQueryUnredacted: `SELECT *, 
			to_hex(payload) AS hex_payload, 
			to_hex(progress) AS hex_progress 
			FROM system.jobs`,
		customQueryRedacted: `SELECT *,
			NULL AS hex_payload,
			to_hex(progress) AS hex_progress
			FROM system.jobs`,
	},
	"system.lease": {
		nonSensitiveCols: NonSensitiveColumns{
			`"descID"`,
			"version",
			`"nodeID"`,
			"expiration",
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
		customQueryUnredacted: `SELECT *, to_hex(value) as hex_value FROM system.settings`,
		customQueryRedacted: `SELECT * FROM (
    		SELECT *, to_hex(value) as hex_value
    		FROM system.settings
			WHERE "valueType" <> 's'
    	) UNION (
			SELECT name, '<redacted>' as value,
			"lastUpdated",
			"valueType",
			to_hex('redacted') as hex_value
			FROM system.settings
			WHERE "valueType"  = 's'
    	)`,
	},
	"system.span_configurations": {
		nonSensitiveCols: NonSensitiveColumns{
			"config",
			"start_key",
			"end_key",
		},
	},
	"system.sql_instances": {
		nonSensitiveCols: NonSensitiveColumns{
			"id",
			"addr",
			"session_id",
			"locality",
		},
	},
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
		},
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
