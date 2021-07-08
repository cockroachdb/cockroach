// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import "github.com/cockroachdb/cockroach/pkg/settings"

// DummyVars contains a list of dummy vars we do not support that
// PostgreSQL does, but are required as an easy fix to make certain
// tooling/ORMs work. These vars should not affect the correctness
// of results.
var DummyVars = map[string]sessionVar{
	"enable_seqscan": makeDummyBooleanSessionVar(
		"enable_seqscan",
		func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.EnableSeqScan)
		},
		func(m *sessionDataMutator, v bool) {
			m.SetEnableSeqScan(v)
		},
		func(sv *settings.Values) string { return "on" },
	),
	"synchronous_commit": makeDummyBooleanSessionVar(
		"synchronous_commit",
		func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.SynchronousCommit)
		},
		func(m *sessionDataMutator, v bool) {
			m.SetSynchronousCommit(v)
		},
		func(sv *settings.Values) string { return "on" },
	),
}

// UnsupportedVars contains the set of PostgreSQL session variables
// and client parameters that are not supported in CockroachDB.
// These are used to produce error messages and telemetry.
var UnsupportedVars = func(ss ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}(
	// The following list can be regenerated with:
	//
	//    grep ', PGC_\(SUSET\|USERSET\)' src/backend/utils/misc/guc.c | \
	//        sed -e 's/^[^"]*"/"/g;s/"[^"]*$/",/g' | \
	//        tr A-Z a-z \
	//        sort -u
	//

	"optimize_bounded_sort",
	// "datestyle",
	// "intervalstyle",
	// "timezone",
	// "application_name",
	"array_nulls",
	"backend_flush_after",
	// "bytea_output",
	"check_function_bodies",
	// "client_encoding",
	// "client_min_messages",
	"commit_delay",
	"commit_siblings",
	"constraint_exclusion",
	"cpu_index_tuple_cost",
	"cpu_operator_cost",
	"cpu_tuple_cost",
	"cursor_tuple_fraction",
	"deadlock_timeout",
	"debug_deadlocks",
	"debug_pretty_print",
	"debug_print_parse",
	"debug_print_plan",
	"debug_print_rewritten",
	"default_statistics_target",
	"default_text_search_config",
	"default_transaction_deferrable",
	// "default_transaction_isolation",
	// "default_transaction_read_only",
	"default_with_oids",
	"dynamic_library_path",
	"effective_cache_size",
	"enable_bitmapscan",
	"enable_gathermerge",
	"enable_hashagg",
	"enable_hashjoin",
	"enable_indexonlyscan",
	"enable_indexscan",
	"enable_material",
	"enable_mergejoin",
	"enable_nestloop",
	// "enable_seqscan",
	"enable_sort",
	"enable_tidscan",
	// "escape_string_warning",
	"exit_on_error",
	// "extra_float_digits",
	"force_parallel_mode",
	"from_collapse_limit",
	"geqo",
	"geqo_effort",
	"geqo_generations",
	"geqo_pool_size",
	"geqo_seed",
	"geqo_selection_bias",
	"geqo_threshold",
	"gin_fuzzy_search_limit",
	"gin_pending_list_limit",
	// "idle_in_transaction_session_timeout",
	"ignore_checksum_failure",
	"join_collapse_limit",
	"lc_messages",
	"lc_monetary",
	"lc_numeric",
	"lc_time",
	"lo_compat_privileges",
	"local_preload_libraries",
	// "lock_timeout",
	"log_btree_build_stats",
	"log_duration",
	"log_error_verbosity",
	"log_executor_stats",
	"log_lock_waits",
	"log_min_duration_statement",
	"log_min_error_statement",
	"log_min_messages",
	"log_parser_stats",
	"log_planner_stats",
	"log_replication_commands",
	"log_statement",
	"log_statement_stats",
	"log_temp_files",
	"maintenance_work_mem",
	"max_parallel_workers",
	"max_parallel_workers_per_gather",
	"max_stack_depth",
	"min_parallel_index_scan_size",
	"min_parallel_table_scan_size",
	"operator_precedence_warning",
	"parallel_setup_cost",
	"parallel_tuple_cost",
	"password_encryption",
	"quote_all_identifiers",
	"random_page_cost",
	"replacement_sort_tuples",
	"role",
	// "row_security",
	// "search_path",
	"seed",
	"seq_page_cost",
	// "session_authorization",
	"session_preload_libraries",
	"session_replication_role",
	// "ssl_renegotiation_limit",
	// "standard_conforming_strings",
	// "statement_timeout",
	// "synchronize_seqscans",
	// "synchronous_commit",
	"tcp_keepalives_count",
	"tcp_keepalives_idle",
	"tcp_keepalives_interval",
	"temp_buffers",
	"temp_file_limit",
	"temp_tablespaces",
	"timezone_abbreviations",
	"trace_lock_oidmin",
	"trace_lock_table",
	"trace_locks",
	"trace_lwlocks",
	"trace_notify",
	"trace_sort",
	"trace_syncscan",
	"trace_userlocks",
	"track_activities",
	"track_counts",
	"track_functions",
	"track_io_timing",
	"transaction_deferrable",
	// "transaction_isolation",
	// "transaction_read_only",
	"transform_null_equals",
	"update_process_title",
	"vacuum_cost_delay",
	"vacuum_cost_limit",
	"vacuum_cost_page_dirty",
	"vacuum_cost_page_hit",
	"vacuum_cost_page_miss",
	"vacuum_freeze_min_age",
	"vacuum_freeze_table_age",
	"vacuum_multixact_freeze_min_age",
	"vacuum_multixact_freeze_table_age",
	"wal_compression",
	"wal_consistency_checking",
	"wal_debug",
	"work_mem",
	"xmlbinary",
	"xmloption",
	"zero_damaged_pages",
)
