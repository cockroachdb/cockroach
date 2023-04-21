// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// registry contains all defined settings, their types and default values.
//
// The registry does not store the current values of the settings; those are
// stored separately in Values, allowing multiple independent instances
// of each setting in the registry.
//
// registry should never be mutated after creation (except in tests), as it is
// read concurrently by different callers.
var registry = make(map[string]internalSetting)

// slotTable stores the same settings as the registry, but accessible by the
// slot index.
var slotTable [MaxSettings]internalSetting

// TestingSaveRegistry can be used in tests to save/restore the current
// contents of the registry.
func TestingSaveRegistry() func() {
	var origRegistry = make(map[string]internalSetting)
	for k, v := range registry {
		origRegistry[k] = v
	}
	return func() {
		registry = origRegistry
	}
}

// When a setting is removed, it should be added to this list so that we cannot
// accidentally reuse its name, potentially mis-handling older values.
var retiredSettings = map[string]struct{}{
	// removed as of 2.0.
	"kv.gc.batch_size":                     {},
	"kv.transaction.max_intents":           {},
	"diagnostics.reporting.report_metrics": {},
	// removed as of 2.1.
	"kv.allocator.stat_based_rebalancing.enabled": {},
	"kv.allocator.stat_rebalance_threshold":       {},
	// removed as of 19.1.
	"kv.raft_log.synchronize": {},
	// removed as of 19.2.
	"schemachanger.bulk_index_backfill.enabled":            {},
	"rocksdb.ingest_backpressure.delay_l0_file":            {}, // never used
	"server.heap_profile.system_memory_threshold_fraction": {},
	"timeseries.storage.10s_resolution_ttl":                {},
	"changefeed.push.enabled":                              {},
	"sql.defaults.optimizer":                               {},
	"kv.bulk_io_write.addsstable_max_rate":                 {},
	// removed as of 20.1.
	"schemachanger.lease.duration":           {},
	"schemachanger.lease.renew_fraction":     {},
	"diagnostics.forced_stat_reset.interval": {},
	// removed as of 20.2.
	"rocksdb.ingest_backpressure.pending_compaction_threshold":         {},
	"sql.distsql.temp_storage.joins":                                   {},
	"sql.distsql.temp_storage.sorts":                                   {},
	"sql.distsql.distribute_index_joins":                               {},
	"sql.distsql.merge_joins.enabled":                                  {},
	"sql.defaults.optimizer_foreign_keys.enabled":                      {},
	"sql.defaults.experimental_optimizer_foreign_key_cascades.enabled": {},
	"sql.parallel_scans.enabled":                                       {},
	"backup.table_statistics.enabled":                                  {},
	// removed as of 21.1.
	"sql.distsql.interleaved_joins.enabled": {},
	"sql.testing.vectorize.batch_size":      {},
	"sql.testing.mutations.max_batch_size":  {},
	"sql.testing.mock_contention.enabled":   {},
	"kv.atomic_replication_changes.enabled": {},
	// removed as of 21.1.2.
	"kv.tenant_rate_limiter.read_requests.rate_limit":   {},
	"kv.tenant_rate_limiter.read_requests.burst_limit":  {},
	"kv.tenant_rate_limiter.write_requests.rate_limit":  {},
	"kv.tenant_rate_limiter.write_requests.burst_limit": {},
	"kv.tenant_rate_limiter.read_bytes.rate_limit":      {},
	"kv.tenant_rate_limiter.read_bytes.burst_limit":     {},
	"kv.tenant_rate_limiter.write_bytes.rate_limit":     {},
	"kv.tenant_rate_limiter.write_bytes.burst_limit":    {},

	// removed as of 21.2.
	"sql.defaults.vectorize_row_count_threshold":                     {},
	"cloudstorage.gs.default.key":                                    {},
	"storage.sst_export.max_intents_per_error":                       {},
	"jobs.registry.leniency":                                         {},
	"sql.defaults.experimental_expression_based_indexes.enabled":     {},
	"kv.transaction.write_pipelining_max_outstanding_size":           {},
	"sql.defaults.optimizer_improve_disjunction_selectivity.enabled": {},
	"bulkio.backup.proxy_file_writes.enabled":                        {},
	"sql.distsql.prefer_local_execution.enabled":                     {},
	"kv.follower_read.target_multiple":                               {},
	"kv.closed_timestamp.close_fraction":                             {},
	"sql.telemetry.query_sampling.qps_threshold":                     {},
	"sql.telemetry.query_sampling.sample_rate":                       {},
	"diagnostics.sql_stat_reset.interval":                            {},
	"changefeed.mem.pushback_enabled":                                {},
	"sql.distsql.index_join_limit_hint.enabled":                      {},

	// removed as of 22.1.
	"sql.defaults.drop_enum_value.enabled":                             {},
	"trace.lightstep.token":                                            {},
	"trace.datadog.agent":                                              {},
	"trace.datadog.project":                                            {},
	"sql.defaults.interleaved_tables.enabled":                          {},
	"sql.defaults.copy_partitioning_when_deinterleaving_table.enabled": {},
	"server.declined_reservation_timeout":                              {},
	"bulkio.backup.resolve_destination_in_job.enabled":                 {},
	"sql.defaults.experimental_hash_sharded_indexes.enabled":           {},
	"schemachanger.backfiller.max_sst_size":                            {},
	"kv.bulk_ingest.buffer_increment":                                  {},
	"schemachanger.backfiller.buffer_increment":                        {},
	"kv.rangefeed.separated_intent_scan.enabled":                       {},

	// removed as of 22.1.2.
	"tenant_cost_model.kv_read_request_cost":            {},
	"tenant_cost_model.kv_read_cost_per_megabyte":       {},
	"tenant_cost_model.kv_write_request_cost":           {},
	"tenant_cost_model.kv_write_cost_per_megabyte":      {},
	"tenant_cost_model.pod_cpu_second_cost":             {},
	"tenant_cost_model.pgwire_egress_cost_per_megabyte": {},
	"sql.ttl.range_batch_size":                          {},

	// removed as of 22.2.
	"bulkio.restore_at_current_time.enabled":                    {},
	"bulkio.import_at_current_time.enabled":                     {},
	"kv.bulk_io_write.experimental_incremental_export_enabled":  {},
	"kv.bulk_io_write.revert_range_time_bound_iterator.enabled": {},
	"kv.rangefeed.catchup_scan_iterator_optimization.enabled":   {},
	"kv.refresh_range.time_bound_iterators.enabled":             {},
	"sql.defaults.datestyle.enabled":                            {},
	"sql.defaults.intervalstyle.enabled":                        {},

	// removed as of 22.2.1
	"sql.ttl.default_range_concurrency":                {},
	"server.web_session.purge.period":                  {},
	"server.web_session.purge.max_deletions_per_cycle": {},
	"server.web_session.auto_logout.timeout":           {},

	// removed as of 23.1.
	"sql.auth.modify_cluster_setting_applies_to_all.enabled": {},
	"sql.catalog.descs.validate_on_write.enabled":            {},
	"sql.distsql.max_running_flows":                          {},
	"sql.distsql.flow_scheduler_queueing.enabled":            {},
	"sql.distsql.drain.cancel_after_wait.enabled":            {},
	"changefeed.active_protected_timestamps.enabled":         {},
	"jobs.scheduler.single_node_scheduler.enabled":           {},
	"tenant_capabilities.authorizer.enabled":                 {},
	// renamed.
	"spanconfig.host_coalesce_adjacent.enabled":            {},
	"sql.defaults.experimental_stream_replication.enabled": {},
}

// sqlDefaultSettings is the list of "grandfathered" existing sql.defaults
// cluster settings. In 22.2 and later, new session settings do not need an
// associated sql.defaults cluster setting. Instead they can have their default
// changed with ALTER ROLE ... SET.
var sqlDefaultSettings = map[string]struct{}{
	// PLEASE DO NOT ADD NEW SETTINGS TO THIS MAP. THANK YOU.
	"sql.defaults.cost_scans_with_default_col_size.enabled":                     {},
	"sql.defaults.datestyle":                                                    {},
	"sql.defaults.datestyle.enabled":                                            {},
	"sql.defaults.default_hash_sharded_index_bucket_count":                      {},
	"sql.defaults.default_int_size":                                             {},
	"sql.defaults.disallow_full_table_scans.enabled":                            {},
	"sql.defaults.distsql":                                                      {},
	"sql.defaults.experimental_alter_column_type.enabled":                       {},
	"sql.defaults.experimental_auto_rehoming.enabled":                           {},
	"sql.defaults.experimental_computed_column_rewrites":                        {},
	"sql.defaults.experimental_distsql_planning":                                {},
	"sql.defaults.experimental_enable_unique_without_index_constraints.enabled": {},
	"sql.defaults.experimental_implicit_column_partitioning.enabled":            {},
	"sql.defaults.experimental_temporary_tables.enabled":                        {},
	"sql.defaults.foreign_key_cascades_limit":                                   {},
	"sql.defaults.idle_in_session_timeout":                                      {},
	"sql.defaults.idle_in_transaction_session_timeout":                          {},
	"sql.defaults.implicit_select_for_update.enabled":                           {},
	"sql.defaults.insert_fast_path.enabled":                                     {},
	"sql.defaults.intervalstyle":                                                {},
	"sql.defaults.intervalstyle.enabled":                                        {},
	"sql.defaults.large_full_scan_rows":                                         {},
	"sql.defaults.locality_optimized_partitioned_index_scan.enabled":            {},
	"sql.defaults.lock_timeout":                                                 {},
	"sql.defaults.multiregion_placement_policy.enabled":                         {},
	"sql.defaults.on_update_rehome_row.enabled":                                 {},
	"sql.defaults.optimizer_use_histograms.enabled":                             {},
	"sql.defaults.optimizer_use_multicol_stats.enabled":                         {},
	"sql.defaults.override_alter_primary_region_in_super_region.enabled":        {},
	"sql.defaults.override_multi_region_zone_config.enabled":                    {},
	"sql.defaults.prefer_lookup_joins_for_fks.enabled":                          {},
	"sql.defaults.primary_region":                                               {},
	"sql.defaults.propagate_input_ordering.enabled":                             {},
	"sql.defaults.reorder_joins_limit":                                          {},
	"sql.defaults.require_explicit_primary_keys.enabled":                        {},
	"sql.defaults.results_buffer.size":                                          {},
	"sql.defaults.serial_normalization":                                         {},
	"sql.defaults.serial_sequences_cache_size":                                  {},
	"sql.defaults.statement_timeout":                                            {},
	"sql.defaults.stub_catalog_tables.enabled":                                  {},
	"sql.defaults.super_regions.enabled":                                        {},
	"sql.defaults.transaction_rows_read_err":                                    {},
	"sql.defaults.transaction_rows_read_log":                                    {},
	"sql.defaults.transaction_rows_written_err":                                 {},
	"sql.defaults.transaction_rows_written_log":                                 {},
	"sql.defaults.use_declarative_schema_changer":                               {},
	"sql.defaults.vectorize":                                                    {},
	"sql.defaults.zigzag_join.enabled":                                          {},
}

// register adds a setting to the registry.
func register(class Class, key, desc string, s internalSetting) {
	if _, ok := retiredSettings[key]; ok {
		panic(fmt.Sprintf("cannot reuse previously defined setting name: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
	}
	if strings.Contains(key, "sql.defaults") {
		if _, ok := sqlDefaultSettings[key]; !ok {
			panic(fmt.Sprintf(
				"new sql.defaults cluster settings: %s is not needed now that `ALTER ROLE ... SET` syntax "+
					"is supported; please remove the new sql.defaults cluster setting", key))
		}
	}
	if len(desc) == 0 {
		panic(fmt.Sprintf("setting missing description: %s", key))
	}
	if r, _ := utf8.DecodeRuneInString(desc); unicode.IsUpper(r) {
		panic(fmt.Sprintf(
			"setting descriptions should start with a lowercase letter: %q, %q", key, desc,
		))
	}
	for _, c := range desc {
		if c == unicode.ReplacementChar {
			panic(fmt.Sprintf("setting descriptions must be valid UTF-8: %q, %q", key, desc))
		}
		if unicode.IsControl(c) {
			panic(fmt.Sprintf(
				"setting descriptions cannot contain control character %q: %q, %q", c, key, desc,
			))
		}
	}
	slot := slotIdx(len(registry))
	s.init(class, key, desc, slot)
	registry[key] = s
	slotTable[slot] = s
}

// NumRegisteredSettings returns the number of registered settings.
func NumRegisteredSettings() int { return len(registry) }

// Keys returns a sorted string array with all the known keys.
func Keys(forSystemTenant bool) (res []string) {
	res = make([]string, 0, len(registry))
	for k, v := range registry {
		if v.isRetired() {
			continue
		}
		if !forSystemTenant && v.Class() == SystemOnly {
			continue
		}
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// LookupForLocalAccess returns a NonMaskedSetting by name. Used when a setting
// is being retrieved for local processing within the cluster and not for
// reporting; sensitive values are accessible.
func LookupForLocalAccess(name string, forSystemTenant bool) (NonMaskedSetting, bool) {
	s, ok := registry[name]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	return s, true
}

// LookupForReporting returns a Setting by name. Used when a setting is being
// retrieved for reporting.
//
// For settings that are non-reportable, the returned Setting hides the current
// value (see Setting.String).
func LookupForReporting(name string, forSystemTenant bool) (Setting, bool) {
	s, ok := registry[name]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	if !s.isReportable() {
		return &maskedSetting{setting: s}, true
	}
	return s, true
}

// ForSystemTenant can be passed to Lookup for code that runs only on the system
// tenant.
const ForSystemTenant = true

// ReadableTypes maps our short type identifiers to friendlier names.
var ReadableTypes = map[string]string{
	"s": "string",
	"i": "integer",
	"f": "float",
	"b": "boolean",
	"z": "byte size",
	"d": "duration",
	"e": "enumeration",
	// This is named "m" (instead of "v") for backwards compatibility reasons.
	"m": "version",
}

// RedactedValue returns:
//   - a string representation of the value, if the setting is reportable (or it
//     is a string setting with an empty value);
//   - "<redacted>" if the setting is not reportable;
//   - "<unknown>" if there is no setting with this name.
func RedactedValue(name string, values *Values, forSystemTenant bool) string {
	if setting, ok := LookupForReporting(name, forSystemTenant); ok {
		return setting.String(values)
	}
	return "<unknown>"
}
