// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"fmt"
	"sort"
	"strings"
)

// registry contains all defined settings, their types and default values.
//
// The registry does not store the current values of the settings; those are
// stored separately in Values, allowing multiple independent instances
// of each setting in the registry.
//
// registry should never be mutated after creation (except in tests), as it is
// read concurrently by different callers.
var registry = make(map[InternalKey]internalSetting)

// tenantReadOnlyKeys contains the keys of settings that have the
// class SystemVisible. This is used to initialize defaults in the
// tenant settings watcher.
var tenantReadOnlyKeys []InternalKey

// aliasRegistry contains the mapping of names to keys, for names
// different from the keys.
var aliasRegistry = make(map[SettingName]aliasEntry)

type aliasEntry struct {
	// key is the setting this name is referring to.
	key InternalKey
	// active indicates whether this name is currently in use.
	active NameStatus
}

// slotTable stores the same settings as the registry, but accessible by the
// slot index.
var slotTable [MaxSettings]internalSetting

// TestingSaveRegistry can be used in tests to save/restore the current
// contents of the registry.
func TestingSaveRegistry() func() {
	var origRegistry = make(map[InternalKey]internalSetting)
	for k, v := range registry {
		origRegistry[k] = v
	}
	var origAliases = make(map[SettingName]aliasEntry)
	for k, v := range aliasRegistry {
		origAliases[k] = v
	}
	var origSystemVisibleKeys = make([]InternalKey, len(tenantReadOnlyKeys))
	copy(origSystemVisibleKeys, tenantReadOnlyKeys)
	return func() {
		registry = origRegistry
		aliasRegistry = origAliases
		tenantReadOnlyKeys = origSystemVisibleKeys
	}
}

// When a setting class changes from ApplicationLevel to System, it should
// be added to this list so that we can offer graceful degradation to
// users of previous versions.
var systemSettingsWithPreviousApplicationClass = map[InternalKey]struct{}{
	// Changed in v23.2.
	"cluster.organization":                        {},
	"enterprise.license":                          {},
	"kv.bulk_io_write.concurrent_export_requests": {},
	"kv.closed_timestamp.propagation_slack":       {},
	"kv.closed_timestamp.side_transport_interval": {},
	"kv.closed_timestamp.target_duration":         {},
	"kv.raft.command.max_size":                    {},
	"kv.rangefeed.enabled":                        {},
	"server.rangelog.ttl":                         {},
	"timeseries.storage.enabled":                  {},
	"timeseries.storage.resolution_10s.ttl":       {},
	"timeseries.storage.resolution_30m.ttl":       {},
}

// SettingPreviouslyHadApplicationClass returns true if the setting
// used to have the ApplicationLevel class.
func SettingPreviouslyHadApplicationClass(key InternalKey) bool {
	_, ok := systemSettingsWithPreviousApplicationClass[key]
	return ok
}

// When a setting is removed, it should be added to this list so that we cannot
// accidentally reuse its name, potentially mis-handling older values.
var retiredSettings = map[InternalKey]struct{}{
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
	"sql.drop_tenant.enabled":                              {},

	// removed as of 23.2.
	"sql.log.unstructured_entries.enabled":                     {},
	"sql.auth.createrole_allows_grant_role_membership.enabled": {},
	"changefeed.replan_flow_frequency":                         {},
	"changefeed.replan_flow_threshold":                         {},
	"jobs.trace.force_dump_mode":                               {},
	"timeseries.storage.30m_resolution_ttl":                    {},
	"server.cpu_profile.enabled":                               {},
	"changefeed.lagging_ranges_threshold":                      {},
	"changefeed.lagging_ranges_polling_rate":                   {},
	"trace.jaeger.agent":                                       {},
	"bulkio.restore.use_simple_import_spans":                   {},
	"bulkio.restore.remove_regions.enabled":                    {},

	// removed as of 24.1
	"storage.mvcc.range_tombstones.enabled":                  {},
	"changefeed.balance_range_distribution.enable":           {},
	"changefeed.mux_rangefeed.enabled":                       {},
	"kv.rangefeed.catchup_scan_concurrency":                  {},
	"physical_replication.producer.mux_rangefeeds.enabled":   {},
	"kv.rangefeed.use_dedicated_connection_class.enabled":    {},
	"sql.trace.session_eventlog.enabled":                     {},
	"sql.show_ranges_deprecated_behavior.enabled":            {},
	"sql.drop_virtual_cluster.enabled":                       {},
	"cross_cluster_replication.enabled":                      {},
	"server.controller.default_tenant.check_service.enabled": {},

	// removed as of 24.2
	"storage.value_blocks.enabled":       {},
	"kv.gc.sticky_hint.enabled":          {},
	"kv.rangefeed.range_stuck_threshold": {},

	// removed as of 24.3
	"bulkio.backup.split_keys_on_timestamps":           {},
	"sql.create_tenant.default_template":               {},
	"kvadmission.low_pri_read_elastic_control.enabled": {},

	// removed as of 25.1
	"sql.auth.resolve_membership_single_scan.enabled":            {},
	"storage.single_delete.crash_on_invariant_violation.enabled": {},
	"storage.single_delete.crash_on_ineffectual.enabled":         {},
	"bulkio.backup.elide_common_prefix.enabled":                  {},
	"kv.bulkio.write_metadata_sst.enabled":                       {},
	"jobs.execution_errors.max_entries":                          {},
	"jobs.execution_errors.max_entry_size":                       {},
	"sql.metrics.statement_details.plan_collection.enabled":      {},
	"sql.metrics.statement_details.plan_collection.period":       {},

	// removed as of 25.2
	"sql.catalog.experimental_use_session_based_leasing": {},
	"bulkio.backup.merge_file_buffer_size":               {},
	"changefeed.new_webhook_sink_enabled":                {},
	"changefeed.new_webhook_sink.enabled":                {},
	"changefeed.new_pubsub_sink_enabled":                 {},
	"changefeed.new_pubsub_sink.enabled":                 {},
}

// grandfatheredDefaultSettings is the list of "grandfathered" existing sql.defaults
// cluster settings. In 22.2 and later, new session settings do not need an
// associated sql.defaults cluster setting. Instead they can have their default
// changed with ALTER ROLE ... SET.
// Caveat: in some cases, we may still add new sql.defaults cluster settings,
// but the new ones *must* be marked as non-public. Undocumented settings are
// excluded from the check that prevents new sql.defaults settings. The
// reason for this is that the rollout automation framework used in
// CockroachCloud works by using cluster settings. If we want to slowly roll out
// a feature that is gated behind a session setting, using a non-public
// sql.defaults cluster setting is the recommended way to do so.
var grandfatheredDefaultSettings = map[InternalKey]struct{}{
	// PLEASE DO NOT ADD NEW SETTINGS TO THIS MAP. THANK YOU.
	"sql.defaults.cost_scans_with_default_col_size.enabled":                     {},
	"sql.defaults.datestyle":                                                    {},
	"sql.defaults.datestyle.enabled":                                            {},
	"sql.defaults.deadlock_timeout":                                             {},
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

// checkNameFound verifies whether the given string is known as key or name.
func checkNameFound(keyOrName string) {
	if _, ok := retiredSettings[InternalKey(keyOrName)]; ok {
		panic(fmt.Sprintf("cannot reuse previously defined setting key: %s", InternalKey(keyOrName)))
	}
	if _, ok := registry[InternalKey(keyOrName)]; ok {
		panic(fmt.Sprintf("setting already defined: %s", keyOrName))
	}
	if a, ok := aliasRegistry[SettingName(keyOrName)]; ok {
		panic(fmt.Sprintf("setting already defined: %s (with key %s)", keyOrName, a.key))
	}
}

// register adds a setting to the registry.
func register(class Class, key InternalKey, desc string, s internalSetting) {
	checkNameFound(string(key))
	if strings.Contains(string(key), "sql.defaults") {
		_, grandfathered := grandfatheredDefaultSettings[key]
		if !grandfathered && s.Visibility() != Reserved {
			panic(fmt.Sprintf(
				"new sql.defaults cluster settings: %s is not needed now that `ALTER ROLE ... SET` syntax "+
					"is supported; please remove the new sql.defaults cluster setting or make it non-public", key))
		}
	}

	slot := slotIdx(len(registry))
	s.init(class, key, desc, slot)
	registry[key] = s
	slotTable[slot] = s
	if class == SystemVisible {
		tenantReadOnlyKeys = append(tenantReadOnlyKeys, key)
	}
}

func registerAlias(key InternalKey, name SettingName, nameStatus NameStatus) {
	checkNameFound(string(name))
	aliasRegistry[name] = aliasEntry{key: key, active: nameStatus}
}

// NumRegisteredSettings returns the number of registered settings.
func NumRegisteredSettings() int { return len(registry) }

// Keys returns a sorted string array with all the known keys.
func Keys(forSystemTenant bool) (res []InternalKey) {
	res = make([]InternalKey, 0, len(registry))
	for k, v := range registry {
		if v.isRetired() {
			continue
		}
		if !forSystemTenant && v.Class() == SystemOnly {
			continue
		}
		res = append(res, k)
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res
}

// SystemVisibleKeys returns a array with all the known keys that
// have the class SystemVisible. It might not be sorted.
// The caller must refrain from modifying the return value.
func SystemVisibleKeys() []InternalKey {
	return tenantReadOnlyKeys
}

// ConsoleKeys return an array with all cluster settings keys
// used by the UI Console.
// This list should only contain settings that have no sensitive
// information, because it will be returned on `settings` endpoint for
// users without VIEWCLUSTERSETTING or MODIFYCLUSTERSETTING permission,
// but that have VIEWACTIVITY or VIEWACTIVITYREDACTED permissions.
func ConsoleKeys() (res []InternalKey) {
	return allConsoleKeys
}

var allConsoleKeys = []InternalKey{
	"keyvisualizer.enabled",
	"keyvisualizer.sample_interval",
	"sql.index_recommendation.drop_unused_duration",
	"sql.insights.anomaly_detection.latency_threshold",
	"sql.insights.high_retry_count.threshold",
	"sql.insights.latency_threshold",
	"sql.stats.automatic_collection.enabled",
	"timeseries.storage.resolution_10s.ttl",
	"timeseries.storage.resolution_30m.ttl",
	"ui.display_timezone",
	"version",
}

// NameToKey returns the key associated with a setting name.
func NameToKey(name SettingName) (key InternalKey, found bool, nameStatus NameStatus) {
	// First check the alias registry.
	if alias, ok := aliasRegistry[name]; ok {
		return alias.key, true, alias.active
	}
	// No alias: did they perhaps use the key directly?
	maybeKey := InternalKey(name)
	if s, ok := registry[maybeKey]; ok {
		nameStatus := NameActive
		if s.Name() != SettingName(maybeKey) {
			// The user is invited to use the new name instead of the key.
			nameStatus = NameRetired
		}
		return maybeKey, true, nameStatus
	}
	return "", false, NameActive
}

// LookupForLocalAccess returns a NonMaskedSetting by name. Used when a setting
// is being retrieved for local processing within the cluster and not for
// reporting; sensitive values are accessible.
func LookupForLocalAccess(
	name SettingName, forSystemTenant bool,
) (NonMaskedSetting, bool, NameStatus) {
	key, ok, nameStatus := NameToKey(name)
	if !ok {
		return nil, ok, nameStatus
	}
	s, ok := LookupForLocalAccessByKey(key, forSystemTenant)
	return s, ok, nameStatus
}

// LookupForLocalAccessByKey returns a NonMaskedSetting by key. Used when a
// setting is being retrieved for local processing within the cluster and not
// for reporting; sensitive values are accessible.
func LookupForLocalAccessByKey(key InternalKey, forSystemTenant bool) (NonMaskedSetting, bool) {
	s, ok := registry[key]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	return s, true
}

// LookupForReportingByKey returns a Setting by key. Used when a setting is being
// retrieved for reporting.
//
// For settings that are non-reportable, the returned Setting hides the current
// value (see Setting.String).
func LookupForReportingByKey(key InternalKey, forSystemTenant bool) (Setting, bool) {
	s, ok := registry[key]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	if !s.isReportable() {
		return &MaskedSetting{setting: s}, true
	}
	return s, true
}

// LookupForDisplay returns a Setting by key. Used when a setting is being
// retrieved for display in SHOW commands or crdb_internal tables.
//
// For settings that are sensitive, the returned Setting hides the current
// value (see Setting.String) if canViewSensitive is false.
func LookupForDisplay(
	name SettingName, forSystemTenant, canViewSensitive bool,
) (Setting, bool, NameStatus) {
	key, ok, nameStatus := NameToKey(name)
	if !ok {
		return nil, ok, nameStatus
	}
	s, ok := LookupForDisplayByKey(key, forSystemTenant, canViewSensitive)
	return s, ok, nameStatus
}

// LookupForDisplayByKey returns a Setting by key. Used when a setting is being
// retrieved for display in SHOW commands or crdb_internal tables.
//
// For settings that are sensitive, the returned Setting hides the current
// value (see Setting.String) if canViewSensitive is false.
func LookupForDisplayByKey(
	key InternalKey, forSystemTenant, canViewSensitive bool,
) (Setting, bool) {
	s, ok := registry[key]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	if s.isSensitive() && !canViewSensitive {
		return &MaskedSetting{setting: s}, true
	}
	return s, true
}

// ForSystemTenant can be passed to Lookup for code that runs only on the system
// tenant.
const ForSystemTenant = true

// ForVirtualCluster can be passed to Lookup for code that runs on
// virtual clusters.
const ForVirtualCluster = false

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
	"p": "protobuf",
}

// RedactedValue returns:
//   - a string representation of the value, if the setting is reportable;
//   - "<redacted>" if the setting is not reportable, sensitive, or a string;
//   - "<unknown>" if there is no setting with this name.
func RedactedValue(key InternalKey, values *Values, forSystemTenant bool) string {
	if k, ok := registry[key]; ok {
		if k.Typ() == "s" || k.isSensitive() || !k.isReportable() {
			return "<redacted>"
		}
	}
	if setting, ok := LookupForReportingByKey(key, forSystemTenant); ok {
		return setting.String(values)
	}
	return "<unknown>"
}

// TestingListPrevAppSettings is exported for testing only.
func TestingListPrevAppSettings() map[InternalKey]struct{} {
	return systemSettingsWithPreviousApplicationClass
}
