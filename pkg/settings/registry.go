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
}

// register adds a setting to the registry.
func register(class Class, key, desc string, s internalSetting) {
	if _, ok := retiredSettings[key]; ok {
		panic(fmt.Sprintf("cannot reuse previously defined setting name: %s", key))
	}
	if _, ok := registry[key]; ok {
		panic(fmt.Sprintf("setting already defined: %s", key))
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

// Lookup returns a Setting by name along with its description.
// For non-reportable setting, it instantiates a MaskedSetting
// to masquerade for the underlying setting.
func Lookup(name string, purpose LookupPurpose, forSystemTenant bool) (Setting, bool) {
	s, ok := registry[name]
	if !ok {
		return nil, false
	}
	if !forSystemTenant && s.Class() == SystemOnly {
		return nil, false
	}
	if purpose == LookupForReporting && !s.isReportable() {
		return &MaskedSetting{setting: s}, true
	}
	return s, true
}

// LookupPurpose indicates what is being done with the setting.
type LookupPurpose int

const (
	// LookupForReporting indicates that a setting is being retrieved
	// for reporting and sensitive values should be scrubbed.
	LookupForReporting LookupPurpose = iota
	// LookupForLocalAccess indicates that a setting is being
	// retrieved for local processing within the cluster and
	// all values should be accessible
	LookupForLocalAccess
)

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

// RedactedValue returns a string representation of the value for settings
// types the are not considered sensitive (numbers, bools, etc) or
// <redacted> for those with values could store sensitive things (i.e. strings).
func RedactedValue(name string, values *Values, forSystemTenant bool) string {
	if setting, ok := Lookup(name, LookupForReporting, forSystemTenant); ok {
		return setting.String(values)
	}
	return "<unknown>"
}
