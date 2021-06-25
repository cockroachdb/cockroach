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
var registry = make(map[string]extendedSetting)

// TestingSaveRegistry can be used in tests to save/restore the current
// contents of the registry.
func TestingSaveRegistry() func() {
	var origRegistry = make(map[string]extendedSetting)
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
	"sql.defaults.vectorize_row_count_threshold":                 {},
	"cloudstorage.gs.default.key":                                {},
	"storage.sst_export.max_intents_per_error":                   {},
	"jobs.registry.leniency":                                     {},
	"sql.defaults.experimental_expression_based_indexes.enabled": {},
	"kv.tenant_rate_limiter.read_request_cost":                   {},
	"kv.tenant_rate_limiter.read_cost_per_megabyte":              {},
	"kv.tenant_rate_limiter.write_request_cost":                  {},
	"kv.tenant_rate_limiter.write_cost_per_megabyte":             {},
	"kv.transaction.write_pipelining_max_outstanding_size":       {},
}

// register adds a setting to the registry.
func register(key, desc string, s extendedSetting) {
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
		panic(fmt.Sprintf("setting descriptions should start with a lowercase letter: %q", desc))
	}
	s.setDescription(desc)
	registry[key] = s
	s.setSlotIdx(len(registry))
}

// NumRegisteredSettings returns the number of registered settings.
func NumRegisteredSettings() int { return len(registry) }

// Keys returns a sorted string array with all the known keys.
func Keys() (res []string) {
	res = make([]string, 0, len(registry))
	for k := range registry {
		if registry[k].isRetired() {
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
func Lookup(name string, purpose LookupPurpose) (Setting, bool) {
	v, ok := registry[name]
	var setting Setting = v
	if ok && purpose == LookupForReporting && !v.isReportable() {
		setting = &MaskedSetting{setting: v}
	}
	return setting, ok
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
func RedactedValue(name string, values *Values) string {
	if setting, ok := Lookup(name, LookupForReporting); ok {
		return setting.String(values)
	}
	return "<unknown>"
}
