// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

var rustPostgresBlocklist = blocklist{}

var rustPostgresIgnoreList = blocklist{
	"binary_copy.read_basic":                "unknown",
	"binary_copy.read_big_rows":             "unknown",
	"binary_copy.read_many_rows":            "unknown",
	"binary_copy.write_basic":               "unknown",
	"binary_copy.write_big_rows":            "unknown",
	"binary_copy.write_many_rows":           "unknown",
	"composites.defaults":                   "unknown",
	"composites.extra_field":                "unknown",
	"composites.missing_field":              "unknown",
	"composites.name_overrides":             "unknown",
	"composites.raw_ident_field":            "unknown",
	"composites.wrong_name":                 "unknown",
	"composites.wrong_type":                 "unknown",
	"domains.defaults":                      "unknown",
	"domains.domain_in_composite":           "unknown",
	"domains.name_overrides":                "unknown",
	"domains.wrong_name":                    "unknown",
	"domains.wrong_type":                    "unknown",
	"enums.defaults":                        "unknown",
	"enums.extra_variant":                   "unknown",
	"enums.missing_variant":                 "unknown",
	"enums.name_overrides":                  "unknown",
	"enums.wrong_name":                      "unknown",
	"runtime.multiple_hosts_multiple_ports": "unknown",
	"runtime.multiple_hosts_one_port":       "unknown",
	"runtime.target_session_attrs_ok":       "unknown",
	"runtime.tcp":                           "unknown",
	"runtime.unix_socket":                   "unknown",
	"test.binary_copy_in":                   "unknown",
	"test.binary_copy_out":                  "unknown",
	"test.copy_in":                          "unknown",
	"test.copy_in_abort":                    "unknown",
	"test.copy_out":                         "unknown",
	"test.nested_transactions":              "unknown",
	"test.notice_callback":                  "unknown",
	"test.notifications_blocking_iter":      "unknown",
	"test.notifications_iter":               "unknown",
	"test.notifications_timeout_iter":       "unknown",
	"test.portal":                           "unknown",
	"test.prefer":                           "unknown",
	"test.prepare":                          "unknown",
	"test.require":                          "unknown",
	"test.require_channel_binding_ok":       "unknown",
	"test.runtime":                          "unknown",
	"test.savepoints":                       "unknown",
	"test.scram_user":                       "unknown",
	"test.transaction_commit":               "unknown",
	"transparent.round_trip":                "unknown",
	"types.composite":                       "unknown",
	"types.domain":                          "unknown",
	"types.enum_":                           "unknown",
	"types.lquery":                          "unknown",
	"types.lquery_any":                      "unknown",
	"types.ltree":                           "unknown",
	"types.ltree_any":                       "unknown",
	"types.ltxtquery":                       "unknown",
	"types.ltxtquery_any":                   "unknown",
	"types.test_array_vec_params":           "unknown",
	"types.test_citext_params":              "unknown",
	"types.test_hstore_params":              "unknown",
	"types.test_i16_params":                 "unknown",
	"types.test_i32_params":                 "unknown",
	"types.test_lsn_params":                 "unknown",
	"types.test_pg_database_datname":        "unknown",
	"types.test_slice":                      "unknown",
	"types.test_slice_range":                "unknown",
}
