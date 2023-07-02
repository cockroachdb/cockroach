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

var rustPostgresBlocklist = blocklist{
	"binary_copy.read_basic":                "No binary COPY TO support - https://github.com/cockroachdb/cockroach/issues/97180",
	"binary_copy.read_big_rows":             "default int size (int4 vs int8) mismatch",
	"binary_copy.read_many_rows":            "No binary COPY TO support - https://github.com/cockroachdb/cockroach/issues/97180",
	"binary_copy.write_basic":               "COPY FROM not supported in extended protocol",
	"binary_copy.write_big_rows":            "COPY FROM not supported in extended protocol",
	"binary_copy.write_many_rows":           "COPY FROM not supported in extended protocol",
	"composites.defaults":                   "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"composites.extra_field":                "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"composites.missing_field":              "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"composites.name_overrides":             "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"composites.raw_ident_field":            "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"composites.wrong_name":                 "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"composites.wrong_type":                 "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"domains.defaults":                      "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"domains.domain_in_composite":           "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"domains.name_overrides":                "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"domains.wrong_name":                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"domains.wrong_type":                    "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"enums.defaults":                        "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"enums.extra_variant":                   "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"enums.missing_variant":                 "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"enums.name_overrides":                  "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"enums.wrong_name":                      "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"runtime.multiple_hosts_multiple_ports": "default int size (int4 vs int8) mismatch",
	"runtime.multiple_hosts_one_port":       "default int size (int4 vs int8) mismatch",
	"runtime.target_session_attrs_ok":       "default int size (int4 vs int8) mismatch",
	"runtime.tcp":                           "default int size (int4 vs int8) mismatch",
	"test.binary_copy_in":                   "COPY FROM not supported in extended protocol",
	"test.binary_copy_out":                  "No COPY TO support - https://github.com/cockroachdb/cockroach/issues/85571",
	"test.copy_in":                          "COPY FROM not supported in extended protocol",
	"test.copy_in_abort":                    "COPY FROM not supported in extended protocol",
	"test.nested_transactions":              "default int size (int4 vs int8) mismatch",
	"test.notice_callback":                  "unsupported feature - https://github.com/cockroachdb/cockroach/issues/17511",
	"test.notifications_blocking_iter":      "unsupported feature - https://github.com/cockroachdb/cockroach/issues/41522",
	"test.notifications_iter":               "unsupported feature - https://github.com/cockroachdb/cockroach/issues/41522",
	"test.notifications_timeout_iter":       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/41522",
	"test.portal":                           "default int size (int4 vs int8) mismatch",
	"test.prefer":                           "password authentication failed",
	"test.prepare":                          "default int size (int4 vs int8) mismatch",
	"test.require":                          "server does not support TLS",
	"test.require_channel_binding_ok":       "password authentication failed",
	"test.runtime":                          "server does not support TLS",
	"test.savepoints":                       "default int size (int4 vs int8) mismatch",
	"test.scram_user":                       "server does not support TLS",
	"test.transaction_commit":               "unknown function: txid_current()",
	"transparent.round_trip":                "default int size (int4 vs int8) mismatch",
	"types.composite":                       "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27792",
	"types.domain":                          "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27796",
	"types.enum_":                           "experimental feature - https://github.com/cockroachdb/cockroach/issues/46260",
	"types.lquery":                          "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.lquery_any":                      "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.ltree":                           "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.ltree_any":                       "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.ltxtquery":                       "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.ltxtquery_any":                   "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/44657",
	"types.test_array_vec_params":           "default int size (int4 vs int8) mismatch",
	"types.test_citext_params":              "unsupported citext type alias - https://github.com/cockroachdb/cockroach/issues/22463",
	"types.test_hstore_params":              "unsupported datatype - https://github.com/cockroachdb/cockroach/issues/41284",
	"types.test_i16_params":                 "default int size (int4 vs int8) mismatch",
	"types.test_i32_params":                 "default int size (int4 vs int8) mismatch",
	"types.test_pg_database_datname":        "default database name mismatch",
	"types.test_slice":                      "default int size (int4 vs int8) mismatch",
	"types.test_slice_range":                "unsupported feature - https://github.com/cockroachdb/cockroach/issues/27791",
}

var rustPostgresIgnoreList = blocklist{
	"runtime.unix_socket": "unknown",
}
