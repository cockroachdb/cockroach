// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobutils

// getExternalBytesForConnectedTenant returns the count of external bytes of the
// tenant that ran the query.
const GetExternalBytesForConnectedTenant = `SELECT
COALESCE(stats->>'external_file_bytes','0') FROM
crdb_internal.tenant_span_stats( ARRAY(SELECT(crdb_internal.tenant_span()[1], crdb_internal.tenant_span()[2])))`

// getExternalBytesUserKeySpace returns the count of external bytes over all
// user key space [TenantTableDataMin, TenantTableDataMax). This can only get
// run from the system tenant.
const GetExternalBytesTenantKeySpace = `SELECT COALESCE(stats->>'external_file_bytes','0') FROM crdb_internal.tenant_span_stats(
		ARRAY(SELECT('\x89'::BYTES,'\xffff'::BYTES)))`
