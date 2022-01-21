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
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

// TODO(rima): Add more tests for serverless debug.zip

// Dummy import to pull in kvtenantccl. This allows us to start tenants.
// We need ccl functionality in order to test debug zip for serverless.
var _ = kvtenantccl.Connector{}

// TestServerlessZipContainsAllInternalTables verifies that we don't add new internal tables
// without also taking them into account in a `debug zip`. If this test fails,
// add your table to either of the []string slices referenced in the test (which
// are used by `debug zip`) or add it as an exception after having verified that
// it indeed should not be collected (this is rare).
// NB: if you're adding a new one, you'll also have to update TestZip.
func TestServerlessZipContainsAllInternalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, db := serverutils.StartTenant(
		t,
		tc.Server(0),
		base.TestTenantArgs{TenantID: serverutils.TestTenantID()},
	)
	defer db.Close()

	rows, err := db.Query(`
SELECT concat('crdb_internal.', table_name) as name
FROM [ SELECT table_name FROM [ SHOW TABLES FROM crdb_internal ] ]
WHERE
table_name NOT IN (
	-- allowlisted tables that don't need to be in debug zip
	'backward_dependencies',
	'builtin_functions',
	'cluster_contended_keys',
	'cluster_contended_indexes',
	'cluster_contended_tables',
	'cluster_inflight_traces',
	'cross_db_references',
	'databases',
	'forward_dependencies',
  'gossip_alerts',
  'gossip_liveness',
  'gossip_network',
  'gossip_nodes',
	'index_columns',
  'kv_node_liveness',
  'kv_node_status',
  'kv_store_status',
	'lost_descriptors_with_data',
	'table_columns',
	'table_row_statistics',
	'ranges',
	'ranges_no_leases',
	'predefined_comments',
	'session_trace',
	'session_variables',
	'tables',
	'cluster_statement_statistics',
	'cluster_transaction_statistics',
	'statement_statistics',
	'transaction_statistics',
	'tenant_usage_details'
)
ORDER BY name ASC`)
	assert.NoError(t, err)

	var tables []string
	for rows.Next() {
		var table string
		assert.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	tables = append(
		tables,
		"system.jobs",
		"system.descriptor",
		"system.namespace",
		"system.scheduled_jobs",
		"system.settings",
	)
	sort.Strings(tables)

	var exp []string
	exp = append(exp, debugZipTablesPerServerlessNode...)
	for _, t := range debugZipTablesPerServerlessCluster {
		t = strings.TrimPrefix(t, `"".`)
		exp = append(exp, t)
	}
	sort.Strings(exp)

	assert.Equal(t, exp, tables)
}
