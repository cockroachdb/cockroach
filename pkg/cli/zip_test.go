// Copyright 2020 The Cockroach Authors.
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
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

// TestZipContainsAllInternalTables verifies that we don't add new internal tables
// without also taking them into account in a `debug zip`. If this test fails,
// add your table to either of the []string slices referenced in the test (which
// are used by `debug zip`) or add it as an exception after having verified that
// it indeed should not be collected (this is rare).
// NB: if you're adding a new one, you'll also have to update TestZip.
func TestZipContainsAllInternalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query(`
SELECT concat('crdb_internal.', table_name) as name FROM [ SHOW TABLES FROM crdb_internal ] WHERE
    table_name NOT IN (
-- whitelisted tables that don't need to be in debug zip
'backward_dependencies',
'builtin_functions',
'create_statements',
'forward_dependencies',
'index_columns',
'table_columns',
'table_indexes',
'ranges',
'ranges_no_leases',
'predefined_comments',
'session_trace',
'session_variables',
'tables'
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
		"system.namespace_deprecated",
	)
	sort.Strings(tables)

	var exp []string
	exp = append(exp, debugZipTablesPerNode...)
	exp = append(exp, debugZipTablesPerCluster...)
	sort.Strings(exp)

	assert.Equal(t, exp, tables)
}

// This test the operation of zip over secure clusters.
func TestZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := newCLITest(cliTestParams{
		storeSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.cleanup()

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages:
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)log file.*$`)
	out = re.ReplaceAllString(out, `log file ...`)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)

	const expected = `debug zip ` + os.DevNull + `
establishing RPC connection to ...
retrieving the node status to get the SQL address...
using SQL address: ...
using SQL connection URL: postgresql://...
writing ` + os.DevNull + `
requesting data for debug/events... writing: debug/events.json
requesting data for debug/rangelog... writing: debug/rangelog.json
requesting data for debug/liveness... writing: debug/liveness.json
requesting data for debug/settings... writing: debug/settings.json
requesting data for debug/reports/problemranges... writing: debug/reports/problemranges.json
retrieving SQL data for crdb_internal.cluster_queries... writing: debug/crdb_internal.cluster_queries.txt
retrieving SQL data for crdb_internal.cluster_sessions... writing: debug/crdb_internal.cluster_sessions.txt
retrieving SQL data for crdb_internal.cluster_settings... writing: debug/crdb_internal.cluster_settings.txt
retrieving SQL data for crdb_internal.jobs... writing: debug/crdb_internal.jobs.txt
retrieving SQL data for system.jobs... writing: debug/system.jobs.txt
retrieving SQL data for system.descriptor... writing: debug/system.descriptor.txt
retrieving SQL data for system.namespace... writing: debug/system.namespace.txt
retrieving SQL data for system.namespace_deprecated... writing: debug/system.namespace_deprecated.txt
retrieving SQL data for crdb_internal.kv_node_status... writing: debug/crdb_internal.kv_node_status.txt
retrieving SQL data for crdb_internal.kv_store_status... writing: debug/crdb_internal.kv_store_status.txt
retrieving SQL data for crdb_internal.schema_changes... writing: debug/crdb_internal.schema_changes.txt
retrieving SQL data for crdb_internal.partitions... writing: debug/crdb_internal.partitions.txt
retrieving SQL data for crdb_internal.zones... writing: debug/crdb_internal.zones.txt
requesting nodes... writing: debug/nodes/1/status.json
using SQL connection URL for node 1: postgresql://...
retrieving SQL data for crdb_internal.feature_usage... writing: debug/nodes/1/crdb_internal.feature_usage.txt
retrieving SQL data for crdb_internal.gossip_alerts... writing: debug/nodes/1/crdb_internal.gossip_alerts.txt
retrieving SQL data for crdb_internal.gossip_liveness... writing: debug/nodes/1/crdb_internal.gossip_liveness.txt
retrieving SQL data for crdb_internal.gossip_network... writing: debug/nodes/1/crdb_internal.gossip_network.txt
retrieving SQL data for crdb_internal.gossip_nodes... writing: debug/nodes/1/crdb_internal.gossip_nodes.txt
retrieving SQL data for crdb_internal.leases... writing: debug/nodes/1/crdb_internal.leases.txt
retrieving SQL data for crdb_internal.node_build_info... writing: debug/nodes/1/crdb_internal.node_build_info.txt
retrieving SQL data for crdb_internal.node_metrics... writing: debug/nodes/1/crdb_internal.node_metrics.txt
retrieving SQL data for crdb_internal.node_queries... writing: debug/nodes/1/crdb_internal.node_queries.txt
retrieving SQL data for crdb_internal.node_runtime_info... writing: debug/nodes/1/crdb_internal.node_runtime_info.txt
retrieving SQL data for crdb_internal.node_sessions... writing: debug/nodes/1/crdb_internal.node_sessions.txt
retrieving SQL data for crdb_internal.node_statement_statistics... writing: debug/nodes/1/crdb_internal.node_statement_statistics.txt
retrieving SQL data for crdb_internal.node_txn_stats... writing: debug/nodes/1/crdb_internal.node_txn_stats.txt
requesting data for debug/nodes/1/details... writing: debug/nodes/1/details.json
requesting data for debug/nodes/1/gossip... writing: debug/nodes/1/gossip.json
requesting data for debug/nodes/1/enginestats... writing: debug/nodes/1/enginestats.json
requesting stacks for node 1... writing: debug/nodes/1/stacks.txt
requesting heap profile for node 1... writing: debug/nodes/1/heap.pprof
requesting heap files for node 1... 0 found
requesting goroutine files for node 1... 0 found
requesting log file ...
requesting ranges... 28 found
writing: debug/nodes/1/ranges/1.json
writing: debug/nodes/1/ranges/2.json
writing: debug/nodes/1/ranges/3.json
writing: debug/nodes/1/ranges/4.json
writing: debug/nodes/1/ranges/5.json
writing: debug/nodes/1/ranges/6.json
writing: debug/nodes/1/ranges/7.json
writing: debug/nodes/1/ranges/8.json
writing: debug/nodes/1/ranges/9.json
writing: debug/nodes/1/ranges/10.json
writing: debug/nodes/1/ranges/11.json
writing: debug/nodes/1/ranges/12.json
writing: debug/nodes/1/ranges/13.json
writing: debug/nodes/1/ranges/14.json
writing: debug/nodes/1/ranges/15.json
writing: debug/nodes/1/ranges/16.json
writing: debug/nodes/1/ranges/17.json
writing: debug/nodes/1/ranges/18.json
writing: debug/nodes/1/ranges/19.json
writing: debug/nodes/1/ranges/20.json
writing: debug/nodes/1/ranges/21.json
writing: debug/nodes/1/ranges/22.json
writing: debug/nodes/1/ranges/23.json
writing: debug/nodes/1/ranges/24.json
writing: debug/nodes/1/ranges/25.json
writing: debug/nodes/1/ranges/26.json
writing: debug/nodes/1/ranges/27.json
writing: debug/nodes/1/ranges/28.json
requesting list of SQL databases... 3 found
requesting database details for defaultdb... writing: debug/schema/defaultdb@details.json
0 tables found
requesting database details for postgres... writing: debug/schema/postgres@details.json
0 tables found
requesting database details for system... writing: debug/schema/system@details.json
22 tables found
requesting table details for system.comments... writing: debug/schema/system/comments.json
requesting table details for system.descriptor... writing: debug/schema/system/descriptor.json
requesting table details for system.eventlog... writing: debug/schema/system/eventlog.json
requesting table details for system.jobs... writing: debug/schema/system/jobs.json
requesting table details for system.lease... writing: debug/schema/system/lease.json
requesting table details for system.locations... writing: debug/schema/system/locations.json
requesting table details for system.namespace... writing: debug/schema/system/namespace.json
requesting table details for system.namespace_deprecated... writing: debug/schema/system/namespace_deprecated.json
requesting table details for system.protected_ts_meta... writing: debug/schema/system/protected_ts_meta.json
requesting table details for system.protected_ts_records... writing: debug/schema/system/protected_ts_records.json
requesting table details for system.rangelog... writing: debug/schema/system/rangelog.json
requesting table details for system.replication_constraint_stats... writing: debug/schema/system/replication_constraint_stats.json
requesting table details for system.replication_critical_localities... writing: debug/schema/system/replication_critical_localities.json
requesting table details for system.replication_stats... writing: debug/schema/system/replication_stats.json
requesting table details for system.reports_meta... writing: debug/schema/system/reports_meta.json
requesting table details for system.role_members... writing: debug/schema/system/role_members.json
requesting table details for system.settings... writing: debug/schema/system/settings.json
requesting table details for system.table_statistics... writing: debug/schema/system/table_statistics.json
requesting table details for system.ui... writing: debug/schema/system/ui.json
requesting table details for system.users... writing: debug/schema/system/users.json
requesting table details for system.web_sessions... writing: debug/schema/system/web_sessions.json
requesting table details for system.zones... writing: debug/schema/system/zones.json
`

	assert.Equal(t, expected, out)
}

func TestZipSpecialNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := newCLITest(cliTestParams{
		storeSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", `
create database "a:b";
create database "a-b";
create database "a-b-1";
create database "SYSTEM";
create table "SYSTEM.JOBS"(x int);
create database "../system";
create table defaultdb."a:b"(x int);
create table defaultdb."a-b"(x int);
create table defaultdb."pg_catalog.pg_class"(x int);
create table defaultdb."../system"(x int);
`})

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	re := regexp.MustCompile(`(?m)^.*(table|database).*$`)
	out = strings.Join(re.FindAllString(out, -1), "\n")

	const expected = `requesting list of SQL databases... 8 found
requesting database details for ../system... writing: debug/schema/___system@details.json
0 tables found
requesting database details for SYSTEM... writing: debug/schema/system@details.json
0 tables found
requesting database details for a-b... writing: debug/schema/a_b@details.json
0 tables found
requesting database details for a-b-1... writing: debug/schema/a_b_1@details.json
0 tables found
requesting database details for a:b... writing: debug/schema/a_b-1@details.json
0 tables found
requesting database details for defaultdb... writing: debug/schema/defaultdb@details.json
5 tables found
requesting table details for defaultdb.../system... writing: debug/schema/defaultdb/___system.json
requesting table details for defaultdb.SYSTEM.JOBS... writing: debug/schema/defaultdb/system_jobs.json
requesting table details for defaultdb.a-b... writing: debug/schema/defaultdb/a_b.json
requesting table details for defaultdb.a:b... writing: debug/schema/defaultdb/a_b-1.json
requesting table details for defaultdb.pg_catalog.pg_class... writing: debug/schema/defaultdb/pg_catalog_pg_class.json
requesting database details for postgres... writing: debug/schema/postgres@details.json
0 tables found
requesting database details for system... writing: debug/schema/system-1@details.json
22 tables found
requesting table details for system.comments... writing: debug/schema/system-1/comments.json
requesting table details for system.descriptor... writing: debug/schema/system-1/descriptor.json
requesting table details for system.eventlog... writing: debug/schema/system-1/eventlog.json
requesting table details for system.jobs... writing: debug/schema/system-1/jobs.json
requesting table details for system.lease... writing: debug/schema/system-1/lease.json
requesting table details for system.locations... writing: debug/schema/system-1/locations.json
requesting table details for system.namespace... writing: debug/schema/system-1/namespace.json
requesting table details for system.namespace_deprecated... writing: debug/schema/system-1/namespace_deprecated.json
requesting table details for system.protected_ts_meta... writing: debug/schema/system-1/protected_ts_meta.json
requesting table details for system.protected_ts_records... writing: debug/schema/system-1/protected_ts_records.json
requesting table details for system.rangelog... writing: debug/schema/system-1/rangelog.json
requesting table details for system.replication_constraint_stats... writing: debug/schema/system-1/replication_constraint_stats.json
requesting table details for system.replication_critical_localities... writing: debug/schema/system-1/replication_critical_localities.json
requesting table details for system.replication_stats... writing: debug/schema/system-1/replication_stats.json
requesting table details for system.reports_meta... writing: debug/schema/system-1/reports_meta.json
requesting table details for system.role_members... writing: debug/schema/system-1/role_members.json
requesting table details for system.settings... writing: debug/schema/system-1/settings.json
requesting table details for system.table_statistics... writing: debug/schema/system-1/table_statistics.json
requesting table details for system.ui... writing: debug/schema/system-1/ui.json
requesting table details for system.users... writing: debug/schema/system-1/users.json
requesting table details for system.web_sessions... writing: debug/schema/system-1/web_sessions.json
requesting table details for system.zones... writing: debug/schema/system-1/zones.json`

	assert.Equal(t, expected, out)
}

// This tests the operation of zip over unavailable clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestUnavailableZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}
	if util.RaceEnabled {
		// Race builds make the servers so slow that they report spurious
		// unavailability.
		t.Skip("not running under race")
	}

	// unavailableCh is used by the replica command filter
	// to conditionally block requests and simulate unavailability.
	var unavailableCh atomic.Value
	closedCh := make(chan struct{})
	close(closedCh)
	unavailableCh.Store(closedCh)
	knobs := &storage.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, _ roachpb.BatchRequest) *roachpb.Error {
			select {
			case <-unavailableCh.Load().(chan struct{}):
			case <-ctx.Done():
			}
			return nil
		},
	}

	// Make a 2-node cluster, with an option to make the first node unavailable.
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Insecure: true, Knobs: base.TestingKnobs{Store: knobs}},
			1: {Insecure: true},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	// Sanity test: check that a simple operation works.
	if _, err := tc.ServerConn(0).Exec("SELECT * FROM system.users"); err != nil {
		t.Fatal(err)
	}

	// Make the first two nodes unavailable.
	ch := make(chan struct{})
	unavailableCh.Store(ch)
	defer close(ch)

	// Zip it. We fake a CLI test context for this.
	c := cliTest{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
	}
	stderr = os.Stdout
	defer func() { stderr = log.OrigStderr }()

	// Keep the timeout short so that the test doesn't take forever.
	out, err := c.RunWithCapture("debug zip " + os.DevNull + " --timeout=.5s")
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages:
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)log file.*$`)
	out = re.ReplaceAllString(out, `log file ...`)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)
	re = regexp.MustCompile(`(?m)\^- resulted in.*$`)
	out = re.ReplaceAllString(out, `^- resulted in ...`)

	// In order to avoid non-determinism here, we erase the output of
	// the range retrieval.
	re = regexp.MustCompile(`(?m)^(requesting ranges.*found|writing: debug/nodes/\d+/ranges).*\n`)
	out = re.ReplaceAllString(out, ``)

	const expected = `debug zip ` + os.DevNull + ` --timeout=.5s
establishing RPC connection to ...
retrieving the node status to get the SQL address...
using SQL address: ...
using SQL connection URL: postgresql://...
writing /dev/null
requesting data for debug/events... writing: debug/events.json
  ^- resulted in ...
requesting data for debug/rangelog... writing: debug/rangelog.json
  ^- resulted in ...
requesting data for debug/liveness... writing: debug/liveness.json
requesting data for debug/settings... writing: debug/settings.json
requesting data for debug/reports/problemranges... writing: debug/reports/problemranges.json
retrieving SQL data for crdb_internal.cluster_queries... writing: debug/crdb_internal.cluster_queries.txt
retrieving SQL data for crdb_internal.cluster_sessions... writing: debug/crdb_internal.cluster_sessions.txt
retrieving SQL data for crdb_internal.cluster_settings... writing: debug/crdb_internal.cluster_settings.txt
retrieving SQL data for crdb_internal.jobs... writing: debug/crdb_internal.jobs.txt
  ^- resulted in ...
retrieving SQL data for system.jobs... writing: debug/system.jobs.txt
  ^- resulted in ...
retrieving SQL data for system.descriptor... writing: debug/system.descriptor.txt
  ^- resulted in ...
retrieving SQL data for system.namespace... writing: debug/system.namespace.txt
  ^- resulted in ...
retrieving SQL data for system.namespace_deprecated... writing: debug/system.namespace_deprecated.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.kv_node_status... writing: debug/crdb_internal.kv_node_status.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.kv_store_status... writing: debug/crdb_internal.kv_store_status.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.schema_changes... writing: debug/crdb_internal.schema_changes.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.partitions... writing: debug/crdb_internal.partitions.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.zones... writing: debug/crdb_internal.zones.txt
  ^- resulted in ...
requesting nodes... writing: debug/nodes
  ^- resulted in ...
writing: debug/nodes/1/status.json
using SQL connection URL for node 1: postgresql://...
retrieving SQL data for crdb_internal.feature_usage... writing: debug/nodes/1/crdb_internal.feature_usage.txt
retrieving SQL data for crdb_internal.gossip_alerts... writing: debug/nodes/1/crdb_internal.gossip_alerts.txt
retrieving SQL data for crdb_internal.gossip_liveness... writing: debug/nodes/1/crdb_internal.gossip_liveness.txt
retrieving SQL data for crdb_internal.gossip_network... writing: debug/nodes/1/crdb_internal.gossip_network.txt
retrieving SQL data for crdb_internal.gossip_nodes... writing: debug/nodes/1/crdb_internal.gossip_nodes.txt
retrieving SQL data for crdb_internal.leases... writing: debug/nodes/1/crdb_internal.leases.txt
retrieving SQL data for crdb_internal.node_build_info... writing: debug/nodes/1/crdb_internal.node_build_info.txt
retrieving SQL data for crdb_internal.node_metrics... writing: debug/nodes/1/crdb_internal.node_metrics.txt
retrieving SQL data for crdb_internal.node_queries... writing: debug/nodes/1/crdb_internal.node_queries.txt
retrieving SQL data for crdb_internal.node_runtime_info... writing: debug/nodes/1/crdb_internal.node_runtime_info.txt
retrieving SQL data for crdb_internal.node_sessions... writing: debug/nodes/1/crdb_internal.node_sessions.txt
retrieving SQL data for crdb_internal.node_statement_statistics... writing: debug/nodes/1/crdb_internal.node_statement_statistics.txt
retrieving SQL data for crdb_internal.node_txn_stats... writing: debug/nodes/1/crdb_internal.node_txn_stats.txt
requesting data for debug/nodes/1/details... writing: debug/nodes/1/details.json
requesting data for debug/nodes/1/gossip... writing: debug/nodes/1/gossip.json
requesting data for debug/nodes/1/enginestats... writing: debug/nodes/1/enginestats.json
requesting stacks for node 1... writing: debug/nodes/1/stacks.txt
requesting heap profile for node 1... writing: debug/nodes/1/heap.pprof
requesting heap files for node 1... 0 found
requesting goroutine files for node 1... 0 found
requesting log file ...
requesting list of SQL databases... writing: debug/schema
  ^- resulted in ...
`
	assert.Equal(t, expected, out)
}

// This tests the operation of zip over partial clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestPartialZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}
	if util.RaceEnabled {
		// We want a low timeout so that the test doesn't take forever;
		// however low timeouts make race runs flaky with false positives.
		t.Skip("not running under race")
	}

	ctx := context.Background()

	// Three nodes. We want to see what `zip` thinks when one of the nodes is down.
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{Insecure: true}})
	defer tc.Stopper().Stop(ctx)

	// Switch off the second node.
	tc.StopServer(1)

	// Zip it. We fake a CLI test context for this.
	c := cliTest{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
	}
	stderr = os.Stdout
	defer func() { stderr = log.OrigStderr }()

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages:
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)log file.*$`)
	out = re.ReplaceAllString(out, `log file ...`)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)
	re = regexp.MustCompile(`(?m)\^- resulted in.*$`)
	out = re.ReplaceAllString(out, `^- resulted in ...`)

	const expected = `debug zip ` + os.DevNull + `
establishing RPC connection to ...
retrieving the node status to get the SQL address...
using SQL address: ...
using SQL connection URL: postgresql://...
writing ` + os.DevNull + `
requesting data for debug/events... writing: debug/events.json
requesting data for debug/rangelog... writing: debug/rangelog.json
requesting data for debug/liveness... writing: debug/liveness.json
requesting data for debug/settings... writing: debug/settings.json
requesting data for debug/reports/problemranges... writing: debug/reports/problemranges.json
retrieving SQL data for crdb_internal.cluster_queries... writing: debug/crdb_internal.cluster_queries.txt
retrieving SQL data for crdb_internal.cluster_sessions... writing: debug/crdb_internal.cluster_sessions.txt
retrieving SQL data for crdb_internal.cluster_settings... writing: debug/crdb_internal.cluster_settings.txt
retrieving SQL data for crdb_internal.jobs... writing: debug/crdb_internal.jobs.txt
retrieving SQL data for system.jobs... writing: debug/system.jobs.txt
retrieving SQL data for system.descriptor... writing: debug/system.descriptor.txt
retrieving SQL data for system.namespace... writing: debug/system.namespace.txt
retrieving SQL data for system.namespace_deprecated... writing: debug/system.namespace_deprecated.txt
retrieving SQL data for crdb_internal.kv_node_status... writing: debug/crdb_internal.kv_node_status.txt
retrieving SQL data for crdb_internal.kv_store_status... writing: debug/crdb_internal.kv_store_status.txt
retrieving SQL data for crdb_internal.schema_changes... writing: debug/crdb_internal.schema_changes.txt
retrieving SQL data for crdb_internal.partitions... writing: debug/crdb_internal.partitions.txt
retrieving SQL data for crdb_internal.zones... writing: debug/crdb_internal.zones.txt
requesting nodes... writing: debug/nodes/1/status.json
using SQL connection URL for node 1: postgresql://...
retrieving SQL data for crdb_internal.feature_usage... writing: debug/nodes/1/crdb_internal.feature_usage.txt
retrieving SQL data for crdb_internal.gossip_alerts... writing: debug/nodes/1/crdb_internal.gossip_alerts.txt
retrieving SQL data for crdb_internal.gossip_liveness... writing: debug/nodes/1/crdb_internal.gossip_liveness.txt
retrieving SQL data for crdb_internal.gossip_network... writing: debug/nodes/1/crdb_internal.gossip_network.txt
retrieving SQL data for crdb_internal.gossip_nodes... writing: debug/nodes/1/crdb_internal.gossip_nodes.txt
retrieving SQL data for crdb_internal.leases... writing: debug/nodes/1/crdb_internal.leases.txt
retrieving SQL data for crdb_internal.node_build_info... writing: debug/nodes/1/crdb_internal.node_build_info.txt
retrieving SQL data for crdb_internal.node_metrics... writing: debug/nodes/1/crdb_internal.node_metrics.txt
retrieving SQL data for crdb_internal.node_queries... writing: debug/nodes/1/crdb_internal.node_queries.txt
retrieving SQL data for crdb_internal.node_runtime_info... writing: debug/nodes/1/crdb_internal.node_runtime_info.txt
retrieving SQL data for crdb_internal.node_sessions... writing: debug/nodes/1/crdb_internal.node_sessions.txt
retrieving SQL data for crdb_internal.node_statement_statistics... writing: debug/nodes/1/crdb_internal.node_statement_statistics.txt
retrieving SQL data for crdb_internal.node_txn_stats... writing: debug/nodes/1/crdb_internal.node_txn_stats.txt
requesting data for debug/nodes/1/details... writing: debug/nodes/1/details.json
requesting data for debug/nodes/1/gossip... writing: debug/nodes/1/gossip.json
requesting data for debug/nodes/1/enginestats... writing: debug/nodes/1/enginestats.json
requesting stacks for node 1... writing: debug/nodes/1/stacks.txt
requesting heap profile for node 1... writing: debug/nodes/1/heap.pprof
requesting heap files for node 1... 0 found
requesting goroutine files for node 1... 0 found
requesting log file ...
requesting ranges... 28 found
writing: debug/nodes/1/ranges/1.json
writing: debug/nodes/1/ranges/2.json
writing: debug/nodes/1/ranges/3.json
writing: debug/nodes/1/ranges/4.json
writing: debug/nodes/1/ranges/5.json
writing: debug/nodes/1/ranges/6.json
writing: debug/nodes/1/ranges/7.json
writing: debug/nodes/1/ranges/8.json
writing: debug/nodes/1/ranges/9.json
writing: debug/nodes/1/ranges/10.json
writing: debug/nodes/1/ranges/11.json
writing: debug/nodes/1/ranges/12.json
writing: debug/nodes/1/ranges/13.json
writing: debug/nodes/1/ranges/14.json
writing: debug/nodes/1/ranges/15.json
writing: debug/nodes/1/ranges/16.json
writing: debug/nodes/1/ranges/17.json
writing: debug/nodes/1/ranges/18.json
writing: debug/nodes/1/ranges/19.json
writing: debug/nodes/1/ranges/20.json
writing: debug/nodes/1/ranges/21.json
writing: debug/nodes/1/ranges/22.json
writing: debug/nodes/1/ranges/23.json
writing: debug/nodes/1/ranges/24.json
writing: debug/nodes/1/ranges/25.json
writing: debug/nodes/1/ranges/26.json
writing: debug/nodes/1/ranges/27.json
writing: debug/nodes/1/ranges/28.json
writing: debug/nodes/2/status.json
using SQL connection URL for node 2: postgresql://...
retrieving SQL data for crdb_internal.feature_usage... writing: debug/nodes/2/crdb_internal.feature_usage.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.gossip_alerts... writing: debug/nodes/2/crdb_internal.gossip_alerts.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.gossip_liveness... writing: debug/nodes/2/crdb_internal.gossip_liveness.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.gossip_network... writing: debug/nodes/2/crdb_internal.gossip_network.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.gossip_nodes... writing: debug/nodes/2/crdb_internal.gossip_nodes.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.leases... writing: debug/nodes/2/crdb_internal.leases.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_build_info... writing: debug/nodes/2/crdb_internal.node_build_info.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_metrics... writing: debug/nodes/2/crdb_internal.node_metrics.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_queries... writing: debug/nodes/2/crdb_internal.node_queries.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_runtime_info... writing: debug/nodes/2/crdb_internal.node_runtime_info.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_sessions... writing: debug/nodes/2/crdb_internal.node_sessions.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_statement_statistics... writing: debug/nodes/2/crdb_internal.node_statement_statistics.txt
  ^- resulted in ...
retrieving SQL data for crdb_internal.node_txn_stats... writing: debug/nodes/2/crdb_internal.node_txn_stats.txt
  ^- resulted in ...
requesting data for debug/nodes/2/details... writing: debug/nodes/2/details.json
  ^- resulted in ...
requesting data for debug/nodes/2/gossip... writing: debug/nodes/2/gossip.json
  ^- resulted in ...
requesting data for debug/nodes/2/enginestats... writing: debug/nodes/2/enginestats.json
  ^- resulted in ...
requesting stacks for node 2... writing: debug/nodes/2/stacks.txt
  ^- resulted in ...
requesting heap profile for node 2... writing: debug/nodes/2/heap.pprof
  ^- resulted in ...
requesting heap files for node 2... writing: debug/nodes/2/heapprof
  ^- resulted in ...
requesting goroutine files for node 2... writing: debug/nodes/2/goroutines
  ^- resulted in ...
requesting log file ...
  ^- resulted in ...
requesting ranges... writing: debug/nodes/2/ranges
  ^- resulted in ...
writing: debug/nodes/3/status.json
using SQL connection URL for node 3: postgresql://...
retrieving SQL data for crdb_internal.feature_usage... writing: debug/nodes/3/crdb_internal.feature_usage.txt
retrieving SQL data for crdb_internal.gossip_alerts... writing: debug/nodes/3/crdb_internal.gossip_alerts.txt
retrieving SQL data for crdb_internal.gossip_liveness... writing: debug/nodes/3/crdb_internal.gossip_liveness.txt
retrieving SQL data for crdb_internal.gossip_network... writing: debug/nodes/3/crdb_internal.gossip_network.txt
retrieving SQL data for crdb_internal.gossip_nodes... writing: debug/nodes/3/crdb_internal.gossip_nodes.txt
retrieving SQL data for crdb_internal.leases... writing: debug/nodes/3/crdb_internal.leases.txt
retrieving SQL data for crdb_internal.node_build_info... writing: debug/nodes/3/crdb_internal.node_build_info.txt
retrieving SQL data for crdb_internal.node_metrics... writing: debug/nodes/3/crdb_internal.node_metrics.txt
retrieving SQL data for crdb_internal.node_queries... writing: debug/nodes/3/crdb_internal.node_queries.txt
retrieving SQL data for crdb_internal.node_runtime_info... writing: debug/nodes/3/crdb_internal.node_runtime_info.txt
retrieving SQL data for crdb_internal.node_sessions... writing: debug/nodes/3/crdb_internal.node_sessions.txt
retrieving SQL data for crdb_internal.node_statement_statistics... writing: debug/nodes/3/crdb_internal.node_statement_statistics.txt
retrieving SQL data for crdb_internal.node_txn_stats... writing: debug/nodes/3/crdb_internal.node_txn_stats.txt
requesting data for debug/nodes/3/details... writing: debug/nodes/3/details.json
requesting data for debug/nodes/3/gossip... writing: debug/nodes/3/gossip.json
requesting data for debug/nodes/3/enginestats... writing: debug/nodes/3/enginestats.json
requesting stacks for node 3... writing: debug/nodes/3/stacks.txt
requesting heap profile for node 3... writing: debug/nodes/3/heap.pprof
requesting heap files for node 3... 0 found
requesting goroutine files for node 3... 0 found
requesting log file ...
requesting ranges... 28 found
writing: debug/nodes/3/ranges/1.json
writing: debug/nodes/3/ranges/2.json
writing: debug/nodes/3/ranges/3.json
writing: debug/nodes/3/ranges/4.json
writing: debug/nodes/3/ranges/5.json
writing: debug/nodes/3/ranges/6.json
writing: debug/nodes/3/ranges/7.json
writing: debug/nodes/3/ranges/8.json
writing: debug/nodes/3/ranges/9.json
writing: debug/nodes/3/ranges/10.json
writing: debug/nodes/3/ranges/11.json
writing: debug/nodes/3/ranges/12.json
writing: debug/nodes/3/ranges/13.json
writing: debug/nodes/3/ranges/14.json
writing: debug/nodes/3/ranges/15.json
writing: debug/nodes/3/ranges/16.json
writing: debug/nodes/3/ranges/17.json
writing: debug/nodes/3/ranges/18.json
writing: debug/nodes/3/ranges/19.json
writing: debug/nodes/3/ranges/20.json
writing: debug/nodes/3/ranges/21.json
writing: debug/nodes/3/ranges/22.json
writing: debug/nodes/3/ranges/23.json
writing: debug/nodes/3/ranges/24.json
writing: debug/nodes/3/ranges/25.json
writing: debug/nodes/3/ranges/26.json
writing: debug/nodes/3/ranges/27.json
writing: debug/nodes/3/ranges/28.json
requesting list of SQL databases... 3 found
requesting database details for defaultdb... writing: debug/schema/defaultdb@details.json
0 tables found
requesting database details for postgres... writing: debug/schema/postgres@details.json
0 tables found
requesting database details for system... writing: debug/schema/system@details.json
22 tables found
requesting table details for system.comments... writing: debug/schema/system/comments.json
requesting table details for system.descriptor... writing: debug/schema/system/descriptor.json
requesting table details for system.eventlog... writing: debug/schema/system/eventlog.json
requesting table details for system.jobs... writing: debug/schema/system/jobs.json
requesting table details for system.lease... writing: debug/schema/system/lease.json
requesting table details for system.locations... writing: debug/schema/system/locations.json
requesting table details for system.namespace... writing: debug/schema/system/namespace.json
requesting table details for system.namespace_deprecated... writing: debug/schema/system/namespace_deprecated.json
requesting table details for system.protected_ts_meta... writing: debug/schema/system/protected_ts_meta.json
requesting table details for system.protected_ts_records... writing: debug/schema/system/protected_ts_records.json
requesting table details for system.rangelog... writing: debug/schema/system/rangelog.json
requesting table details for system.replication_constraint_stats... writing: debug/schema/system/replication_constraint_stats.json
requesting table details for system.replication_critical_localities... writing: debug/schema/system/replication_critical_localities.json
requesting table details for system.replication_stats... writing: debug/schema/system/replication_stats.json
requesting table details for system.reports_meta... writing: debug/schema/system/reports_meta.json
requesting table details for system.role_members... writing: debug/schema/system/role_members.json
requesting table details for system.settings... writing: debug/schema/system/settings.json
requesting table details for system.table_statistics... writing: debug/schema/system/table_statistics.json
requesting table details for system.ui... writing: debug/schema/system/ui.json
requesting table details for system.users... writing: debug/schema/system/users.json
requesting table details for system.web_sessions... writing: debug/schema/system/web_sessions.json
requesting table details for system.zones... writing: debug/schema/system/zones.json
`
	assert.Equal(t, expected, out)
}
