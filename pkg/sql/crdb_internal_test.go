// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAllNamesInternal tests system.namespace entries.
func TestGetAllNamesInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _ /* sqlDB */, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		batch := txn.NewBatch()
		batch.Put(catalogkeys.MakeObjectNameKey(keys.SystemSQLCodec, 999, 444, "bob"), 9999)
		batch.Put(catalogkeys.MakePublicObjectNameKey(keys.SystemSQLCodec, 1000, "alice"), 10000)
		return txn.CommitInBatch(ctx, batch)
	})
	require.NoError(t, err)

	names, err := sql.TestingGetAllNames(ctx, nil, s.InternalExecutor().(*sql.InternalExecutor))
	require.NoError(t, err)

	assert.Equal(t, descpb.NameInfo{ParentID: 999, ParentSchemaID: 444, Name: "bob"}, names[9999])
	assert.Equal(t, descpb.NameInfo{ParentID: 1000, ParentSchemaID: 29, Name: "alice"}, names[10000])
}

// TestRangeLocalityBasedOnNodeIDs tests that the replica_localities shown in crdb_internal.ranges
// are correct reflection of the localities of the stores in the range descriptor which
// is in the replicas column
func TestRangeLocalityBasedOnNodeIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// NodeID=1, StoreID=1
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "1"}}},
			},
			ReplicationMode: base.ReplicationAuto,
		},
	)
	defer tc.Stopper().Stop(ctx)
	assert.EqualValues(t, 1, tc.Servers[len(tc.Servers)-1].GetFirstStoreID())

	// Set to 2 so the next store id will be 3.
	assert.NoError(t, tc.Servers[0].DB().Put(ctx, keys.StoreIDGenerator, 2))

	// NodeID=2, StoreID=3
	tc.AddAndStartServer(t,
		base.TestServerArgs{
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "2"}}},
		},
	)
	assert.EqualValues(t, 3, tc.Servers[len(tc.Servers)-1].GetFirstStoreID())

	// Set to 1 so the next store id will be 2.
	assert.NoError(t, tc.Servers[0].DB().Put(ctx, keys.StoreIDGenerator, 1))

	// NodeID=3, StoreID=2
	tc.AddAndStartServer(t,
		base.TestServerArgs{
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "3"}}},
		},
	)
	assert.EqualValues(t, 2, tc.Servers[len(tc.Servers)-1].GetFirstStoreID())
	assert.NoError(t, tc.WaitForFullReplication())

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	var replicas, localities string
	sqlDB.QueryRow(t, `select replicas, replica_localities from crdb_internal.ranges limit 1`).
		Scan(&replicas, &localities)

	assert.Equal(t, "{1,2,3}", replicas)
	// If range is represented as tuple of node ids then the result will be {node=1,node=2,node=3}.
	// If range is represented as tuple of store ids then the result will be {node=1,node=3,node=2}.
	assert.Equal(t, "{node=1,node=3,node=2}", localities)
}

func TestGossipAlertsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if err := s.GossipI().(*gossip.Gossip).AddInfoProto(gossip.MakeNodeHealthAlertKey(456), &statuspb.HealthCheckResult{
		Alerts: []statuspb.HealthAlert{{
			StoreID:     123,
			Category:    statuspb.HealthAlert_METRICS,
			Description: "foo",
			Value:       100.0,
		}},
	}, time.Hour); err != nil {
		t.Fatal(err)
	}

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT * FROM crdb_internal.gossip_alerts WHERE store_id = 123")
	if err != nil {
		t.Fatal(err)
	}

	if a, e := len(row), 5; a != e {
		t.Fatalf("got %d rows, wanted %d", a, e)
	}
	a := fmt.Sprintf("%v %v %v %v %v", row[0], row[1], row[2], row[3], row[4])
	e := "456 123 'metrics' 'foo' 100.0"
	if a != e {
		t.Fatalf("got:\n%s\nexpected:\n%s", a, e)
	}
}

// TestOldBitColumnMetadata checks that a pre-2.1 BIT columns
// shows up properly in metadata post-2.1.
func TestOldBitColumnMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT);
`); err != nil {
		t.Fatal(err)
	}

	// We now want to create a pre-2.1 table descriptor with an
	// old-style bit column. We're going to edit the table descriptor
	// manually, without going through SQL.
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
		kvDB, keys.SystemSQLCodec, "t", "test")
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].Name == "k" {
			tableDesc.Columns[i].Type.InternalType.VisibleType = 4 // Pre-2.1 BIT.
			tableDesc.Columns[i].Type.InternalType.Width = 12      // Arbitrary non-std INT size.
			break
		}
	}
	// To make this test future-proof we must ensure that there isn't
	// any logic in an unrelated place which will prevent the table from
	// being committed. To verify this, we add another column and check
	// it appears in introspection afterwards.
	//
	// We also avoid the regular schema change logic entirely, because
	// this may be equipped with code to "fix" the old-style BIT column
	// we defined above.
	alterCmd, err := parser.ParseOne("ALTER TABLE t ADD COLUMN z INT")
	if err != nil {
		t.Fatal(err)
	}
	colDef := alterCmd.AST.(*tree.AlterTable).Cmds[0].(*tree.AlterTableAddColumn).ColumnDef
	cdd, err := tabledesc.MakeColumnDefDescs(ctx, colDef, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	col := cdd.ColumnDescriptor
	col.ID = tableDesc.NextColumnID
	tableDesc.NextColumnID++
	tableDesc.Families[0].ColumnNames = append(tableDesc.Families[0].ColumnNames, col.Name)
	tableDesc.Families[0].ColumnIDs = append(tableDesc.Families[0].ColumnIDs, col.ID)
	tableDesc.Columns = append(tableDesc.Columns, *col)
	tableDesc.PrimaryIndex.StoreColumnIDs = append(tableDesc.PrimaryIndex.StoreColumnIDs, col.ID)
	tableDesc.PrimaryIndex.StoreColumnNames = append(tableDesc.PrimaryIndex.StoreColumnNames, col.Name)

	// Write the modified descriptor.
	if err := kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		return txn.Put(ctx, catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID), tableDesc.DescriptorProto())
	}); err != nil {
		t.Fatal(err)
	}

	// Read the column metadata from information_schema.
	rows, err := sqlDB.Query(`
SELECT column_name, character_maximum_length, numeric_precision, numeric_precision_radix, crdb_sql_type
  FROM t.information_schema.columns
 WHERE table_catalog = 't' AND table_schema = 'public' AND table_name = 'test'
   AND column_name != 'rowid'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	expected := 0
	for rows.Next() {
		var colName string
		var charMaxLength, numPrec, numPrecRadix pgtype.Int8
		var sqlType string
		if err := rows.Scan(&colName, &charMaxLength, &numPrec, &numPrecRadix, &sqlType); err != nil {
			t.Fatal(err)
		}
		switch colName {
		case "k":
			if charMaxLength.Status != pgtype.Null {
				t.Fatalf("x character_maximum_length: expected null, got %d", charMaxLength.Int)
			}
			if numPrec.Int != 64 {
				t.Fatalf("x numeric_precision: expected 64, got %v", numPrec.Get())
			}
			if numPrecRadix.Int != 2 {
				t.Fatalf("x numeric_precision_radix: expected 64, got %v", numPrecRadix.Get())
			}
			if sqlType != "INT8" {
				t.Fatalf("x crdb_sql_type: expected INT8, got %q", sqlType)
			}
			expected |= 2
		case "z":
			// This is just a canary to verify that the manually-modified
			// table descriptor is visible to introspection.
			expected |= 1
		default:
			t.Fatalf("unexpected col: %q", colName)
		}
	}
	if expected != 3 {
		t.Fatal("did not find both expected rows")
	}

	// Now test the workaround: using ALTER to "upgrade" the type fully to INT.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN k SET DATA TYPE INT8`); err != nil {
		t.Fatal(err)
	}

	// And verify that this has re-set the fields.
	tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	found := false
	for i := range tableDesc.Columns {
		col := &tableDesc.Columns[i]
		if col.Name == "k" {
			// TODO(knz): post-2.2, visible types for integer types are gone.
			if col.Type.InternalType.VisibleType != 0 {
				t.Errorf("unexpected visible type: got %d, expected 0", col.Type.InternalType.VisibleType)
			}
			if col.Type.Width() != 64 {
				t.Errorf("unexpected width: got %d, expected 64", col.Type.Width())
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("column disappeared")
	}
}

func TestClusterQueriesTxnData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1);
`); err != nil {
		t.Fatal(err)
	}

	txn, err := sqlDB.Begin()
	require.NoError(t, err)
	defer func() {
		_ = txn.Rollback()
	}()

	// Look up the schema first so only the read txn is recorded in
	// kv trace logs. We explicitly specify the schema to avoid an extra failed
	// lease acquisition, which occurs in a separate transaction, to work around
	// a current limitation in schema resolution. See #53301.
	if _, err := txn.Exec(`SELECT * FROM t.public.t`); err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Exec(
		`SET tracing=on,kv; SELECT * FROM t.public.t; SET TRACING=off`); err != nil {
		t.Fatal(err)
	}

	// The log messages we are looking for are structured like
	// [....,txn=<txnID>], so search for those and extract the id.
	row := txn.QueryRow(`
SELECT
	string_to_array(regexp_extract(tag, 'txn=[a-zA-Z0-9]*'), '=')[2]
FROM
	[SHOW KV TRACE FOR SESSION]
WHERE
  tag LIKE '%txn=%' LIMIT 1`)
	var txnID string
	if err := row.Scan(&txnID); err != nil {
		t.Fatal(err)
	}

	// Now, run a SHOW QUERIES statement, in the same transaction.
	// The txn_id we find there should be the same as the one we parsed,
	// and the txn_start time should be before the start time of the statement.
	row = txn.QueryRow(`
SELECT
	txn_id, start
FROM
  crdb_internal.cluster_queries
WHERE
	query LIKE '%SHOW CLUSTER QUERIES%'`)

	var (
		foundTxnID string
		txnStart   time.Time
		queryStart time.Time
	)
	if err := row.Scan(&foundTxnID, &queryStart); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(foundTxnID, txnID) {
		t.Errorf("expected to find txn id with prefix %s, but found %s", txnID, foundTxnID)
	}

	// Find the transaction start time and ensure that the query started after it.
	row = txn.QueryRow(`SELECT start FROM crdb_internal.node_transactions WHERE id = $1`, foundTxnID)
	if err := row.Scan(&txnStart); err != nil {
		t.Fatal(err)
	}
	if txnStart.After(queryStart) {
		t.Error("expected txn to start before query")
	}
}

// TestInvalidObjects table descriptors that don't validate will show up in
// table `crdb_internal.invalid_objects`.
func TestInvalidObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	var id int
	var dbName, schemaName, objName, errStr string
	// No inconsistency should be found so this should return ErrNoRow.
	require.Error(t, sqlDB.QueryRow(`SELECT * FROM "".crdb_internal.invalid_objects`).
		Scan(&id, dbName, schemaName, objName, errStr))

	if _, err := sqlDB.Exec(`CREATE DATABASE t;
CREATE TABLE t.test (k INT8);
CREATE TABLE fktbl (id INT8 PRIMARY KEY);
CREATE TABLE tbl (
	customer INT8 NOT NULL REFERENCES fktbl (id)
);
CREATE TABLE nojob (k INT8);`); err != nil {
		t.Fatal(err)
	}

	databaseID := int(sqlutils.QueryDatabaseID(t, sqlDB, "t"))
	tableTID := int(sqlutils.QueryTableID(t, sqlDB, "t", "public", "test"))
	tableFkTblID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "fktbl"))
	tableTblID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "tbl"))
	tableNoJobID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "nojob"))

	// Now introduce some inconsistencies.
	if _, err := sqlDB.Exec(fmt.Sprintf(`
INSERT INTO system.users VALUES ('node', NULL, true);
GRANT node TO root;
DELETE FROM system.descriptor WHERE id = %d;
DELETE FROM system.descriptor WHERE id = %d;
SELECT
	crdb_internal.unsafe_upsert_descriptor(
		id,
		crdb_internal.json_to_pb(
			'cockroach.sql.sqlbase.Descriptor',
			json_set(
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				),
				ARRAY['table', 'mutationJobs'],
				jsonb_build_array(
					jsonb_build_object('job_id', 123456)
				),
				true
			)
		),
		true
	)
FROM
	system.descriptor
WHERE
	id = %d;
UPDATE system.namespace SET id = 12345 WHERE id = %d;
`, databaseID, tableFkTblID, tableNoJobID, tableTID)); err != nil {
		t.Fatal(err)
	}

	require.NoError(t, sqlDB.QueryRow(`SELECT id FROM system.descriptor ORDER BY id DESC LIMIT 1`).
		Scan(&id))
	require.Equal(t, tableNoJobID, id)

	rows, err := sqlDB.Query(`SELECT * FROM "".crdb_internal.invalid_objects`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &dbName, &schemaName, &objName, &errStr))
	require.Equal(t, tableTID, id)
	require.Equal(t, "", dbName)
	require.Equal(t, "", schemaName)
	require.Equal(t, fmt.Sprintf(`relation "test" (%d): referenced database ID %d: referenced descriptor not found`, tableTID, databaseID), errStr)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &dbName, &schemaName, &objName, &errStr))
	require.Equal(t, tableTID, id)
	require.Equal(t, "", dbName)
	require.Equal(t, "", schemaName)
	require.Equal(t, fmt.Sprintf(`relation "test" (%d): expected matching namespace entry value, instead found 12345`, tableTID), errStr)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &dbName, &schemaName, &objName, &errStr))
	require.Equal(t, tableTblID, id)
	require.Equal(t, "defaultdb", dbName)
	require.Equal(t, "public", schemaName)
	require.Equal(t, fmt.Sprintf(
		`relation "tbl" (%d): invalid foreign key: missing table=%d: referenced table ID %d: referenced descriptor not found`,
		tableTblID, tableFkTblID, tableFkTblID), errStr)

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &dbName, &schemaName, &objName, &errStr))
	require.Equal(t, tableNoJobID, id)
	require.Equal(t, "defaultdb", dbName)
	require.Equal(t, "public", schemaName)
	require.Equal(t, "nojob", objName)
	require.Equal(t, `mutation job 123456: job not found`, errStr)

	require.False(t, rows.Next())
}

func TestDistSQLFlowsVirtualTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	const gatewayNodeID = 0

	var queryRunningAtomic, stallAtomic int64
	unblock := make(chan struct{})

	// We'll populate the key for the knob after we create the table.
	var tableKey atomic.Value
	tableKey.Store(roachpb.Key(""))
	getTableKey := func() roachpb.Key { return tableKey.Load().(roachpb.Key) }
	spanFromKey := func(k roachpb.Key) roachpb.Span {
		return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	}
	getTableSpan := func() roachpb.Span { return spanFromKey(getTableKey()) }

	// Install a store filter which, if both queryRunningAtomic and stallAtomic
	// are 1, will block the scan requests until 'unblock' channel is closed.
	//
	// The filter is needed in order to pause the execution of the running flows
	// in order to observe them in the virtual tables.
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, req roachpb.BatchRequest) *roachpb.Error {
					if atomic.LoadInt64(&stallAtomic) == 1 {
						if req.IsSingleRequest() {
							scan, ok := req.Requests[0].GetInner().(*roachpb.ScanRequest)
							if ok && getTableSpan().ContainsKey(scan.Key) && atomic.LoadInt64(&queryRunningAtomic) == 1 {
								t.Logf("stalling on scan at %s and waiting for test to unblock...", scan.Key)
								<-unblock
							}
						}
					}
					return nil
				},
			},
		},
	}

	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      params,
	})
	defer tc.Stopper().Stop(context.Background())

	// Create a table with 3 rows, split them into 3 ranges with each node
	// having one.
	db := tc.ServerConn(gatewayNodeID)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		3,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)
	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT VALUES (1), (2)")
	sqlDB.Exec(
		t,
		fmt.Sprintf("ALTER TABLE test.foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 1), (ARRAY[%d], 2)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		),
	)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	tableID := sqlutils.QueryTableID(t, sqlDB.DB, "test", "public", "foo")
	tableKey.Store(execCfg.Codec.TablePrefix(tableID))

	const query = "SELECT * FROM test.foo"

	// When maxRunningFlows is 0, we expect the remote flows to be queued up and
	// the test query will error out; when it is 1, we block the execution of
	// running flows.
	for maxRunningFlows := range []int{0, 1} {
		t.Run(fmt.Sprintf("MaxRunningFlows=%d", maxRunningFlows), func(t *testing.T) {
			// Limit the execution of remote flows and shorten the timeout.
			const flowStreamTimeout = 1 // in seconds
			sqlDB.Exec(t, "SET CLUSTER SETTING sql.distsql.max_running_flows=$1", maxRunningFlows)
			sqlDB.Exec(t, "SET CLUSTER SETTING sql.distsql.flow_stream_timeout=$1", fmt.Sprintf("%ds", flowStreamTimeout))

			// Wait for all nodes to get the updated values of these cluster
			// settings.
			testutils.SucceedsSoon(t, func() error {
				for nodeID := 0; nodeID < numNodes; nodeID++ {
					conn := tc.ServerConn(nodeID)
					db := sqlutils.MakeSQLRunner(conn)
					var flows int
					db.QueryRow(t, "SHOW CLUSTER SETTING sql.distsql.max_running_flows").Scan(&flows)
					if flows != maxRunningFlows {
						return errors.New("old max_running_flows value")
					}
					var timeout string
					db.QueryRow(t, "SHOW CLUSTER SETTING sql.distsql.flow_stream_timeout").Scan(&timeout)
					if timeout != fmt.Sprintf("00:00:0%d", flowStreamTimeout) {
						return errors.Errorf("old flow_stream_timeout value")
					}
				}
				return nil
			})

			if maxRunningFlows == 1 {
				atomic.StoreInt64(&stallAtomic, 1)
				defer func() {
					atomic.StoreInt64(&stallAtomic, 0)
				}()
			}

			// Spin up a separate goroutine that will run the query. If
			// maxRunningFlows is 0, the query eventually will error out because
			// the remote flows don't connect in time; if maxRunningFlows is 1,
			// the query will succeed once we close 'unblock' channel.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				conn := tc.ServerConn(gatewayNodeID)
				atomic.StoreInt64(&queryRunningAtomic, 1)
				_, err := conn.ExecContext(ctx, query)
				atomic.StoreInt64(&queryRunningAtomic, 0)
				return err
			})

			t.Log("waiting for remote flows to be scheduled or run")
			testutils.SucceedsSoon(t, func() error {
				for idx, s := range []*distsql.ServerImpl{
					tc.Server(1).DistSQLServer().(*distsql.ServerImpl),
					tc.Server(2).DistSQLServer().(*distsql.ServerImpl),
				} {
					numQueued := s.NumRemoteFlowsInQueue()
					if numQueued != 1-maxRunningFlows {
						return errors.Errorf("%d flows are found in the queue of node %d, %d expected", numQueued, idx+1, 1-maxRunningFlows)
					}
					numRunning := s.NumRemoteRunningFlows()
					if numRunning != maxRunningFlows {
						return errors.Errorf("%d flows are found in the queue of node %d, %d expected", numRunning, idx+1, maxRunningFlows)
					}
				}
				return nil
			})

			t.Log("checking the virtual tables")
			const (
				clusterScope  = "cluster"
				nodeScope     = "node"
				runningStatus = "running"
				queuedStatus  = "queued"
			)
			getNum := func(db *sqlutils.SQLRunner, scope, status string) int {
				querySuffix := fmt.Sprintf("FROM crdb_internal.%s_distsql_flows WHERE status = '%s'", scope, status)
				// Check that all remote flows (if any) correspond to the
				// expected statement.
				stmts := db.QueryStr(t, "SELECT stmt "+querySuffix)
				for _, stmt := range stmts {
					require.Equal(t, query, stmt[0])
				}
				var num int
				db.QueryRow(t, "SELECT count(*) "+querySuffix).Scan(&num)
				return num
			}
			for nodeID := 0; nodeID < numNodes; nodeID++ {
				conn := tc.ServerConn(nodeID)
				db := sqlutils.MakeSQLRunner(conn)

				// Check cluster level table.
				expRunning, expQueued := 0, 2
				if maxRunningFlows == 1 {
					expRunning, expQueued = expQueued, expRunning
				}
				gotRunning, gotQueued := getNum(db, clusterScope, runningStatus), getNum(db, clusterScope, queuedStatus)
				if gotRunning != expRunning {
					t.Fatalf("unexpected output from cluster_distsql_flows on node %d (running=%d)", nodeID+1, gotRunning)
				}
				if maxRunningFlows == 1 {
					if gotQueued != expQueued {
						t.Fatalf("unexpected output from cluster_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
					}
				} else {
					if gotQueued > expQueued { // it's possible for the query to have already errored out
						t.Fatalf("unexpected output from cluster_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
					}
				}

				// Check node level table.
				if nodeID == gatewayNodeID {
					if getNum(db, nodeScope, runningStatus) != 0 || getNum(db, nodeScope, queuedStatus) != 0 {
						t.Fatal("unexpectedly non empty output from node_distsql_flows on the gateway")
					}
				} else {
					expRunning, expQueued = 0, 1
					if maxRunningFlows == 1 {
						expRunning, expQueued = expQueued, expRunning
					}
					gotRunning, gotQueued = getNum(db, nodeScope, runningStatus), getNum(db, nodeScope, queuedStatus)
					if gotRunning != expRunning {
						t.Fatalf("unexpected output from node_distsql_flows on node %d (running=%d)", nodeID+1, gotRunning)
					}
					if maxRunningFlows == 1 {
						if gotQueued != expQueued {
							t.Fatalf("unexpected output from node_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
						}
					} else {
						if gotQueued > expQueued { // it's possible for the query to have already errored out
							t.Fatalf("unexpected output from node_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
						}
					}
				}
			}

			if maxRunningFlows == 1 {
				// Unblock the scan requests.
				close(unblock)
			}

			t.Log("waiting for query to finish")
			err := g.Wait()
			if maxRunningFlows == 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// setupTraces takes two tracers (potentially on different nodes), and creates
// two span hierarchies as depicted below. The method returns the traceIDs for
// both these span hierarchies, along with a cleanup method to Finish() all the
// opened spans.
//
// Traces on node1:
// -------------
// root                            <-- traceID1
//   root.child                    <-- traceID1
//     root.child.detached_child   <-- traceID1
//
// Traces on node2:
// -------------
// root.child.remotechild			<-- traceID1
// root.child.remotechilddone		<-- traceID1
// root2												<-- traceID2
// 		root2.child								<-- traceID2
func setupTraces(t1, t2 *tracing.Tracer) (tracingpb.TraceID, func()) {
	// Start a root span on "node 1".
	root := t1.StartSpan("root", tracing.WithRecording(tracing.RecordingVerbose))

	time.Sleep(10 * time.Millisecond)

	// Start a child span on "node 1".
	child := t1.StartSpan("root.child", tracing.WithParent(root))

	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting is not deterministic.
	time.Sleep(10 * time.Millisecond)

	// Start a forked child span on "node 1".
	childDetachedChild := t1.StartSpan("root.child.detached_child", tracing.WithParent(child), tracing.WithDetachedRecording())

	// Start a remote child span on "node 2".
	childRemoteChild := t2.StartSpan("root.child.remotechild", tracing.WithRemoteParentFromSpanMeta(child.Meta()))

	time.Sleep(10 * time.Millisecond)

	// Start another remote child span on "node 2" that we finish.
	childRemoteChildFinished := t2.StartSpan("root.child.remotechilddone", tracing.WithRemoteParentFromSpanMeta(child.Meta()))
	child.ImportRemoteSpans(childRemoteChildFinished.FinishAndGetRecording(tracing.RecordingVerbose))

	// Start another remote child span on "node 2" that we finish. This will have
	// a different trace_id from the spans created above.
	root2 := t2.StartSpan("root2", tracing.WithRecording(tracing.RecordingVerbose))

	// Start a child span on "node 2".
	child2 := t2.StartSpan("root2.child", tracing.WithParent(root2))
	return root.TraceID(), func() {
		for _, span := range []*tracing.Span{root, child, childDetachedChild,
			childRemoteChild, root2, child2} {
			span.Finish()
		}
	}
}

func TestClusterInflightTracesVirtualTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	node1Tracer := tc.Server(0).TracerI().(*tracing.Tracer)
	node2Tracer := tc.Server(1).TracerI().(*tracing.Tracer)

	traceID, cleanup := setupTraces(node1Tracer, node2Tracer)
	defer cleanup()

	// The cluster_inflight_traces table is magic and only returns results when
	// the query contains an index constraint.

	t.Run("no-index-constraint", func(t *testing.T) {
		sqlDB.CheckQueryResults(t, `SELECT * from crdb_internal.cluster_inflight_traces`, [][]string{})
	})

	t.Run("with-index-constraint", func(t *testing.T) {
		// We expect there to be 3 tracing.Recordings rooted at
		// root and root.child.remotechild.
		expectedRows := []struct {
			traceID int
			nodeID  int
		}{
			{
				traceID: int(traceID),
				nodeID:  1,
			},
			{
				traceID: int(traceID),
				nodeID:  2,
			},
		}
		var rowIdx int
		rows := sqlDB.Query(t, `SELECT trace_id, node_id, trace_str, jaeger_json from crdb_internal.cluster_inflight_traces WHERE trace_id=$1`, traceID)
		defer rows.Close()
		for rows.Next() {
			var traceID, nodeID int
			var traceStr, jaegarJSON string
			require.NoError(t, rows.Scan(&traceID, &nodeID, &traceStr, &jaegarJSON))
			require.Less(t, rowIdx, len(expectedRows))
			expected := expectedRows[rowIdx]
			require.Equal(t, expected.traceID, traceID)
			require.Equal(t, expected.nodeID, nodeID)
			require.NotEmpty(t, traceStr)
			require.NotEmpty(t, jaegarJSON)
			rowIdx++
		}
	})
}

// TestInternalJobsTableRetryColumns tests values of last_run, next_run, and
// num_runs columns in crdb_internal.jobs table. The test creates a job in
// system.jobs table and retrieves the job's information from crdb_internal.jobs
// table for validation.
func TestInternalJobsTableRetryColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(validateFn func(context.Context, *sqlutils.SQLRunner)) func(t *testing.T) {
		return func(t *testing.T) {
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: &jobs.TestingKnobs{
						DisableAdoptions: true,
					},
				},
			})
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)
			tdb := sqlutils.MakeSQLRunner(db)

			tdb.Exec(t,
				"INSERT INTO system.jobs (id, status, created, payload) values ($1, $2, $3, 'test'::bytes)",
				1, jobs.StatusRunning, timeutil.Now(),
			)

			validateFn(ctx, tdb)
		}
	}

	t.Run("null values", testFn(func(_ context.Context, tdb *sqlutils.SQLRunner) {
		// Values should be NULL if not populated.
		tdb.CheckQueryResults(t, `
SELECT last_run IS NULL,
       next_run IS NOT NULL,
       num_runs = 0,
       execution_errors IS NULL
  FROM crdb_internal.jobs WHERE job_id = 1`,
			[][]string{{"true", "true", "true", "true"}})
	}))

	t.Run("valid backoff params", testFn(func(_ context.Context, tdb *sqlutils.SQLRunner) {
		lastRun := timeutil.Unix(1, 0)
		tdb.Exec(t, "UPDATE system.jobs SET last_run = $1, num_runs = 1 WHERE id = 1", lastRun)
		tdb.Exec(t, "SET CLUSTER SETTING jobs.registry.retry.initial_delay = '1s'")
		tdb.Exec(t, "SET CLUSTER SETTING jobs.registry.retry.max_delay = '1s'")

		var validLastRun, validNextRun, validNumRuns bool
		tdb.QueryRow(t,
			"SELECT last_run = $1, next_run = $2, num_runs = 1 FROM crdb_internal.jobs WHERE job_id = 1",
			lastRun, lastRun.Add(time.Second),
		).Scan(&validLastRun, &validNextRun, &validNumRuns)
		require.True(t, validLastRun)
		require.True(t, validNextRun)
		require.True(t, validNumRuns)
	}))
}

func TestIsAtLeastVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettings(),
		},
	})
	defer tc.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	for _, tc := range []struct {
		version  string
		expected string
		errorRE  string
	}{
		{version: "21.2", expected: "true"},
		{version: "99.2", expected: "false"},
		{version: "foo", errorRE: ".*invalid version.*"},
	} {
		query := fmt.Sprintf("SELECT crdb_internal.is_at_least_version('%s')", tc.version)
		if tc.errorRE != "" {
			db.ExpectErr(t, tc.errorRE, query)
		} else {
			db.CheckQueryResults(t, query, [][]string{{tc.expected}})
		}
	}
}
