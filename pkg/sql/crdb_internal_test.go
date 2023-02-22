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
	gosql "database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
		batch.Put(catalogkeys.EncodeNameKey(keys.SystemSQLCodec, &descpb.NameInfo{ParentID: 999, ParentSchemaID: 444, Name: "bob"}), 9999)
		batch.Put(catalogkeys.EncodeNameKey(keys.SystemSQLCodec, &descpb.NameInfo{ParentID: 1000, ParentSchemaID: 29, Name: "alice"}), 10000)
		return txn.CommitInBatch(ctx, batch)
	})
	require.NoError(t, err)

	var names map[descpb.ID]catalog.NameKey
	require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) (err error) {
		names, err = sql.TestingGetAllNames(ctx, txn)
		return err
	}))
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
		sessiondata.RootUserSessionDataOverride,
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
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	cdd, err := tabledesc.MakeColumnDefDescs(ctx, colDef, nil, evalCtx)
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

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// No inconsistency should be found.
	tdb.CheckQueryResults(t, `SELECT count(*) FROM "".crdb_internal.invalid_objects`, [][]string{{"0"}})

	// Create a database and some tables.
	tdb.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT8);
CREATE TABLE fktbl (id INT8 PRIMARY KEY);
CREATE TABLE tbl (
	customer INT8 NOT NULL REFERENCES fktbl (id)
);
CREATE TABLE nojob (k INT8);
	`)

	// Retrieve their IDs.
	databaseID := int(sqlutils.QueryDatabaseID(t, sqlDB, "t"))
	schemaID := int(sqlutils.QuerySchemaID(t, sqlDB, "t", "public"))
	tableTID := int(sqlutils.QueryTableID(t, sqlDB, "t", "public", "test"))
	tableFkTblID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "fktbl"))
	tableTblID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "tbl"))
	tableNoJobID := int(sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "nojob"))
	const fakeID = 12345

	// Now introduce some inconsistencies.
	tdb.Exec(t, fmt.Sprintf(`
INSERT INTO system.users VALUES ('node', NULL, true, 3);
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
UPDATE system.namespace SET id = %d WHERE id = %d;
	`, databaseID, tableFkTblID, tableNoJobID, fakeID, tableTID))

	tdb.CheckQueryResults(t, `SELECT id FROM system.descriptor ORDER BY id DESC LIMIT 1`, [][]string{
		{fmt.Sprintf("%d", tableNoJobID)},
	})

	tdb.CheckQueryResults(t, `SELECT * FROM "".crdb_internal.invalid_objects`, [][]string{
		{fmt.Sprintf("%d", tableTID), fmt.Sprintf("[%d]", databaseID), "public", "test",
			fmt.Sprintf(`relation "test" (%d): referenced database ID %d: referenced descriptor not found`, tableTID, databaseID),
		},
		{fmt.Sprintf("%d", tableTID), fmt.Sprintf("[%d]", databaseID), "public", "test",
			fmt.Sprintf(`relation "test" (%d): expected matching namespace entry value, instead found 12345`, tableTID),
		},
		{fmt.Sprintf("%d", tableTblID), "defaultdb", "public", "tbl",
			fmt.Sprintf(
				`relation "tbl" (%d): invalid foreign key: missing table=%d:`+
					` referenced table ID %d: referenced descriptor not found`,
				tableTblID, tableFkTblID, tableFkTblID),
		},
		{fmt.Sprintf("%d", tableNoJobID), "defaultdb", "public", "nojob",
			fmt.Sprintf(`relation "nojob" (%d): unknown mutation ID 0 associated with job ID 123456`, tableNoJobID),
		},
		{fmt.Sprintf("%d", tableNoJobID), "defaultdb", "public", "nojob", `mutation job 123456: job not found`},
		{fmt.Sprintf("%d", schemaID), fmt.Sprintf("[%d]", databaseID), "public", "",
			fmt.Sprintf(`schema "public" (%d): referenced database ID %d: referenced descriptor not found`, schemaID, databaseID),
		},
		{fmt.Sprintf("%d", databaseID), "t", "", "", `referenced descriptor not found`},
		{fmt.Sprintf("%d", tableFkTblID), "defaultdb", "public", "fktbl", `referenced descriptor not found`},
		{fmt.Sprintf("%d", fakeID), fmt.Sprintf("[%d]", databaseID), "public", "test", `referenced descriptor not found`},
	})
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
				TestingRequestFilter: func(_ context.Context, req *kvpb.BatchRequest) *kvpb.Error {
					if atomic.LoadInt64(&stallAtomic) == 1 {
						if req.IsSingleRequest() {
							scan, ok := req.Requests[0].GetInner().(*kvpb.ScanRequest)
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

	atomic.StoreInt64(&stallAtomic, 1)

	// Spin up a separate goroutine that will run the query. The query will
	// succeed once we close 'unblock' channel.
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

	t.Log("waiting for remote flows to be run")
	testutils.SucceedsSoon(t, func() error {
		for idx, s := range []*distsql.ServerImpl{
			tc.Server(1).DistSQLServer().(*distsql.ServerImpl),
			tc.Server(2).DistSQLServer().(*distsql.ServerImpl),
		} {
			numRunning := s.NumRemoteRunningFlows()
			if numRunning != 1 {
				return errors.Errorf("%d flows are found in the queue of node %d, %d expected", numRunning, idx+1, 1)
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
		// Check that all remote flows (if any) correspond to the expected
		// statement.
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
		expRunning, expQueued := 2, 0
		gotRunning, gotQueued := getNum(db, clusterScope, runningStatus), getNum(db, clusterScope, queuedStatus)
		if gotRunning != expRunning {
			t.Fatalf("unexpected output from cluster_distsql_flows on node %d (running=%d)", nodeID+1, gotRunning)
		}
		if gotQueued != expQueued {
			t.Fatalf("unexpected output from cluster_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
		}

		// Check node level table.
		if nodeID == gatewayNodeID {
			if getNum(db, nodeScope, runningStatus) != 0 || getNum(db, nodeScope, queuedStatus) != 0 {
				t.Fatal("unexpectedly non empty output from node_distsql_flows on the gateway")
			}
		} else {
			expRunning, expQueued = 1, 0
			gotRunning, gotQueued = getNum(db, nodeScope, runningStatus), getNum(db, nodeScope, queuedStatus)
			if gotRunning != expRunning {
				t.Fatalf("unexpected output from node_distsql_flows on node %d (running=%d)", nodeID+1, gotRunning)
			}
			if gotQueued != expQueued {
				t.Fatalf("unexpected output from node_distsql_flows on node %d (queued=%d)", nodeID+1, gotQueued)
			}
		}
	}

	// Unblock the scan requests.
	close(unblock)

	t.Log("waiting for query to finish")
	err := g.Wait()
	require.NoError(t, err)
}

// setupTraces takes two tracers (potentially on different nodes), and creates
// two span hierarchies as depicted below. The method returns the traceIDs for
// both these span hierarchies, along with a cleanup method to Finish() all the
// opened spans.
//
// Traces on node1:
// -------------
// root                            <-- traceID1
//
//	root.child                    <-- traceID1
//	  root.child.detached_child   <-- traceID1
//
// Traces on node2:
// -------------
// root.child.remotechild			<-- traceID1
// root.child.remotechilddone		<-- traceID1
// root2												<-- traceID2
//
//	root2.child								<-- traceID2
func setupTraces(t1, t2 *tracing.Tracer) (tracingpb.TraceID, func()) {
	// Start a root span on "node 1".
	root := t1.StartSpan("root", tracing.WithRecording(tracingpb.RecordingVerbose))

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
	child.ImportRemoteRecording(childRemoteChildFinished.FinishAndGetRecording(tracingpb.RecordingVerbose))

	// Start another remote child span on "node 2" that we finish. This will have
	// a different trace_id from the spans created above.
	root2 := t2.StartSpan("root2", tracing.WithRecording(tracingpb.RecordingVerbose))

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
		_, err := sqlDB.DB.ExecContext(ctx, `SELECT * from crdb_internal.cluster_inflight_traces`)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "a trace_id value needs to be specified")
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
					// DisableAdoptions needs this.
					UpgradeManager: &upgradebase.TestingKnobs{
						DontUseJobs: true,
					},
				},
			})
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)
			tdb := sqlutils.MakeSQLRunner(db)

			payload := jobspb.Payload{
				Details:       jobspb.WrapPayloadDetails(jobspb.ImportDetails{}),
				UsernameProto: username.RootUserName().EncodeProto(),
			}
			payloadBytes, err := protoutil.Marshal(&payload)
			assert.NoError(t, err)
			tdb.Exec(t,
				"INSERT INTO system.jobs (id, status, created, payload) values ($1, $2, $3, $4)",
				1, jobs.StatusRunning, timeutil.Now(), payloadBytes,
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
		{version: "1000099.2", expected: "false"},
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

func TestTxnContentionEventsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster. (One node is sufficient; the outliers system
	// is currently in-memory only.)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	testTxnContentionEventsTableHelper(t, ctx, conn, sqlDB)
}

func TestTxnContentionEventsTableMultiTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				// Test is designed to run with explicit tenants. No need to
				// implicitly create a tenant.
				DisableDefaultTestTenant: true,
			},
		})
	defer tc.Stopper().Stop(ctx)
	_, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
	})

	conn, err := tSQL.Conn(ctx)
	require.NoError(t, err)
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer tSQL.Close()

	testTxnContentionEventsTableHelper(t, ctx, tSQL, sqlDB)
}

func testTxnContentionEventsTableHelper(t *testing.T, ctx context.Context, conn *gosql.DB, sqlDB *sqlutils.SQLRunner) {
	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.metrics.statement_details.plan_collection.enabled = false;`)

	// Reduce the resolution interval to speed up the test.
	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '100ms'`)

	sqlDB.Exec(t, "CREATE TABLE t (id string, s string);")

	causeContention := func(insertValue string, updateValue string) {
		// Create a new connection, and then in a go routine have it start a
		// transaction, update a row, sleep for a time, and then complete the
		// transaction. With original connection attempt to update the same row
		// being updated concurrently in the separate go routine, this will be
		// blocked until the original transaction completes.
		var wgTxnStarted sync.WaitGroup
		wgTxnStarted.Add(1)

		// Lock to wait for the txn to complete to avoid the test finishing
		// before the txn is committed.
		var wgTxnDone sync.WaitGroup
		wgTxnDone.Add(1)

		go func() {
			defer wgTxnDone.Done()
			tx, errTxn := conn.BeginTx(ctx, &gosql.TxOptions{})
			require.NoError(t, errTxn)
			_, errTxn = tx.ExecContext(ctx,
				"INSERT INTO t (id, s) VALUES ('test', $1);",
				insertValue)
			require.NoError(t, errTxn)
			wgTxnStarted.Done()
			_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.5);")
			require.NoError(t, errTxn)
			errTxn = tx.Commit()
			require.NoError(t, errTxn)
		}()

		start := timeutil.Now()

		// Need to wait for the txn to start to ensure lock contention.
		wgTxnStarted.Wait()
		// This will be blocked until the updateRowWithDelay finishes.
		_, errUpdate := conn.ExecContext(
			ctx, "UPDATE t SET s = $1 where id = 'test';", updateValue)
		require.NoError(t, errUpdate)
		end := timeutil.Now()
		require.GreaterOrEqual(t, end.Sub(start), 500*time.Millisecond)

		wgTxnDone.Wait()
	}

	causeContention("insert1", "update1")
	causeContention("insert2", "update2")

	rowCount := 0

	// Verify the table content is valid.
	// Filter the fingerprint id to only be the query in the test.
	// This ensures the event is the one caused in the test and not by some other
	// internal workflow.
	testutils.SucceedsWithin(t, func() error {
		rows, errVerify := conn.QueryContext(ctx, `SELECT 
			blocking_txn_id, 
			waiting_txn_id,
			waiting_stmt_id,
			encode(
					 waiting_txn_fingerprint_id, 'hex'
			 ) AS waiting_txn_fingerprint_id,
			contending_pretty_key,
			database_name,
			schema_name,
			table_name,
			index_name
			FROM crdb_internal.transaction_contention_events tce 
			inner join ( 
			select
      fingerprint_id,
			transaction_fingerprint_id, 
			metadata->'query' as query 
			from crdb_internal.statement_statistics t 
			where metadata->>'query' like 'UPDATE t SET %') stats 
			on stats.transaction_fingerprint_id = tce.waiting_txn_fingerprint_id
			  and stats.fingerprint_id = tce.waiting_stmt_fingerprint_id`)
		if errVerify != nil {
			return errVerify
		}

		for rows.Next() {
			rowCount++

			var blockingTxnId, waitingTxnId, waitingStmtId, waitingStmtFingerprint string
			var prettyKey, dbName, schemaName, tableName, indexName string
			errVerify = rows.Scan(&blockingTxnId, &waitingTxnId, &waitingStmtId, &waitingStmtFingerprint, &prettyKey, &dbName, &schemaName, &tableName, &indexName)
			if errVerify != nil {
				return errVerify
			}

			const defaultIdString = "0x0000000000000000"
			if blockingTxnId == defaultIdString {
				return fmt.Errorf("transaction_contention_events had default txn blocking id %s, waiting txn id %s", blockingTxnId, waitingTxnId)
			}

			if waitingTxnId == defaultIdString {
				return fmt.Errorf("transaction_contention_events had default waiting txn id %s, blocking txn id %s", waitingTxnId, blockingTxnId)
			}

			if !strings.HasPrefix(prettyKey, "/Table/") {
				return fmt.Errorf("prettyKey should be defaultdb: %s, %s, %s, %s, %s", prettyKey, dbName, schemaName, tableName, indexName)
			}

			if dbName != "defaultdb" {
				return fmt.Errorf("dbName should be defaultdb: %s, %s, %s, %s, %s", prettyKey, dbName, schemaName, tableName, indexName)
			}

			if schemaName != "public" {
				return fmt.Errorf("schemaName should be public: %s, %s, %s, %s, %s", prettyKey, dbName, schemaName, tableName, indexName)
			}

			if tableName != "t" {
				return fmt.Errorf("tableName should be t: %s, %s, %s, %s, %s", prettyKey, dbName, schemaName, tableName, indexName)
			}

			if indexName != "t_pkey" {
				return fmt.Errorf("indexName should be t_pkey: %s, %s, %s, %s, %s", prettyKey, dbName, schemaName, tableName, indexName)
			}
		}

		if rowCount < 1 {
			return fmt.Errorf("transaction_contention_events did not return any rows")
		}
		return nil
	}, 5*time.Second)

	require.LessOrEqual(t, rowCount, 2, "transaction_contention_events "+
		"found 3 rows. It should only record first, but there is a chance based "+
		"on sampling to get 2 rows.")

}

// This test doesn't care about the contents of these virtual tables;
// other places (the insights integration tests) do that for us.
// What we look at here is the role-option-checking we need to make sure
// the current sql user has permission to read these tables at all.
// VIEWACTIVITY or VIEWACTIVITYREDACTED should be sufficient.
func TestExecutionInsights(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start the cluster.
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{Settings: settings}}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// We'll check both the cluster-wide table and the node-local one.
	virtualTables := []interface{}{
		"cluster_execution_insights",
		"node_execution_insights",
	}
	testutils.RunValues(t, "table", virtualTables, func(t *testing.T, table interface{}) {
		testCases := []struct {
			option  string
			granted bool
		}{
			{option: "VIEWACTIVITY", granted: true},
			{option: "VIEWACTIVITYREDACTED", granted: true},
			{option: "NOVIEWACTIVITY"},
			{option: "NOVIEWACTIVITYREDACTED"},
		}
		for _, testCase := range testCases {
			t.Run(fmt.Sprintf("option=%s", testCase.option), func(t *testing.T) {
				// Create a test user with the role option we're testing.
				sqlDB.Exec(t, fmt.Sprintf("CREATE USER testuser WITH %s", testCase.option))
				defer func() {
					sqlDB.Exec(t, "DROP USER testuser")
				}()

				// Connect to the cluster as the test user.
				pgUrl, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(),
					fmt.Sprintf("TestExecutionInsights-%s-%s", table, testCase.option),
					url.User("testuser"),
				)
				defer cleanup()
				db, err := gosql.Open("postgres", pgUrl.String())
				require.NoError(t, err)
				defer func() { _ = db.Close() }()

				// Try to read the virtual table, and see that we can or cannot as expected.
				rows, err := db.Query(fmt.Sprintf("SELECT count(*) FROM crdb_internal.%s", table))
				defer func() {
					if rows != nil {
						_ = rows.Close()
					}
				}()
				if testCase.granted {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		}

	})
}

var _ jobs.Resumer = &fakeResumer{}

type fakeResumer struct {
}

// Resume implements the jobs.Resumer interface.
func (*fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return nil
}

// Resume implements the jobs.Resumer interface.
func (*fakeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return jobErr
}

// TestInternalSystemJobsTableMirrorsSystemJobsTable asserts that
// entries created in system.jobs exactly match the results generated by
// crdb_internal.system_jobs. This test also asserts that the column names
// match.
func TestInternalSystemJobsTableMirrorsSystemJobsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Because this test modifies system.jobs and asserts its contents,
			// we should disable jobs from being adopted and disable automatic jobs
			// from being created.
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs: true,
			},
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true,
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	payload := jobspb.Payload{
		Details:       jobspb.WrapPayloadDetails(jobspb.ImportDetails{}),
		UsernameProto: username.RootUserName().EncodeProto(),
	}
	payloadBytes, err := protoutil.Marshal(&payload)
	assert.NoError(t, err)

	tdb.Exec(t,
		"INSERT INTO system.jobs (id, status, created, payload) values ($1, $2, $3, $4)",
		1, jobs.StatusRunning, timeutil.Now(), payloadBytes,
	)

	tdb.Exec(t,
		"INSERT INTO system.jobs values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
		2, jobs.StatusRunning, timeutil.Now(), payloadBytes, []byte("progress"), "created by", 2, []byte("claim session id"),
		2, 2, timeutil.Now(), jobspb.TypeImport.String(),
	)

	res := tdb.QueryStr(t, "SELECT * FROM system.jobs ORDER BY id")
	tdb.CheckQueryResults(t, `SELECT * FROM crdb_internal.system_jobs ORDER BY id`, res)
	tdb.CheckQueryResults(t, `
			SELECT id, status, created, payload, progress, created_by_type, created_by_id, claim_session_id, 
             claim_instance_id, num_runs, last_run, job_type
			FROM crdb_internal.system_jobs ORDER BY id`,
		res,
	)
	tdb.CheckQueryResults(t, `
			SELECT id, status, created, payload, progress, created_by_type, created_by_id, claim_session_id,
      claim_instance_id, num_runs, last_run, job_type
			FROM system.jobs ORDER BY id`,
		res,
	)
}

// TestInternalSystemJobsTableWorksWithVersionPreV23_1BackfillTypeColumnInJobsTable
// tests that crdb_internal.system_jobs and crdb_internal.jobs work when
// the server has a version pre-V23_1AddTypeColumnToJobsTable. In this version,
// the job_type column was added to the system.jobs table.
func TestInternalSystemJobsTableWorksWithVersionPreV23_1BackfillTypeColumnInJobsTable(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride: clusterversion.ByKey(
					clusterversion.V23_1BackfillTypeColumnInJobsTable - 1),
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t,
		"SELECT * FROM crdb_internal.jobs",
	)
	tdb.Exec(t,
		"SELECT * FROM crdb_internal.system_jobs",
	)
	// Exercise indexes.
	tdb.Exec(t,
		"SELECT * FROM crdb_internal.system_jobs WHERE job_type = 'CHANGEFEED'",
	)
	tdb.Exec(t,
		"SELECT * FROM crdb_internal.system_jobs WHERE id = 0",
	)
	tdb.Exec(t,
		"SELECT * FROM crdb_internal.system_jobs WHERE status = 'running'",
	)
}

// TestCorruptPayloadError asserts that we can an error
// with the correct hint when we fail to decode a payload.
func TestCorruptPayloadError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Because this test modifies system.jobs and asserts its contents,
			// we should disable jobs from being adopted and disable automatic jobs
			// from being created.
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs: true,
			},
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true,
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t,
		"INSERT INTO system.jobs (id, status, created, payload) values ($1, $2, $3, $4)",
		1, jobs.StatusRunning, timeutil.Now(), []byte("invalid payload"),
	)

	tdb.ExpectErrWithHint(t, "could not decode the payload for job 1. consider deleting this job from system.jobs", "SELECT * FROM crdb_internal.system_jobs")
	tdb.ExpectErrWithHint(t, "could not decode the payload for job 1. consider deleting this job from system.jobs", "SELECT * FROM crdb_internal.jobs")
}

// TestInternalSystemJobsAccess asserts which entries a user can query
// based on their grants and role options.
func TestInternalSystemJobsAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			KeyVisualizer: &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	rootDB := sqlutils.MakeSQLRunner(db)

	// Even though this test modifies the system.jobs table and asserts its contents, we
	// do not disable background job creation nor job adoption. This is because creating
	// users requires jobs to be created and run. Thus, this test only creates jobs of type
	// jobspb.TypeImport and overrides the import resumer.
	registry := s.JobRegistry().(*jobs.Registry)
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{}
	registry.TestingResumerCreationKnobs[jobspb.TypeImport] = func(r jobs.Resumer) jobs.Resumer {
		return &fakeResumer{}
	}

	asUser := func(user string, f func(userDB *sqlutils.SQLRunner)) {
		pgURL := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, "test"),
			Host:   s.SQLAddr(),
		}
		db2, err := gosql.Open("postgres", pgURL.String())
		assert.NoError(t, err)
		defer db2.Close()
		userDB := sqlutils.MakeSQLRunner(db2)

		f(userDB)
	}

	rootDB.Exec(t, "CREATE USER user1 WITH CONTROLJOB PASSWORD 'test'")
	rootDB.Exec(t, "CREATE USER user2 WITH PASSWORD 'test'")
	rootDB.Exec(t, "CREATE USER adminUser WITH PASSWORD 'test'")
	rootDB.Exec(t, "GRANT admin to adminUser WITH ADMIN OPTION")

	// Create a job with user1 as the owner.
	rec := jobs.Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.MakeSQLUsernameFromPreNormalizedString("user1"),
	}
	_, err := registry.CreateJobWithTxn(ctx, rec, 1, nil /* txn */)
	assert.NoError(t, err)

	// Create a job with user2 as the owner.
	rec = jobs.Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.MakeSQLUsernameFromPreNormalizedString("user2"),
	}
	_, err = registry.CreateJobWithTxn(ctx, rec, 2, nil /* txn */)
	assert.NoError(t, err)

	// Create a job with an admin as the owner.
	rec = jobs.Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.MakeSQLUsernameFromPreNormalizedString("adminuser"),
	}
	_, err = registry.CreateJobWithTxn(ctx, rec, 3, nil /* txn */)
	assert.NoError(t, err)

	// user1 can see all jobs not owned by admins because they have the CONTROLJOB role option.
	asUser("user1", func(userDB *sqlutils.SQLRunner) {
		userDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"1"}, {"2"}})
	})

	// user2 can only see their own job
	asUser("user2", func(userDB *sqlutils.SQLRunner) {
		userDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"2"}})
	})

	// Admins can see all jobs.
	rootDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"1"}, {"2"}, {"3"}})
}
