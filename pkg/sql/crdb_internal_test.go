// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	params, _ := createTestServerParamsAllowTenants()

	s, _ /* sqlDB */, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		batch := txn.NewBatch()
		batch.Put(catalogkeys.EncodeNameKey(s.Codec(), &descpb.NameInfo{ParentID: 999, ParentSchemaID: 444, Name: "bob"}), 9999)
		batch.Put(catalogkeys.EncodeNameKey(s.Codec(), &descpb.NameInfo{ParentID: 1000, ParentSchemaID: 29, Name: "alice"}), 10000)
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

	params, _ := createTestServerParamsAllowTenants()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stop(context.Background())
	ctx := context.Background()

	if err := s.StorageLayer().GossipI().(*gossip.Gossip).AddInfoProto(gossip.MakeNodeHealthAlertKey(456), &statuspb.HealthCheckResult{
		Alerts: []statuspb.HealthAlert{{
			StoreID:     123,
			Category:    statuspb.HealthAlert_METRICS,
			Description: "foo",
			Value:       100.0,
		}},
	}, time.Hour); err != nil {
		t.Fatal(err)
	}

	ie := s.SystemLayer().InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRowEx(ctx, "test", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
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

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

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
		kvDB, s.Codec(), "t", "test")
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
	cdd, err := tabledesc.MakeColumnDefDescs(ctx, colDef, nil, evalCtx, tree.ColumnDefaultExprInAddColumn)
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
		return txn.Put(ctx, catalogkeys.MakeDescMetadataKey(s.Codec(), tableDesc.ID), tableDesc.DescriptorProto())
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
	tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, s.Codec(), "t", "test")
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

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	params.Knobs = base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true,
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

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
				json_set(
					json_set(
						crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor, false),
						ARRAY['table', 'mutationJobs'],
						jsonb_build_array(jsonb_build_object('job_id', 123456, 'mutation_id', 1))
					),
					ARRAY['table', 'mutations'],
					jsonb_build_array(jsonb_build_object('mutation_id', 1))
				),
				ARRAY['table', 'privileges', 'ownerProto'],
				to_json('dropped_user')
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
			fmt.Sprintf(`relation "nojob" (%d): mutation in state UNKNOWN, direction NONE, and no column/index descriptor`, tableNoJobID),
		},
		{fmt.Sprintf("%d", tableNoJobID), "defaultdb", "public", "nojob",
			fmt.Sprintf(`descriptor "nojob" (%d) is owned by a role "dropped_user" that doesn't exist`, tableNoJobID),
		},
		{fmt.Sprintf("%d", tableNoJobID), "defaultdb", "public", "nojob", `mutation job 123456: job not found`},
		{fmt.Sprintf("%d", schemaID), fmt.Sprintf("[%d]", databaseID), "public", "",
			fmt.Sprintf(`schema "public" (%d): referenced database ID %d: referenced descriptor not found`, schemaID, databaseID),
		},
		{fmt.Sprintf("%d", databaseID), "t", "", "", `referenced schema ID 104: referenced descriptor not found`},
		{fmt.Sprintf("%d", tableFkTblID), "defaultdb", "public", "fktbl", `referenced schema ID 107: referenced descriptor not found`},
		{fmt.Sprintf("%d", fakeID), fmt.Sprintf("[%d]", databaseID), "public", "test", `referenced schema ID 12345: referenced descriptor not found`},
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

	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
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
	const clusterScope = "cluster"
	const nodeScope = "node"
	getNum := func(db *sqlutils.SQLRunner, scope string) int {
		// Count the number of flows matching our target query. Note that there
		// could be other flows in the table for the internal operations.
		countQuery := fmt.Sprintf("SELECT count(*) FROM crdb_internal.%s_distsql_flows WHERE stmt = '%s'", scope, query)
		var num int
		db.QueryRow(t, countQuery).Scan(&num)
		return num
	}
	for nodeID := 0; nodeID < numNodes; nodeID++ {
		conn := tc.ServerConn(nodeID)
		db := sqlutils.MakeSQLRunner(conn)

		// Check cluster level table.
		actual := getNum(db, clusterScope)
		if actual != 2 {
			t.Fatalf("unexpected output from cluster_distsql_flows on node %d (found %d)", nodeID+1, actual)
		}

		// Check node level table.
		if nodeID == gatewayNodeID {
			if getNum(db, nodeScope) != 0 {
				t.Fatal("unexpectedly non empty output from node_distsql_flows on the gateway")
			}
		} else {
			actual = getNum(db, nodeScope)
			if actual != 1 {
				t.Fatalf("unexpected output from node_distsql_flows on node %d (found %d)", nodeID+1, actual)
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
		rows := sqlDB.Query(t, `
                  SELECT trace_id, node_id, trace_str, jaeger_json
                  FROM crdb_internal.cluster_inflight_traces
                  WHERE trace_id = $1
                  ORDER BY node_id;`, // sort by node_id in case instances are returned out of order
			traceID)
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

func TestIsAtLeastVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
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

	// Start the server. (One node is sufficient; the outliers system
	// is currently in-memory only.)
	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			KVClient: &kvcoord.ClientTestingKnobs{
				// This test shouldn't care where a transaction's record is anchored,
				// but it does because of:
				// https://github.com/cockroachdb/cockroach/pull/125744
				DisableTxnAnchorKeyRandomization: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(conn)
	testTxnContentionEventsTableHelper(t, ctx, conn, sqlDB)
	testTxnContentionEventsTableWithDroppedInfo(t, ctx, conn, sqlDB)
}

func TestTxnContentionEventsTableWithRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := sqlDB.Exec("SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '10ms'")
	require.NoError(t, err)
	rangeKey := "/Local/Range/Table/106/1/-1704619207610523008/RangeDescriptor"
	rangeKeyEscaped := fmt.Sprintf("\"%s\"", rangeKey)
	s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry.AddContentionEvent(contentionpb.ExtendedContentionEvent{
		BlockingEvent: kvpb.ContentionEvent{
			Key: roachpb.Key(rangeKey),
			TxnMeta: enginepb.TxnMeta{
				Key: roachpb.Key(rangeKey),
				ID:  uuid.MakeV4(),
			},

			Duration: 1 * time.Minute,
		},
		BlockingTxnFingerprintID: 9001,
		WaitingTxnID:             uuid.MakeV4(),
		WaitingTxnFingerprintID:  9002,
		WaitingStmtID:            clusterunique.ID{Uint128: uint128.Uint128{Lo: 9003, Hi: 1004}},
		WaitingStmtFingerprintID: 9004,
		ContentionType:           contentionpb.ContentionType_LOCK_WAIT,
	})

	// Contention flush can take some time to flush the events.
	testutils.SucceedsSoon(t, func() error {
		row := sqlDB.QueryRow(`SELECT
    database_name, 
    schema_name, 
    table_name, 
    index_name 
		FROM crdb_internal.transaction_contention_events
		WHERE contending_pretty_key = $1`, rangeKeyEscaped)

		var db, schema, table, index string
		err = row.Scan(&db, &schema, &table, &index)
		if err != nil {
			return err
		}
		if db != "" || schema != "" || table != rangeKeyEscaped || index != "" {
			return errors.Newf(
				"unexpected row: db=%s, schema=%s, table=%s, index=%s", db, schema, table, index,
			)
		}
		return nil
	})
}

func TestTxnContentionEventsTableMultiTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestTenantAlwaysEnabled,
		Knobs: base.TestingKnobs{
			KVClient: &kvcoord.ClientTestingKnobs{
				// This test shouldn't care where a transaction's record is anchored,
				// but it does because of:
				// https://github.com/cockroachdb/cockroach/pull/125744
				DisableTxnAnchorKeyRandomization: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	testTxnContentionEventsTableHelper(t, ctx, db, sqlDB)
}

func causeContention(
	t *testing.T, conn *gosql.DB, table string, insertValue string, updateValue string,
) {
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
			fmt.Sprintf("INSERT INTO %s (id, s) VALUES ('test', $1);", table),
			insertValue)
		require.NoError(t, errTxn)
		wgTxnStarted.Done()
		// Wait for the update to show up in cluster_queries.
		testutils.SucceedsSoon(t, func() error {
			row := tx.QueryRowContext(
				ctx, "SELECT EXISTS (SELECT * FROM crdb_internal.cluster_queries WHERE query LIKE '%/* shuba */')",
			)
			var seen bool
			if err := row.Scan(&seen); err != nil {
				return err
			}
			if !seen {
				return errors.Errorf("did not see update statement")
			}
			return nil
		})
		_, errTxn = tx.ExecContext(ctx, "select pg_sleep(.5);")
		require.NoError(t, errTxn)
		errTxn = tx.Commit()
		require.NoError(t, errTxn)
	}()

	// Need to wait for the txn to start to ensure lock contention.
	wgTxnStarted.Wait()
	// This will be blocked until the insert txn finishes.
	start := timeutil.Now()
	_, errUpdate := conn.ExecContext(
		ctx, fmt.Sprintf("UPDATE %s SET s = $1 where id = 'test' /* shuba */;", table), updateValue)
	require.NoError(t, errUpdate)
	end := timeutil.Now()
	require.GreaterOrEqual(t, end.Sub(start), 500*time.Millisecond)

	wgTxnDone.Wait()
}

func testTxnContentionEventsTableHelper(
	t *testing.T, ctx context.Context, conn *gosql.DB, sqlDB *sqlutils.SQLRunner,
) {

	// Reduce the resolution interval to speed up the test.
	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '100ms'`)

	sqlDB.Exec(t, "CREATE TABLE t (id string, s string);")

	causeContention(t, conn, "t", "insert1", "update1")
	causeContention(t, conn, "t", "insert2", "update2")

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

func testTxnContentionEventsTableWithDroppedInfo(
	t *testing.T, ctx context.Context, conn *gosql.DB, sqlDB *sqlutils.SQLRunner,
) {
	// Reduce the resolution interval to speed up the test.
	sqlDB.Exec(
		t,
		`SET CLUSTER SETTING sql.contention.event_store.resolution_interval = '100ms'`)

	rowCount := 0
	query := `SELECT
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
			where metadata->>'query' like 'UPDATE test.t2 SET %') stats
			on stats.transaction_fingerprint_id = tce.waiting_txn_fingerprint_id
			  and stats.fingerprint_id = tce.waiting_stmt_fingerprint_id`
	var dbName, schemaName, tableName, indexName string

	checkValues := func(expectedDB string, expectedSchema string, expectedTable string, expectedIndex string) {
		testutils.SucceedsWithin(t, func() error {
			rowCount = 0
			rows, errVerify := conn.QueryContext(ctx, query)
			if errVerify != nil {
				return errVerify
			}

			for rows.Next() {
				rowCount++
				errVerify = rows.Scan(&dbName, &schemaName, &tableName, &indexName)
				if errVerify != nil {
					return errVerify
				}

				if dbName != expectedDB {
					return fmt.Errorf("dbName should be %s: %s, %s, %s, %s", expectedDB, dbName, schemaName, tableName, indexName)
				}

				if schemaName != expectedSchema {
					return fmt.Errorf("schemaName should be %s: %s, %s, %s, %s", expectedSchema, dbName, schemaName, tableName, indexName)
				}

				if !strings.Contains(tableName, expectedTable) {
					return fmt.Errorf("tableName should contain %s: %s, %s, %s, %s", expectedTable, dbName, schemaName, tableName, indexName)
				}

				if !strings.Contains(indexName, expectedIndex) {
					return fmt.Errorf("indexName should contain %s: %s, %s, %s, %s", expectedIndex, dbName, schemaName, tableName, indexName)
				}
			}

			if rowCount < 1 {
				return fmt.Errorf("transaction_contention_events did not return any rows")
			}
			return nil
		}, 5*time.Second)
	}

	sqlDB.Exec(t, "CREATE DATABASE test")
	sqlDB.Exec(t, "CREATE TABLE test.t2 (id string, s string);")
	sqlDB.Exec(t, "CREATE INDEX idx_test ON test.t2 (id);")

	causeContention(t, conn, "test.t2", "insert1", "update1")

	// Test all values as is.
	checkValues("test", "public", "t2", "idx_test")

	// Test deleting the index.
	sqlDB.Exec(t, "DROP INDEX test.t2@idx_test;")
	checkValues("test", "public", "t2", "[dropped index id:")

	// Test deleting the table.
	sqlDB.Exec(t, "DROP TABLE test.t2;")
	checkValues("", "", "[dropped table id:", "[dropped index]")

	// Test deleting the database.
	sqlDB.Exec(t, "DROP DATABASE test;")
	checkValues("", "", "[dropped table id:", "[dropped index]")

	// New test deleting the database only.
	query = `SELECT
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
			where metadata->>'query' like 'UPDATE test2.t3 SET %') stats
			on stats.transaction_fingerprint_id = tce.waiting_txn_fingerprint_id
			  and stats.fingerprint_id = tce.waiting_stmt_fingerprint_id`
	sqlDB.Exec(t, "CREATE DATABASE test2")
	sqlDB.Exec(t, "CREATE TABLE test2.t3 (id string, s string);")
	sqlDB.Exec(t, "CREATE INDEX idx_test3 ON test2.t3 (id);")
	causeContention(t, conn, "test2.t3", "insert1", "update1")

	sqlDB.Exec(t, "DROP DATABASE test2;")
	checkValues("", "", "[dropped table id:", "[dropped index]")
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
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)

	// We'll check both the cluster-wide table and the node-local one.
	virtualTables := []string{
		"cluster_execution_insights",
		"node_execution_insights",
	}
	testutils.RunValues(t, "table", virtualTables, func(t *testing.T, table string) {
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
				tdb := s.SQLConn(t, serverutils.User("testuser"))

				// Try to read the virtual table, and see that we can or cannot as expected.
				rows, err := tdb.Query(fmt.Sprintf("SELECT count(*) FROM crdb_internal.%s", table))
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

func (*fakeResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
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
		"INSERT INTO system.jobs (id, status, created) values ($1, $2, $3)",
		1, jobs.StateRunning, timeutil.Now(),
	)
	tdb.Exec(t,
		"INSERT INTO system.job_info (job_id, info_key, value) values ($1, $2, $3)",
		1, jobs.GetLegacyPayloadKey(), payloadBytes,
	)

	tdb.Exec(t,
		`INSERT INTO system.jobs (id, status, created, created_by_type, created_by_id, 
                         claim_session_id, claim_instance_id, num_runs, last_run, job_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		2, jobs.StateRunning, timeutil.Now(), "created by", 2, []byte("claim session id"),
		2, 2, timeutil.Now(), jobspb.TypeImport.String(),
	)
	tdb.Exec(t,
		"INSERT INTO system.job_info (job_id, info_key, value) values ($1, $2, $3)",
		2, jobs.GetLegacyPayloadKey(), payloadBytes,
	)
	tdb.Exec(t,
		"INSERT INTO system.job_info (job_id, info_key, value) values ($1, $2, $3)",
		2, jobs.GetLegacyProgressKey(), []byte("progress"),
	)

	res := tdb.QueryStr(t, `
			SELECT id, status, created, created_by_type, created_by_id, claim_session_id,
      claim_instance_id, num_runs, last_run, job_type
			FROM system.jobs ORDER BY id`,
	)
	tdb.CheckQueryResults(t, `
			SELECT id, status, created, created_by_type, created_by_id, claim_session_id,
             claim_instance_id, num_runs, last_run, job_type
			FROM crdb_internal.system_jobs ORDER BY id`,
		res,
	)

	// TODO(adityamaru): add checks for payload and progress
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
	registry := s.ApplicationLayer().JobRegistry().(*jobs.Registry)
	registry.TestingWrapResumerConstructor(jobspb.TypeImport, func(r jobs.Resumer) jobs.Resumer {
		return &fakeResumer{}
	})

	asUser := func(user string, f func(userDB *sqlutils.SQLRunner)) {
		pgURL := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, "test"),
			Host:   s.AdvSQLAddr(),
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

	// user1 can see all jobs because they have the CONTROLJOB role option.
	asUser("user1", func(userDB *sqlutils.SQLRunner) {
		userDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"1"}, {"2"}, {"3"}})
	})

	// user2 can only see their own job
	asUser("user2", func(userDB *sqlutils.SQLRunner) {
		userDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"2"}})
	})

	// Admins can see all jobs.
	rootDB.CheckQueryResults(t, "SELECT id FROM crdb_internal.system_jobs WHERE id IN (1,2,3) ORDER BY id", [][]string{{"1"}, {"2"}, {"3"}})
}

// TestVirtualTableDoesntHangOnQueryCanceledError is a regression test for
// #99753 which verifies that virtual table generation doesn't hang when the
// worker goroutine returns "query canceled error".
//
// In particular, the following setup is used:
//   - issue SHOW JOBS query which internally issues a query against
//     crdb_internal.system_jobs virtual table
//   - that virtual table is generated by issuing "system-jobs-scan" internal
//     query
//   - during that "system-jobs-scan" query we're injecting the query canceled
//     error (in other words, the error is injected during the generation of
//     crdb_internal.system_jobs virtual table).
//
// The injection is achieved by adding a callback to DistSQLReceiver.Push which
// replaces the first piece of metadata it sees with the error.
func TestVirtualTableDoesntHangOnQueryCanceledError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// addCallback determines whether the push callback should be added.
	var addCallback atomic.Bool
	var numCallbacksAdded atomic.Int32
	err := cancelchecker.QueryCanceledError
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					DistSQLReceiverPushCallbackFactory: func(ctx context.Context, _ string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
						if !addCallback.Load() {
							return nil
						}
						opName, ok := sql.GetInternalOpName(ctx)
						if !ok || !(opName == "system-jobs-scan" || opName == "system-jobs-join") {
							return nil
						}
						numCallbacksAdded.Add(1)
						return func(row rowenc.EncDatumRow, batch coldata.Batch, meta *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
							if meta != nil {
								*meta = execinfrapb.ProducerMetadata{}
								meta.Err = err
							}
							return row, batch, meta
						}
					},
				},
			},
			Insecure: true,
		}})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(db)

	addCallback.Store(true)
	sqlDB.ExpectErr(t, err.Error(), "SHOW JOBS")
	addCallback.Store(false)

	// Sanity check that the callback was added at least once.
	require.Greater(t, numCallbacksAdded.Load(), int32(0))
}

// systemPTSTableRow is a struct representing a row in system.protected_ts_records.
type systemPTSTableRow struct {
	id       string
	ts       string
	metaType string
	meta     []byte
	numSpans int
	spans    []byte
	verified bool
	target   []byte
}

// systemPTSTableRow is a struct representing a row in
// crdb_internal.kv_protected_ts_records.
type virtualPTSTableRow struct {
	systemPTSTableRow
	decodedMeta    []byte
	decodedTargets []byte
	internalMeta   []byte
	numRanges      int
	lastUpdated    string
}

func protect(
	t *testing.T, ctx context.Context, internalDB isql.DB, ptm *ptstorage.Manager, rec *ptpb.Record,
) {
	err := internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return ptm.WithTxn(txn).Protect(ctx, rec)
	})
	require.NoError(t, err)
}

// Reads a row from the system PTS table and the virtual PTS table and returns them.
// Asserts the common columns match before returning.
func scanRecord(
	t *testing.T, sqlDB *sqlutils.SQLRunner, recID uuid.Bytes,
) (systemPTSTableRow, virtualPTSTableRow) {
	systemRow := sqlDB.QueryRow(t, "SELECT * FROM system.protected_ts_records WHERE id = $1", recID.String())
	virtualRow := sqlDB.QueryRow(t, "SELECT * FROM crdb_internal.kv_protected_ts_records WHERE id = $1",
		recID.String())

	var systemRowData systemPTSTableRow
	systemRow.Scan(&systemRowData.id, &systemRowData.ts, &systemRowData.metaType, &systemRowData.meta,
		&systemRowData.numSpans, &systemRowData.spans, &systemRowData.verified, &systemRowData.target)

	var virtualRowData virtualPTSTableRow
	virtualRow.Scan(&virtualRowData.id, &virtualRowData.ts, &virtualRowData.metaType, &virtualRowData.meta,
		&virtualRowData.numSpans, &virtualRowData.spans, &virtualRowData.verified, &virtualRowData.target,
		&virtualRowData.decodedMeta, &virtualRowData.decodedTargets, &virtualRowData.internalMeta,
		&virtualRowData.numRanges, &virtualRowData.lastUpdated)

	require.Equal(t, systemRowData.id, virtualRowData.id)
	require.Equal(t, systemRowData.ts, virtualRowData.ts)
	require.Equal(t, systemRowData.metaType, virtualRowData.metaType)
	require.Equal(t, systemRowData.meta, virtualRowData.meta)
	require.Equal(t, systemRowData.numSpans, virtualRowData.numSpans)
	require.Equal(t, systemRowData.spans, virtualRowData.spans)
	require.Equal(t, systemRowData.verified, virtualRowData.verified)
	require.Equal(t, systemRowData.target, virtualRowData.target)

	return systemRowData, virtualRowData
}

func TestVirtualPTSTableDeprecated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx2 := context.Background()

	var testServerArgs base.TestServerArgs
	ptsKnobs := &protectedts.TestingKnobs{}
	ptsKnobs.DisableProtectedTimestampForMultiTenant = true
	testServerArgs.Knobs.ProtectedTS = ptsKnobs
	srv, conn, _ := serverutils.StartServer(t, testServerArgs)
	defer srv.Stopper().Stop(ctx2)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(conn)
	internalDB := s.InternalDB().(isql.DB)
	ptm := ptstorage.New(s.ClusterSettings(), ptsKnobs)

	t.Run("nil-targets", func(t *testing.T) {
		rec := &ptpb.Record{
			ID:        uuid.MakeV4().GetBytes(),
			Timestamp: s.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			DeprecatedSpans: []roachpb.Span{
				{
					Key:    keys.SystemSQLCodec.TablePrefix(42),
					EndKey: keys.SystemSQLCodec.TablePrefix(42).PrefixEnd(),
				},
			},
			MetaType: "foo",
		}

		protect(t, ctx2, internalDB, ptm, rec)
		_, virtualRow := scanRecord(t, sqlDB, rec.ID)
		require.Equal(t, []byte(nil), virtualRow.decodedMeta)
		require.Equal(t, []byte(nil), virtualRow.internalMeta)
		require.Equal(t, []byte(nil), virtualRow.decodedTargets)
		require.Equal(t, -1, virtualRow.numRanges)
	})
}

// TestVirtualPTSTable asserts the behavior of
// crdb_internal.kv_protected_ts_records, which includes showing records from
// the underlying system table and decoding them.
func TestVirtualPTSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx2 := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx2)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(conn)
	internalDB := s.InternalDB().(isql.DB)
	ptm := ptstorage.New(s.ClusterSettings(), nil)

	tableTargets := func(ids ...uint32) *ptpb.Target {
		var tableIDs []descpb.ID
		for _, id := range ids {
			tableIDs = append(tableIDs, descpb.ID(id))
		}
		return ptpb.MakeSchemaObjectsTarget(tableIDs)
	}

	// Assert the columns expected in the system table and the columns expected in the virtual table.
	systemRows := sqlDB.Query(t, "SELECT * FROM system.protected_ts_records")
	internalRows := sqlDB.Query(t, "SELECT * FROM crdb_internal.kv_protected_ts_records")
	systemCols, err := systemRows.Columns()
	require.NoError(t, err)
	internalCols, err := internalRows.Columns()
	require.NoError(t, err)
	require.Equal(t, systemCols, []string{"id", "ts", "meta_type", "meta", "num_spans", "spans", "verified", "target"},
		"unexpected column in PTS system.protected_ts_records. make sure to add this column to "+
			" crdb_internal.kv_protected_ts_records as well")
	require.Equal(t, internalCols, []string{
		"id", "ts", "meta_type", "meta", "num_spans", "spans", "verified", "target", "decoded_meta", "decoded_target",
		"internal_meta", "num_ranges", "last_updated",
	})

	// Assert the job metadata that is extracted when the PTS record meta type is jobsprotectedts.Jobs.
	t.Run("meta-type-is-jobs", func(t *testing.T) {
		// Create a dummy job. We need the job ID to reference a real job because the virtual table
		// looks up job metadata via the job registry.
		jobRec := jobs.Record{
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
			Username: username.TestUserName(),
		}

		reg := s.JobRegistry().(*jobs.Registry)
		jobID := reg.MakeJobID()

		job, err := reg.CreateJobWithTxn(ctx2, jobRec, jobID, nil /* txn */)
		require.NoError(t, err)

		rec := jobsprotectedts.MakeRecord(
			uuid.MakeV4(),
			int64(job.ID()),
			s.Clock().Now(),
			[]roachpb.Span{},
			jobsprotectedts.Jobs,
			tableTargets(),
		)
		protect(t, ctx2, internalDB, ptm, rec)

		_, virtualRow := scanRecord(t, sqlDB, rec.ID)
		require.Equal(t, []byte(fmt.Sprintf(`{"jobID": %d}`, jobID)), virtualRow.decodedMeta)
		require.Equal(t, []byte(fmt.Sprintf(`{"jobUsername": "%s"}`, username.TestUserName().Normalized())),
			virtualRow.internalMeta)
		require.Equal(t, []byte(`{"schemaObjects": {}}`), virtualRow.decodedTargets)
		require.Equal(t, 0, virtualRow.numRanges)
	})

	t.Run("table-covers-all-meta-types", func(t *testing.T) {
		require.Equal(t, "", jobsprotectedts.GetMetaType(3),
			"expected there to only be two meta types, but found more. if a new meta type was added, "+
				"please add support for it in the `crdb_internal.kv_protected_ts_records table and update this test. "+
				"alternatively, file an issue and leave a todo")
	})

	// Assert the schedule metadata that is extracted when the PTS record meta type is jobsprotectedts.Schedules.
	t.Run("meta-type-is-schedules", func(t *testing.T) {
		// No reason to use JobSchedulerTestEnv, which lets one manipulate time,
		// the PTS table, and executors.
		schedEnv := scheduledjobs.ProdJobSchedulerEnv

		sj := jobs.NewScheduledJob(schedEnv)
		sj.SetOwner(username.TestUserName())
		sj.SetScheduleLabel("test-schedule")
		sj.SetExecutionDetails(tree.ScheduledBackupExecutor.InternalName(), jobspb.ExecutionArguments{})
		sj.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))

		err := internalDB.Txn(ctx2, func(ctx3 context.Context, txn isql.Txn) error {
			err2 := jobs.ScheduledJobTxn(txn).Create(ctx3, sj)
			require.NoError(t, err2)
			return nil
		})
		require.NoError(t, err)

		rec := jobsprotectedts.MakeRecord(
			uuid.MakeV4(),
			int64(sj.ScheduleID()),
			s.Clock().Now(),
			[]roachpb.Span{},
			jobsprotectedts.Schedules,
			tableTargets(),
		)
		protect(t, ctx2, internalDB, ptm, rec)

		_, virtualRow := scanRecord(t, sqlDB, rec.ID)
		require.Equal(t, []byte(fmt.Sprintf(`{"scheduleID": %d}`, sj.ScheduleID())), virtualRow.decodedMeta)
		require.Equal(t, []byte(fmt.Sprintf(`{"scheduleLabel": "%s", "scheduleOwner": "%s"}`,
			"test-schedule", username.TestUserName().Normalized())), virtualRow.internalMeta)
		require.Equal(t, []byte(`{"schemaObjects": {}}`), virtualRow.decodedTargets)
		require.Equal(t, 0, virtualRow.numRanges)
	})

	// Assert that the table descriptor ids are correctly decoded and the number of ranges calculation is
	// accurate.
	t.Run("table-desc-ids-and-num-ranges", func(t *testing.T) {
		// Create two tables with 101 and 201 ranges respectively.
		sqlDB.ExecMultiple(t,
			`CREATE TABLE foo (i INT PRIMARY KEY)`,
			`INSERT INTO foo (i) SELECT * FROM generate_series(1, 200);`,
			`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 200, 2));`,

			`CREATE TABLE foo2 (i INT PRIMARY KEY)`,
			`INSERT INTO foo2 (i) SELECT * FROM generate_series(1, 200);`,
			`ALTER TABLE foo2 SPLIT AT (SELECT * FROM generate_series(1, 200, 1));`,
		)
		var tableID1 uint32
		var tableID2 uint32
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
			` WHERE name = 'foo' AND database_name = current_database()`).Scan(&tableID1)
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
			` WHERE name = 'foo2' AND database_name = current_database()`).Scan(&tableID2)

		rec := ptpb.Record{
			ID:        uuid.MakeV4().GetBytes(),
			Timestamp: s.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			MetaType:  "foo",
			Meta:      []byte("bar"),
			Target:    tableTargets(tableID1, tableID2),
		}
		protect(t, ctx2, internalDB, ptm, &rec)

		_, virtualRow := scanRecord(t, sqlDB, rec.ID)
		require.Equal(t, []byte(nil), virtualRow.decodedMeta)
		require.Equal(t, []byte(nil), virtualRow.internalMeta)
		require.Equal(t, []byte(fmt.Sprintf(`{"schemaObjects": {"ids": [%d, %d]}}`, tableID1, tableID2)), virtualRow.decodedTargets)
		require.Equal(t, 302, virtualRow.numRanges)
	})

	// Assert that the `last_updated` column shows the right timestamp.
	t.Run("last-updated", func(t *testing.T) {
		rec := ptpb.Record{
			ID:        uuid.MakeV4().GetBytes(),
			Timestamp: s.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			MetaType:  "foo",
			Meta:      []byte("bar"),
			Target:    tableTargets(),
		}
		protect(t, ctx2, internalDB, ptm, &rec)

		var ts string
		sqlDB.QueryRow(t, `SELECT crdb_internal_mvcc_timestamp FROM system.protected_ts_records`+
			` WHERE id = $1`, rec.ID.String()).Scan(&ts)
		_, virtualRow := scanRecord(t, sqlDB, rec.ID)
		require.Equal(t, ts, virtualRow.lastUpdated)
	})
}

// TestMVCCValueHeaderSystemColumns tests that the system columns that read MVCCValueHeaders data.
func TestMVCCValueHeaderSystemColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	internalDB := srv.ApplicationLayer().InternalDB().(isql.DB)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE test")
	sqlDB.Exec(t, "CREATE TABLE test.foo (pk int primary key, v1 int, v2 int, INDEX(v2))")

	_, err := internalDB.Executor().ExecEx(ctx, "test-insert-with-origin-id",
		nil,
		sessiondata.InternalExecutorOverride{OriginIDForLogicalDataReplication: 42},
		"INSERT INTO test.foo VALUES (1, 1, 1), (2, 2, 2)")
	require.NoError(t, err)

	_, err = internalDB.Executor().ExecEx(ctx, "test-insert-with-origin-id",
		nil,
		sessiondata.InternalExecutorOverride{},
		"INSERT INTO test.foo VALUES (3, 3, 3)")
	require.NoError(t, err)

	queries := map[string]string{
		"primary":    "SELECT pk, v1, crdb_internal_origin_id FROM test.foo ",
		"index join": "SELECT pk, v1, crdb_internal_origin_id FROM test.foo@{FORCE_INDEX=foo_v2_idx}",
	}
	exp := [][]string{
		{"1", "1", "42"},
		{"2", "2", "42"},
		{"3", "3", "0"}}
	for n, q := range queries {
		t.Run(n, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
				if vectorize {
					sqlDB.Exec(t, "SET vectorize=on")
				} else {
					sqlDB.Exec(t, "SET vectorize=off")
				}
				sqlDB.CheckQueryResults(t, q, exp)
			})
		})
	}
}
