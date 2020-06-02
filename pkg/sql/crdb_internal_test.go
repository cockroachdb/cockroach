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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/pgtype"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAllNamesInternal tests both namespace and namespace_deprecated
// entries.
func TestGetAllNamesInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _ /* sqlDB */, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		batch := txn.NewBatch()
		batch.Put(sqlbase.NewTableKey(999, 444, "bob").Key(keys.SystemSQLCodec), 9999)
		batch.Put(sqlbase.NewDeprecatedTableKey(1000, "alice").Key(keys.SystemSQLCodec), 10000)
		batch.Put(sqlbase.NewDeprecatedTableKey(999, "overwrite_me_old_value").Key(keys.SystemSQLCodec), 9999)
		return txn.CommitInBatch(ctx, batch)
	})
	require.NoError(t, err)

	names, err := sql.TestingGetAllNames(ctx, nil, s.InternalExecutor().(*sql.InternalExecutor))
	require.NoError(t, err)

	assert.Equal(t, sql.NamespaceKey{ParentID: 999, ParentSchemaID: 444, Name: "bob"}, names[9999])
	assert.Equal(t, sql.NamespaceKey{ParentID: 1000, Name: "alice"}, names[10000])
}

// TestRangeLocalityBasedOnNodeIDs tests that the replica_localities shown in crdb_internal.ranges
// are correct reflection of the localities of the stores in the range descriptor which
// is in the replicas column
func TestRangeLocalityBasedOnNodeIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	// Set to 2 so the the next store id will be 3.
	assert.NoError(t, tc.Servers[0].DB().Put(ctx, keys.StoreIDGenerator, 2))

	// NodeID=2, StoreID=3
	tc.AddServer(t,
		base.TestServerArgs{
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "2"}}},
		},
	)
	assert.EqualValues(t, 3, tc.Servers[len(tc.Servers)-1].GetFirstStoreID())

	// Set to 1 so the next store id will be 2.
	assert.NoError(t, tc.Servers[0].DB().Put(ctx, keys.StoreIDGenerator, 1))

	// NodeID=3, StoreID=2
	tc.AddServer(t,
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
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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
	tableDesc := sqlbase.TestingGetMutableExistingTableDescriptor(
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
	col, _, _, err := sqlbase.MakeColumnDefDescs(ctx, colDef, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	col.ID = tableDesc.NextColumnID
	tableDesc.NextColumnID++
	tableDesc.Families[0].ColumnNames = append(tableDesc.Families[0].ColumnNames, col.Name)
	tableDesc.Families[0].ColumnIDs = append(tableDesc.Families[0].ColumnIDs, col.ID)
	tableDesc.Columns = append(tableDesc.Columns, *col)

	// Write the modified descriptor.
	if err := kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID), tableDesc.DescriptorProto())
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
	tableDesc = sqlbase.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
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
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1);
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`BEGIN`); err != nil {
		t.Fatal(err)
	}

	// Look up the schema first so only the read txn is recorded in
	// kv trace logs.
	if _, err := sqlDB.Exec(`SELECT * FROM t.t`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(
		`SET tracing=on,kv; SELECT * FROM t.t; SET TRACING=off`); err != nil {
		t.Fatal(err)
	}

	// The log messages we are looking for are structured like
	// [....,txn=<txnID>], so search for those and extract the id.
	row := sqlDB.QueryRow(`
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
	row = sqlDB.QueryRow(`
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
	row = sqlDB.QueryRow(`SELECT start FROM crdb_internal.node_transactions WHERE id = $1`, foundTxnID)
	if err := row.Scan(&txnStart); err != nil {
		t.Fatal(err)
	}
	if txnStart.After(queryStart) {
		t.Error("expected txn to start before query")
	}
}

// TestCrdbInternalJobsOOM verifies that the memory budget works correctly for
// crdb_internal.jobs.
func TestCrdbInternalJobsOOM(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The budget needs to be large enough to establish the initial database
	// connection, but small enough to overflow easily. It's set to be comfortably
	// large enough that the server can start up with a bit of
	// extra space to overflow.
	const lowMemoryBudget = 250000
	const fieldSize = 10000
	const numRows = 10
	const statement = "SELECT count(*) FROM crdb_internal.jobs"

	insertRows := func(sqlDB *gosql.DB) {
		for i := 0; i < numRows; i++ {
			if _, err := sqlDB.Exec(`
INSERT INTO system.jobs(id, status, payload, progress)
VALUES ($1, 'StatusRunning', repeat('a', $2)::BYTES, repeat('a', $2)::BYTES)`, i, fieldSize); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("over budget jobs table", func(t *testing.T) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			SQLMemoryPoolSize: lowMemoryBudget,
		})
		defer s.Stopper().Stop(context.Background())

		insertRows(sqlDB)
		_, err := sqlDB.Exec(statement)
		if err == nil {
			t.Fatalf("Expected \"%s\" to consume too much memory, found no error", statement)
		}
		if pErr := (*pq.Error)(nil); !(errors.As(err, &pErr) &&
			pgcode.MakeCode(string(pErr.Code)) == pgcode.OutOfMemory) {
			t.Fatalf("Expected \"%s\" to consume too much memory, found unexpected error %+v", statement, pErr)
		}
	})

	t.Run("under budget jobs table", func(t *testing.T) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			SQLMemoryPoolSize: 2 * lowMemoryBudget,
		})
		defer s.Stopper().Stop(context.Background())

		insertRows(sqlDB)
		if _, err := sqlDB.Exec(statement); err != nil {
			t.Fatal(err)
		}
	})
}
