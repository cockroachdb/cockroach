// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangelog_test

import (
	"context"
	_ "embed"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog/internal/rangelogtestpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// encodedRangeLogData is protobuf encoded rangelog data derived in 22.2
// from the csv file in testdata/rangelog.csv. The command to generate
// the rangelog.bin file is:
//
//	go run ./internal/genrangelogdatapb ./testdata/rangelog.csv ./testdata/rangelog.bin
//
//go:embed testdata/rangelog.bin
var encodedRangeLogData []byte

func TestRangeLogRoundTrips(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We're going to test that the data we have stored as encoded protobuf
	// round-trips through the writer, back out through sql as CSV, decodes
	// and has the same structure.
	var rangeLogData rangelogtestpb.RangeLogData
	require.NoError(t, protoutil.Unmarshal(
		encodedRangeLogData, &rangeLogData,
	))

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Inject a table to write into.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tn := tree.MakeTableNameWithSchema("defaultdb", "public", "rangelog")
	injectRangelogTable(t, tdb, tn)

	// Write the data.
	ie := s.InternalExecutor().(sqlutil.InternalExecutor)
	require.NoError(t, insertRangeLogData(ctx, kvDB, ie, tn, &rangeLogData))

	// Validate that it round-trips.
	got := tdb.QueryStr(t, "SELECT * FROM "+tn.String())
	after, err := rangelogtestpb.ParseRows(got)
	require.NoError(t, err)
	require.Equal(t, &rangeLogData, after)
}

// insertRangeLogData transactionally inserts the provided rangeLogData.
func insertRangeLogData(
	ctx context.Context,
	kvDB *kv.DB,
	ie sqlutil.InternalExecutor,
	tn tree.TableName,
	rangeLogData *rangelogtestpb.RangeLogData,
) error {
	doInserts := func() (isRestart bool, _ error) {
		var offset int
		genID := func() int64 {
			defer func() { offset++ }()
			return rangeLogData.UniqueIds[offset]
		}
		w := rangelog.NewTestWriter(genID, ie, tn.String())
		var called bool
		errRestart := errors.New("restart")
		err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if called {
				return errRestart
			}
			called = true
			for _, rec := range rangeLogData.Events {
				if err := w.WriteRangeLogEvent(ctx, txn, *rec); err != nil {
					return err
				}
			}
			return nil
		})
		if isRestart = errors.Is(err, errRestart); isRestart {
			err = nil
		}
		return isRestart, err
	}
	// We need a loop because there might be transaction restarts, and
	// we want to make sure the IDs are all the same.
	for {
		if isRestart, err := doInserts(); !isRestart {
			return err
		}
	}
}

// injectRangeLogTable will inject a table with the provided name that has
// the same structure as the system.rangelog table. It returns the table's
// ID.
func injectRangelogTable(
	t *testing.T, tdb *sqlutils.SQLRunner, tn tree.TableName,
) catalog.TableDescriptor {
	// Create a table with a default definition based on the schema of
	// the rangelog table.
	tdb.Exec(t, strings.Replace(
		systemschema.RangeEventTableSchema,
		"system.rangelog", tn.String(), 1,
	))

	// Make a deep copy of the system database's table descriptor.
	clone := systemschema.RangeEventTable.NewBuilder().
		BuildExistingMutable().NewBuilder(). // deep copy
		BuildExistingMutable().(*tabledesc.Mutable)

	// Modify the table descriptor we cloned above to have an appropriate
	// version, name, parents, and mod times.
	clone.Version++
	clone.Name = tn.Object()
	var modTimeStr string
	tdb.QueryRow(t, `
  WITH db_id AS (
                SELECT id
                  FROM system.namespace
                 WHERE "parentID" = 0 AND name = $1
             ),
       schema_id AS (
                    SELECT id
                      FROM system.namespace
                     WHERE "parentID" = (SELECT id FROM db_id)
                       AND "parentSchemaID" = 0
                       AND name = $2
                 )
SELECT "parentID", "parentSchemaID", id, crdb_internal_mvcc_timestamp
  FROM system.namespace
 WHERE name = $3
   AND "parentID" = (SELECT id FROM db_id)
   AND "parentSchemaID" = (SELECT id FROM schema_id)`,
		tn.Catalog(), tn.Schema(), tn.Object()).
		Scan(&clone.ParentID, &clone.UnexposedParentSchemaID, &clone.ID,
			&modTimeStr)
	modTime, err := hlc.ParseHLC(modTimeStr)
	require.NoError(t, err)
	clone.ModificationTime = modTime
	clone.CreateAsOfTime = modTime
	cloneBuilder := clone.NewBuilder()
	require.NoError(t, cloneBuilder.RunPostDeserializationChanges())
	clone = cloneBuilder.BuildExistingMutable().(*tabledesc.Mutable)

	// Overwrite the originally created table with the proper descriptor
	// protobuf.
	data, err := protoutil.Marshal(clone.DescriptorProto())
	require.NoError(t, err)
	tdb.Exec(t, "SELECT crdb_internal.unsafe_upsert_descriptor($1, $2)",
		clone.ID, data)
	return clone.ImmutableCopy().(catalog.TableDescriptor)
}
