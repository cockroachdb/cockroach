// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangelog_test

import (
	"context"
	_ "embed"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog/internal/rangelogtestpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// TestRangeLog tests the RangeLogWriter implementation by ensuring that
// a representative set of events (encoded in a testdata file) can be
// round-tripped through the system, read from sql, and written to the
// key-value store in the same way that a legacy, internal executor-backed
// implementation would.
func TestRangeLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to test that the data we have stored as encoded protobuf
	// round-trips through the writer, back out through sql as CSV, decodes
	// and has the same structure.
	var rangeLogData rangelogtestpb.RangeLogData
	require.NoError(t, protoutil.Unmarshal(
		encodedRangeLogData, &rangeLogData,
	))

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(ctx)

	// Inject two tables to write into. The first table will be written into by
	// kv-based implementation. The second table will be written into by the
	// internal-executor based implementation. We'll then ensure that the data
	// written by the two are the same.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tn1 := tree.MakeTableNameWithSchema("defaultdb", "public", "rangelog")
	td1 := injectRangelogTable(t, tdb, tn1)
	tn2 := tree.MakeTableNameWithSchema("defaultdb", "public", "rangelog2")
	td2 := injectRangelogTable(t, tdb, tn2)

	// Write the data.
	ec := s.ExecutorConfig().(sql.ExecutorConfig)
	codec := ec.Codec
	ie := ec.InternalDB.Executor()
	mkWriter := func(genID func() int64) kvserver.RangeLogWriter {
		genA, genB := makeTeeIDGen(genID)
		return &teeWriter{
			a: rangelog.NewTestWriter(codec, genA, td1),
			b: rangelog.NewInternalExecutorWriter(genB, ie, tn2.String()),
		}
	}
	require.NoError(t, insertRangeLogData(ctx, kvDB, mkWriter, &rangeLogData))

	// Ensure that the data written to both tables is identical except for the
	// key prefix and checksum.
	const rawKVsWithoutPrefix = `
SELECT crdb_internal.pretty_key(key, 1), 
       substring(encode(val, 'hex') from 9) -- strip the checksum
  FROM crdb_internal.scan(crdb_internal.index_span($1, 1)) as t(key, val)`
	require.Equal(t,
		tdb.QueryStr(t, rawKVsWithoutPrefix, td2.GetID()),
		tdb.QueryStr(t, rawKVsWithoutPrefix, td1.GetID()))

	// Validate that the data can be read from SQL.
	checkDataRoundTrips := func(tn tree.TableName) {
		beforeEncoded, err := protoutil.Marshal(&rangeLogData)
		require.NoError(t, err)
		got := tdb.QueryStr(t, "SELECT * FROM "+tn.String())
		after, err := rangelogtestpb.ParseRows(got)
		require.NoError(t, err)
		afterEncoded, err := protoutil.Marshal(after)
		require.NoError(t, err)
		require.Equal(t, beforeEncoded, afterEncoded)
	}
	checkDataRoundTrips(tn1)
	checkDataRoundTrips(tn2)
}

type idGen = rangelog.IDGen

// insertRangeLogData inserts the provided rangeLogData in a transaction.
func insertRangeLogData(
	ctx context.Context,
	kvDB *kv.DB,
	c func(gen idGen) kvserver.RangeLogWriter,
	rangeLogData *rangelogtestpb.RangeLogData,
) error {
	makeIDGen := func() idGen {
		var offset int
		return func() int64 {
			defer func() { offset++ }()
			return rangeLogData.UniqueIds[offset]
		}
	}
	doInserts := func() (isRestart bool, _ error) {
		w := c(makeIDGen())
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

// makeTeeIDGen takes a function which returns an integer and
// returns two functions, each of which will return the same
// sequence of integers.
func makeTeeIDGen(id idGen) (genA, genB idGen) {
	var a, b []int64
	makeGen := func(s *[]int64) func() int64 {
		return func() (ret int64) {
			if len(*s) == 0 {
				v := id()
				a, b = append(a, v), append(b, v)
			}
			ret, (*s) = (*s)[0], (*s)[1:]
			return ret
		}
	}
	return makeGen(&a), makeGen(&b)
}

// teeWriter writes all entries to both a and b. If an error occurs writing to
// a, no write to b is attempted.
type teeWriter struct {
	a, b kvserver.RangeLogWriter
}

func (t teeWriter) WriteRangeLogEvent(
	ctx context.Context, runner kvserver.DBOrTxn, event kvserverpb.RangeLogEvent,
) error {
	if err := t.a.WriteRangeLogEvent(ctx, runner, event); err != nil {
		return err
	}
	return t.b.WriteRangeLogEvent(ctx, runner, event)
}
