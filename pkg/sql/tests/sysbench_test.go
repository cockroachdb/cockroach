// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

const (
	sysbenchTables          = 10    // how many tables in the dataset
	sysbenchRowsPerTable    = 10000 // how many rows per table in the dataset
	sysbenchRangeSize       = 100   // how many rows to scan during range scans
	sysbenchPointSelects    = 10    // how many point selects to perform per transaction
	sysbenchSimpleRanges    = 1     // how many simple range scans to perform per transaction
	sysbenchSumRanges       = 1     // how many sum range scans to perform per transaction
	sysbenchOrderRanges     = 1     // how many order range scans to perform per transaction
	sysbenchDistinctRanges  = 1     // how many distinct range scans to perform per transaction
	sysbenchIndexUpdates    = 1     // how many index updates to perform per transaction
	sysbenchNonIndexUpdates = 1     // how many non-index updates to perform per transaction
	sysbenchDeleteInserts   = 1     // how many delete-inserts to perform per transaction
)

// tableNum is an identifier for a table in the sysbench schema.
type tableNum uint64

// rowID is an identifier for a row in a sysbench table.
type rowID uint64

// kValue is the value of the k column in a sysbench row.
type kValue uint64

// cValue is the value of the c column in a sysbench row.
type cValue string

// padValue is the value of the pad column in a sysbench row.
type padValue string

func randTableNum(rng *rand.Rand) tableNum {
	return tableNum(rng.Intn(sysbenchTables))
}

func randRowID(rng *rand.Rand) rowID {
	return rowID(rng.Intn(sysbenchRowsPerTable))
}

func randKValue(rng *rand.Rand) kValue {
	return kValue(rng.Intn(sysbenchRowsPerTable))
}

func randCValue(rng *rand.Rand) cValue {
	return cValue(randutil.RandString(rng, 119, randutil.PrintableKeyAlphabet))
}

func randPadValue(rng *rand.Rand) padValue {
	return padValue(randutil.RandString(rng, 59, randutil.PrintableKeyAlphabet))
}

// sysbenchDriver is capable of running sysbench.
type sysbenchDriver interface {
	// Transaction orchestration operations.
	Begin()
	Commit()
	// Read-only operations.
	PointSelect(tableNum, rowID)
	SimpleRange(tableNum, rowID, rowID)
	SumRange(tableNum, rowID, rowID)
	OrderRange(tableNum, rowID, rowID)
	DistinctRange(tableNum, rowID, rowID)
	// Read-write operations.
	IndexUpdate(tableNum, rowID)
	NonIndexUpdate(tableNum, rowID, cValue)
	DeleteInsert(tableNum, rowID, kValue, cValue, padValue)

	// Prepares the schema and connection for the workload.
	prep(rng *rand.Rand)
}

const (
	sysbenchDB          = `sysbench`
	sysbenchTableFmt    = `sbtest%d`
	sysbenchCreateDB    = `CREATE DATABASE ` + sysbenchDB
	sysbenchCreateTable = `CREATE TABLE sbtest%d(
	  id INT8 PRIMARY KEY,
	  k INT8 NOT NULL DEFAULT 0,
	  c CHAR(120) NOT NULL DEFAULT '',
	  pad CHAR(60) NOT NULL DEFAULT ''
	)`
	sysbenchCreateIndex = `CREATE INDEX k_%[1]d ON sbtest%[1]d(k)`

	sysbenchStmtBegin          = `BEGIN`
	sysbenchStmtCommit         = `COMMIT`
	sysbenchStmtPointSelect    = `SELECT c FROM sbtest%d WHERE id=$1`
	sysbenchStmtSimpleRange    = `SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2`
	sysbenchStmtSumRange       = `SELECT sum(k) FROM sbtest%d WHERE id BETWEEN $1 AND $2`
	sysbenchStmtOrderRange     = `SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c`
	sysbenchStmtDistinctRange  = `SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c`
	sysbenchStmtIndexUpdate    = `UPDATE sbtest%d SET k=k+1 WHERE id=$1`
	sysbenchStmtNonIndexUpdate = `UPDATE sbtest%d SET c=$2 WHERE id=$1`
	sysbenchStmtDelete         = `DELETE FROM sbtest%d WHERE id=$1`
	sysbenchStmtInsert         = `INSERT INTO sbtest%d (id, k, c, pad) VALUES ($1, $2, $3, $4)`
)

// sysbenchSQL is SQL-based implementation of sysbenchDriver.
type sysbenchSQL struct {
	ctx  context.Context
	conn *pgx.Conn
	stmt struct {
		begin          string
		commit         string
		pointSelect    [sysbenchTables]string
		simpleRange    [sysbenchTables]string
		sumRange       [sysbenchTables]string
		orderRange     [sysbenchTables]string
		distinctRange  [sysbenchTables]string
		indexUpdate    [sysbenchTables]string
		nonIndexUpdate [sysbenchTables]string
		delete         [sysbenchTables]string
		insert         [sysbenchTables]string
	}
}

func newSysbenchSQL(ctx context.Context, b *testing.B) (sysbenchDriver, func()) {
	st := cluster.MakeTestingClusterSettings()
	// NOTE: disabling background work makes the benchmark more predictable, but
	// also moderately less realistic.
	disableBackgroundWork(st)
	s := serverutils.StartServerOnly(b, base.TestServerArgs{Settings: st})
	pgURL, cleanupURL := s.ApplicationLayer().PGUrl(b, serverutils.DBName(sysbenchDB))
	conn := try(pgx.Connect(ctx, pgURL.String()))
	cleanup := func() {
		cleanupURL()
		s.Stopper().Stop(ctx)
	}
	return &sysbenchSQL{
		ctx:  ctx,
		conn: conn,
	}, cleanup
}

func (s *sysbenchSQL) Begin() {
	try(s.conn.Exec(s.ctx, s.stmt.begin))
}

func (s *sysbenchSQL) Commit() {
	try(s.conn.Exec(s.ctx, s.stmt.commit))
}

func (s *sysbenchSQL) PointSelect(t tableNum, id rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.pointSelect[t], id))
}

func (s *sysbenchSQL) SimpleRange(t tableNum, id rowID, id2 rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.simpleRange[t], id, id2))
}

func (s *sysbenchSQL) SumRange(t tableNum, id rowID, id2 rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.sumRange[t], id, id2))
}

func (s *sysbenchSQL) OrderRange(t tableNum, id rowID, id2 rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.orderRange[t], id, id2))
}

func (s *sysbenchSQL) DistinctRange(t tableNum, id rowID, id2 rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.distinctRange[t], id, id2))
}

func (s *sysbenchSQL) IndexUpdate(t tableNum, id rowID) {
	try(s.conn.Exec(s.ctx, s.stmt.indexUpdate[t], id))
}

func (s *sysbenchSQL) NonIndexUpdate(t tableNum, id rowID, cValue cValue) {
	try(s.conn.Exec(s.ctx, s.stmt.nonIndexUpdate[t], id, cValue))
}

func (s *sysbenchSQL) DeleteInsert(t tableNum, id rowID, k kValue, c cValue, pad padValue) {
	try(s.conn.Exec(s.ctx, s.stmt.delete[t], id))
	try(s.conn.Exec(s.ctx, s.stmt.insert[t], id, k, c, pad))
}

func (s *sysbenchSQL) prep(rng *rand.Rand) {
	s.prepSchema(rng)
	s.prepConn()
}

func (s *sysbenchSQL) prepSchema(rng *rand.Rand) {
	// Create the database.
	try(s.conn.Exec(s.ctx, sysbenchCreateDB))

	// NOTE: this is faster without parallelism, for some reason.
	for i := range sysbenchTables {
		// Create the table.
		try(s.conn.Exec(s.ctx, fmt.Sprintf(sysbenchCreateTable, i)))

		// Import rows into the table.
		table := pgx.Identifier{fmt.Sprintf(sysbenchTableFmt, i)}
		cols := []string{`id`, `k`, `c`, `pad`}
		rows := pgx.CopyFromSlice(sysbenchRowsPerTable, func(j int) ([]any, error) {
			id := rowID(j)
			k := randKValue(rng)
			c := randCValue(rng)
			pad := randPadValue(rng)
			return []any{id, k, c, pad}, nil
		})
		try(s.conn.CopyFrom(s.ctx, table, cols, rows))

		// Create the secondary index on the table.
		try(s.conn.Exec(s.ctx, fmt.Sprintf(sysbenchCreateIndex, i)))
	}
}

func (s *sysbenchSQL) prepConn() {
	s.stmt.begin = try(s.conn.Prepare(s.ctx, "begin", sysbenchStmtBegin)).Name
	s.stmt.commit = try(s.conn.Prepare(s.ctx, "commit", sysbenchStmtCommit)).Name
	for i := range sysbenchTables {
		pointSelectName, pointSelectSQL := fmt.Sprintf("pointSelect%d", i), fmt.Sprintf(sysbenchStmtPointSelect, i)
		simpleRangeName, simpleRangeSQL := fmt.Sprintf("simpleRange%d", i), fmt.Sprintf(sysbenchStmtSimpleRange, i)
		sumRangeName, sumRangeSQL := fmt.Sprintf("sumRange%d", i), fmt.Sprintf(sysbenchStmtSumRange, i)
		orderRangeName, orderRangeSQL := fmt.Sprintf("orderRange%d", i), fmt.Sprintf(sysbenchStmtOrderRange, i)
		distinctRangeName, distinctRangeSQL := fmt.Sprintf("distinctRange%d", i), fmt.Sprintf(sysbenchStmtDistinctRange, i)
		indexUpdateName, indexUpdateSQL := fmt.Sprintf("indexUpdate%d", i), fmt.Sprintf(sysbenchStmtIndexUpdate, i)
		nonIndexUpdateName, nonIndexUpdateSQL := fmt.Sprintf("nonIndexUpdate%d", i), fmt.Sprintf(sysbenchStmtNonIndexUpdate, i)
		deleteName, deleteSQL := fmt.Sprintf("delete%d", i), fmt.Sprintf(sysbenchStmtDelete, i)
		insertName, insertSQL := fmt.Sprintf("insert%d", i), fmt.Sprintf(sysbenchStmtInsert, i)

		s.stmt.pointSelect[i] = try(s.conn.Prepare(s.ctx, pointSelectName, pointSelectSQL)).Name
		s.stmt.simpleRange[i] = try(s.conn.Prepare(s.ctx, simpleRangeName, simpleRangeSQL)).Name
		s.stmt.sumRange[i] = try(s.conn.Prepare(s.ctx, sumRangeName, sumRangeSQL)).Name
		s.stmt.orderRange[i] = try(s.conn.Prepare(s.ctx, orderRangeName, orderRangeSQL)).Name
		s.stmt.distinctRange[i] = try(s.conn.Prepare(s.ctx, distinctRangeName, distinctRangeSQL)).Name
		s.stmt.indexUpdate[i] = try(s.conn.Prepare(s.ctx, indexUpdateName, indexUpdateSQL)).Name
		s.stmt.nonIndexUpdate[i] = try(s.conn.Prepare(s.ctx, nonIndexUpdateName, nonIndexUpdateSQL)).Name
		s.stmt.delete[i] = try(s.conn.Prepare(s.ctx, deleteName, deleteSQL)).Name
		s.stmt.insert[i] = try(s.conn.Prepare(s.ctx, insertName, insertSQL)).Name
	}
}

// sysbenchKV is KV-based implementation of sysbenchDriver.
type sysbenchKV struct {
	ctx         context.Context
	db          *kv.DB
	txn         *kv.Txn
	pkPrefix    [sysbenchTables]roachpb.Key
	indexPrefix [sysbenchTables]roachpb.Key
}

func newSysbenchKV(ctx context.Context, b *testing.B) (sysbenchDriver, func()) {
	st := cluster.MakeTestingClusterSettings()
	// NOTE: disabling background work makes the benchmark more predictable, but
	// also moderately less realistic.
	disableBackgroundWork(st)
	s, _, db := serverutils.StartServer(b, base.TestServerArgs{Settings: st})
	cleanup := func() {
		s.Stopper().Stop(ctx)
	}
	return &sysbenchKV{
		ctx: ctx,
		db:  db,
	}, cleanup
}

func (s *sysbenchKV) Begin() {
	s.txn = s.db.NewTxn(s.ctx, "sysbench")
}

func (s *sysbenchKV) Commit() {
	try0(s.txn.Commit(s.ctx))
}

func (s *sysbenchKV) PointSelect(t tableNum, id rowID) {
	key := s.pkKey(t, id)
	val := try(s.txn.Get(s.ctx, key))
	if !val.Exists() {
		panic(errors.New("row not found"))
	}
}

func (s *sysbenchKV) scanRange(t tableNum, id rowID, id2 rowID) {
	start := s.pkKey(t, id)
	end := s.pkKey(t, id2)
	try(s.txn.Scan(s.ctx, start, end, 0 /* maxRows */))
}

func (s *sysbenchKV) SimpleRange(t tableNum, id rowID, id2 rowID) {
	s.scanRange(t, id, id2)
	// Ignore SQL-level post-processing.
}

func (s *sysbenchKV) SumRange(t tableNum, id rowID, id2 rowID) {
	s.scanRange(t, id, id2)
	// Ignore SQL-level post-processing.
}

func (s *sysbenchKV) OrderRange(t tableNum, id rowID, id2 rowID) {
	s.scanRange(t, id, id2)
	// Ignore SQL-level post-processing.
}

func (s *sysbenchKV) DistinctRange(t tableNum, id rowID, id2 rowID) {
	s.scanRange(t, id, id2)
	// Ignore SQL-level post-processing.
}

func (s *sysbenchKV) IndexUpdate(t tableNum, id rowID) {
	// Read the primary key, increment the k value, and replace.
	pkKey := s.pkKey(t, id)
	val := try(s.txn.GetForUpdate(s.ctx, pkKey, kvpb.BestEffort))
	if !val.Exists() {
		panic(errors.New("row not found"))
	}

	// Decode the old kValue, modify it, and encode it back.
	pkValue := try(val.Value.GetBytes())
	oldK := s.decodePKValue(pkValue)
	newK := oldK + 1
	s.encodePKValue(pkValue, newK)

	// Construct the old and new index keys, plus the new index value.
	oldIndexKey := s.indexKey(t, oldK, id)
	newIndexKey := s.indexKey(t, newK, id)
	newIndexValue := s.indexValue(newIndexKey)

	// Issue the mutation batch.
	var b kv.Batch
	b.Put(pkKey, pkValue)
	b.Del(oldIndexKey)
	b.Put(newIndexKey, newIndexValue)
	try0(s.txn.Run(s.ctx, &b))
}

func (s *sysbenchKV) NonIndexUpdate(t tableNum, id rowID, _ cValue) {
	// Read the primary key and write it back.
	pkKey := s.pkKey(t, id)
	val := try(s.txn.GetForUpdate(s.ctx, pkKey, kvpb.BestEffort))
	if !val.Exists() {
		panic(errors.New("row not found"))
	}
	pkValue := try(val.Value.GetBytes())
	pkValue[len(pkValue)-1]++ // modify the last byte
	try0(s.txn.Put(s.ctx, pkKey, pkValue))
}

func (s *sysbenchKV) DeleteInsert(t tableNum, id rowID, newK kValue, _ cValue, _ padValue) {
	// Read the primary key and delete both keys from the old row.
	pkKey := s.pkKey(t, id)
	// NOTE: we don't use GetForUpdate. See #50181.
	val := try(s.txn.Get(s.ctx, pkKey))
	if !val.Exists() {
		panic(errors.New("row not found"))
	}
	pkValue := try(val.Value.GetBytes())
	oldK := s.decodePKValue(pkValue)
	oldIndexKey := s.indexKey(t, oldK, id)

	var b1 kv.Batch
	b1.Del(pkKey)
	b1.Del(oldIndexKey)
	try0(s.txn.Run(s.ctx, &b1))

	// Insert the new row.
	s.encodePKValue(pkValue, newK)
	newIndexKey := s.indexKey(t, newK, id)
	newIndexValue := s.indexValue(newIndexKey)

	var b2 kv.Batch
	b2.CPut(pkKey, pkValue, nil /* expValue */)
	b2.InitPut(oldIndexKey, newIndexValue, false /* failOnTombstones */)
	try0(s.txn.Run(s.ctx, &b2))
}

func (s *sysbenchKV) pkKey(t tableNum, id rowID) []byte {
	key := s.pkPrefix[t]
	key = encoding.EncodeUvarintAscending(key, uint64(id))
	key = keys.MakeFamilyKey(key, 0)
	return key
}

func (s *sysbenchKV) indexKey(t tableNum, k kValue, id rowID) []byte {
	key := s.indexPrefix[t]
	key = encoding.EncodeUvarintAscending(key, uint64(k))
	key = encoding.EncodeUvarintAscending(key, uint64(id))
	key = keys.MakeFamilyKey(key, 0)
	return key
}

func (s *sysbenchKV) pkValue(rng *rand.Rand, pkKey roachpb.Key, k kValue) []byte {
	const pkValueSize = 185 // measured from sysbenchSQL
	b := randutil.RandBytes(rng, pkValueSize)
	s.encodePKValue(b, k)
	var v roachpb.Value
	v.SetBytes(b)
	v.InitChecksum(pkKey)
	return v.RawBytes
}

func (s *sysbenchKV) encodePKValue(b []byte, k kValue) {
	// Replace the first 8 bytes with the k value, so that we can decode k to keep
	// the secondary index in sync.
	encoding.EncodeUint64Ascending(b[:8], uint64(k))
}

func (s *sysbenchKV) decodePKValue(b []byte) kValue {
	// Extract the k value from the first 8 bytes. The rest is random.
	_, k, err := encoding.DecodeUint64Ascending(b[:8])
	if err != nil {
		panic(errors.Wrapf(err, "decoding primary key value"))
	}
	return kValue(k)
}

func (s *sysbenchKV) indexValue(indexKey roachpb.Key) []byte {
	var v roachpb.Value
	v.SetBytes(nil)
	v.InitChecksum(indexKey)
	return v.RawBytes
}

func (s *sysbenchKV) prep(rng *rand.Rand) {
	s.prepKeyPrefixes()
	s.prepDataset(rng)
}

func (s *sysbenchKV) prepKeyPrefixes() {
	const tableNumOffset = 100
	for i := range sysbenchTables {
		s.pkPrefix[i] = keys.SystemSQLCodec.IndexPrefix(uint32(tableNumOffset+i), 1)
		s.pkPrefix[i] = slices.Clip(s.pkPrefix[i])
		s.indexPrefix[i] = keys.SystemSQLCodec.IndexPrefix(uint32(tableNumOffset+i), 2)
		s.indexPrefix[i] = slices.Clip(s.indexPrefix[i])
	}
}

func (s *sysbenchKV) prepDataset(rng *rand.Rand) {
	for i := range sysbenchTables {
		t := tableNum(i)
		var b kv.Batch
		for j := range sysbenchRowsPerTable {
			id := rowID(j)
			k := randKValue(rng)
			pkKey := s.pkKey(t, id)
			pkValue := s.pkValue(rng, pkKey, k)
			indexKey := s.indexKey(t, k, id)
			indexValue := s.indexValue(indexKey)
			b.Put(pkKey, pkValue)
			b.Put(indexKey, indexValue)
		}
		try0(s.db.Run(s.ctx, &b))
	}

	// Scan across all tables to ensure that intent resolution has completed.
	for i := range sysbenchTables {
		try(s.db.Scan(s.ctx, s.pkPrefix[i], s.indexPrefix[i].PrefixEnd(), 0))
	}
}

func sysbenchExecutePointSelects(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchPointSelects {
		s.PointSelect(t, randRowID(rng))
	}
}

func sysbenchExecuteRange(op func(tableNum, rowID, rowID), count int, rng *rand.Rand) {
	t := randTableNum(rng)
	for range count {
		id := randRowID(rng)
		id2 := id + sysbenchRangeSize - 1
		op(t, id, id2)
	}
}

func sysbenchExecuteSimpleRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.SimpleRange, sysbenchSimpleRanges, rng)
}

func sysbenchExecuteSumRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.SumRange, sysbenchSumRanges, rng)
}

func sysbenchExecuteOrderRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.OrderRange, sysbenchOrderRanges, rng)
}

func sysbenchExecuteDistinctRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.DistinctRange, sysbenchDistinctRanges, rng)
}

func sysbenchExecuteIndexUpdates(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchIndexUpdates {
		s.IndexUpdate(t, randRowID(rng))
	}
}

func sysbenchExecuteNonIndexUpdates(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchNonIndexUpdates {
		s.NonIndexUpdate(t, randRowID(rng), randCValue(rng))
	}
}

func sysbenchExecuteDeleteInserts(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchDeleteInserts {
		id := randRowID(rng)
		k := randKValue(rng)
		c := randCValue(rng)
		pad := randPadValue(rng)
		s.DeleteInsert(t, id, k, c, pad)
	}
}

// sysbenchWorkload executes a single transaction of a sysbench workload.
type sysbenchWorkload func(sysbenchDriver, *rand.Rand)

func sysbenchOltpReadOnly(s sysbenchDriver, rng *rand.Rand) {
	s.Begin()
	sysbenchExecutePointSelects(s, rng)
	sysbenchExecuteSimpleRanges(s, rng)
	sysbenchExecuteSumRanges(s, rng)
	sysbenchExecuteOrderRanges(s, rng)
	sysbenchExecuteDistinctRanges(s, rng)
	s.Commit()
}

func sysbenchOltpWriteOnly(s sysbenchDriver, rng *rand.Rand) {
	s.Begin()
	sysbenchExecuteIndexUpdates(s, rng)
	sysbenchExecuteNonIndexUpdates(s, rng)
	sysbenchExecuteDeleteInserts(s, rng)
	s.Commit()
}

func sysbenchOltpReadWrite(s sysbenchDriver, rng *rand.Rand) {
	s.Begin()
	sysbenchExecutePointSelects(s, rng)
	sysbenchExecuteSimpleRanges(s, rng)
	sysbenchExecuteSumRanges(s, rng)
	sysbenchExecuteOrderRanges(s, rng)
	sysbenchExecuteDistinctRanges(s, rng)
	sysbenchExecuteIndexUpdates(s, rng)
	sysbenchExecuteNonIndexUpdates(s, rng)
	sysbenchExecuteDeleteInserts(s, rng)
	s.Commit()
}

func BenchmarkSysbench(b *testing.B) {
	defer log.Scope(b).Close(b)
	for _, sysFn := range []func(context.Context, *testing.B) (sysbenchDriver, func()){
		newSysbenchSQL,
		newSysbenchKV,
		// TODO(nvanbenschoten): add a pebble-level implementation.
	} {
		sysTyp := runtime.FuncForPC(reflect.ValueOf(sysFn).Pointer()).Name()
		sysTyp = strings.TrimPrefix(sysTyp, "github.com/cockroachdb/cockroach/pkg/sql/tests_test.newSysbench")
		b.Run(sysTyp, func(b *testing.B) {
			for _, opFn := range []sysbenchWorkload{
				sysbenchOltpReadOnly,
				sysbenchOltpWriteOnly,
				sysbenchOltpReadWrite,
			} {
				opTyp := runtime.FuncForPC(reflect.ValueOf(opFn).Pointer()).Name()
				opTyp = strings.TrimPrefix(opTyp, "github.com/cockroachdb/cockroach/pkg/sql/tests_test.sysbench")
				b.Run(opTyp, func(b *testing.B) {
					defer func() {
						if r := recover(); r != nil {
							b.Fatalf("%+v", r)
						}
					}()

					ctx := context.Background()
					sys, cleanup := sysFn(ctx, b)
					defer cleanup()

					rng := rand.New(rand.NewSource(0))
					sys.prep(rng)

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						opFn(sys, rng)
					}
				})
			}
		})
	}
}

func try0(err error) {
	if err != nil {
		panic(errors.WrapWithDepth(1, err, "try failed"))
	}
}

// NB: this should be called try1 to follow the naming scheme established above,
// but this is the most common form of try, so we give it the shorter name.
func try[T any](t T, err error) T {
	if err != nil {
		panic(errors.WrapWithDepth(1, err, "try failed"))
	}
	return t
}
