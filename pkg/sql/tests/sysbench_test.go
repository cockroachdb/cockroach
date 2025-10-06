// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	runtimepprof "runtime/pprof"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// This file contains a microbenchmark test suite that emulates the sysbench
// benchmark tool (https://github.com/akopytov/sysbench), using the same schema
// and operations as sysbench's "oltp" workloads.

// These constants match the values that we use in the sysbench roachtest suite.
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
	sysbenchDB       = `sysbench`
	sysbenchTableFmt = `sbtest%d`
	sysbenchCreateDB = `CREATE DATABASE ` + sysbenchDB
	// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L193
	sysbenchCreateTable = `CREATE TABLE sbtest%d(
	  id INT8 PRIMARY KEY,
	  k INT8 NOT NULL DEFAULT 0,
	  c CHAR(120) NOT NULL DEFAULT '',
	  pad CHAR(60) NOT NULL DEFAULT ''
	)`
	sysbenchCreateIndex = `CREATE INDEX k_%[1]d ON sbtest%[1]d(k)` // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L245
	sysbenchAnalyze     = `ANALYZE sbtest%[1]d`

	sysbenchStmtBegin          = `BEGIN`
	sysbenchStmtCommit         = `COMMIT`
	sysbenchStmtPointSelect    = `SELECT c FROM sbtest%d WHERE id=$1`                                    // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L252
	sysbenchStmtSimpleRange    = `SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2`                     // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L255
	sysbenchStmtSumRange       = `SELECT sum(k) FROM sbtest%d WHERE id BETWEEN $1 AND $2`                // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L258
	sysbenchStmtOrderRange     = `SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c`          // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L261
	sysbenchStmtDistinctRange  = `SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c` // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L264
	sysbenchStmtIndexUpdate    = `UPDATE sbtest%d SET k=k+1 WHERE id=$1`                                 // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L267
	sysbenchStmtNonIndexUpdate = `UPDATE sbtest%d SET c=$2 WHERE id=$1`                                  // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L270
	sysbenchStmtDelete         = `DELETE FROM sbtest%d WHERE id=$1`                                      // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L273
	sysbenchStmtInsert         = `INSERT INTO sbtest%d (id, k, c, pad) VALUES ($1, $2, $3, $4)`          // https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L276
)

// sysbenchDriverConstructor constructs a new sysbenchDriver, along with a
// cleanup function for that driver.
type sysbenchDriverConstructor func(context.Context, *testing.B) (sysbenchDriver, func())

func newTestCluster(
	b *testing.B, nodes int, localRPCFastPath bool,
) serverutils.TestClusterInterface {
	st := cluster.MakeTestingClusterSettings()
	// NOTE: disabling background work makes the benchmark more predictable, but
	// also moderately less realistic.
	disableBackgroundWork(st)
	const cacheSize = 2 * 1024 * 1024 * 1024 // 2GB
	return serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings:  st,
			CacheSize: cacheSize,
			Knobs: base.TestingKnobs{
				DialerKnobs: nodedialer.DialerTestingKnobs{
					TestingNoLocalClientOptimization: !localRPCFastPath,
				},
			},
		}},
	)
}

// sysbenchSQL is SQL-based implementation of sysbenchDriver. It runs SQL
// statements against a single node cluster.
//
// TODO(nvanbenschoten): add a 3-node cluster variant of this driver.
// TODO(nvanbenschoten): add a variant of this driver which bypasses the gRPC
// local fast-path optimization.
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

func newSysbenchSQL(nodes int, localRPCFastPath bool) sysbenchDriverConstructor {
	return func(ctx context.Context, b *testing.B) (sysbenchDriver, func()) {
		tc := newTestCluster(b, nodes, localRPCFastPath)
		try0(tc.WaitForFullReplication())
		pgURL, cleanupURL := tc.ApplicationLayer(0).PGUrl(b, serverutils.DBName(sysbenchDB))
		conn := try(pgx.Connect(ctx, pgURL.String()))
		cleanup := func() {
			cleanupURL()
			tc.Stopper().Stop(ctx)
		}
		return &sysbenchSQL{
			ctx:  ctx,
			conn: conn,
		}, cleanup
	}
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

		// Collect table statistics.
		try(s.conn.Exec(s.ctx, fmt.Sprintf(sysbenchAnalyze, i)))
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

// sysbenchKV is KV-based implementation of sysbenchDriver. It bypasses the SQL
// layer and runs the workload directly against the KV layer, on a single node
// cluster.
//
// TODO(nvanbenschoten): add a 3-node cluster variant of this driver.
// TODO(nvanbenschoten): add a variant of this driver which bypasses the gRPC
// local fast-path optimization.
type sysbenchKV struct {
	ctx         context.Context
	db          *kv.DB
	txn         *kv.Txn
	pkPrefix    [sysbenchTables]roachpb.Key
	indexPrefix [sysbenchTables]roachpb.Key
}

func newSysbenchKV(nodes int, localRPCFastPath bool) sysbenchDriverConstructor {
	return func(ctx context.Context, b *testing.B) (sysbenchDriver, func()) {
		tc := newTestCluster(b, nodes, localRPCFastPath)
		db := tc.Server(0).DB()
		cleanup := func() {
			tc.Stopper().Stop(ctx)
		}
		return &sysbenchKV{
			ctx: ctx,
			db:  db,
		}, cleanup
	}
}

func (s *sysbenchKV) Begin() {
	s.txn = s.db.NewTxn(s.ctx, "sysbench")
}

func (s *sysbenchKV) Commit() {
	try0(s.txn.Commit(s.ctx))
}

func (s *sysbenchKV) PointSelect(t tableNum, id rowID) {
	key := s.pkKey(t, id)
	var val kv.KeyValue
	if s.txn != nil {
		val = try(s.txn.Get(s.ctx, key))
	} else {
		val = try(s.db.Get(s.ctx, key))
	}
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
	newIndexValue := s.indexValue()

	// Issue the mutation batch.
	var b kv.Batch
	b.Put(pkKey, pkValue)
	b.Del(oldIndexKey)
	b.Put(newIndexKey, newIndexValue)
	try0(s.txn.Run(s.ctx, &b))

	// Verify that the old secondary index key was found and deleted.
	if len(b.Results[1].Keys) != 1 {
		panic(errors.New("key not found and deleted"))
	}
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

	// Verify that both keys were found and deleted.
	for _, res := range b1.Results {
		if len(res.Keys) != 1 {
			panic(errors.New("key not found and deleted"))
		}
	}

	// Insert the new row.
	s.encodePKValue(pkValue, newK)
	newIndexKey := s.indexKey(t, newK, id)
	newIndexValue := s.indexValue()

	var b2 kv.Batch
	b2.CPut(pkKey, pkValue, nil /* expValue */)
	b2.InitPut(newIndexKey, newIndexValue, false /* failOnTombstones */)
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

func (s *sysbenchKV) pkValue(rng *rand.Rand, k kValue) []byte {
	const pkValueSize = 185 // measured from sysbenchSQL
	b := randutil.RandBytes(rng, pkValueSize)
	s.encodePKValue(b, k)
	return b
}

func (s *sysbenchKV) encodePKValue(b []byte, k kValue) {
	// Replace the first 8 bytes with the k value, so that we can decode k to keep
	// the secondary index in sync.
	binary.BigEndian.PutUint64(b, uint64(k))
}

func (s *sysbenchKV) decodePKValue(b []byte) kValue {
	// Extract the k value from the first 8 bytes. The rest is random.
	return kValue(binary.BigEndian.Uint64(b))
}

func (s *sysbenchKV) indexValue() []byte {
	return make([]byte, 0)
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
			pkValue := s.pkValue(rng, k)
			indexKey := s.indexKey(t, k, id)
			indexValue := s.indexValue()
			b.Put(pkKey, pkValue)
			b.Put(indexKey, indexValue)
		}
		try0(s.db.Run(s.ctx, &b))
	}

	// Scan across all tables to ensure that intent resolution has completed.
	for i := range sysbenchTables {
		try(s.db.Scan(s.ctx, s.pkPrefix[i], s.indexPrefix[i].PrefixEnd(), 0))
	}

	// Split between each table and index to ensure cross-range transactions.
	for i := range sysbenchTables {
		noExpiration := hlc.Timestamp{}
		try0(s.db.AdminSplit(s.ctx, s.pkPrefix[i], noExpiration))
		try0(s.db.AdminSplit(s.ctx, s.indexPrefix[i], noExpiration))
	}
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L419
func sysbenchExecutePointSelects(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchPointSelects {
		s.PointSelect(t, randRowID(rng))
	}
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L430
func sysbenchExecuteRange(op func(tableNum, rowID, rowID), count int, rng *rand.Rand) {
	t := randTableNum(rng)
	for range count {
		id := randRowID(rng)
		id2 := id + sysbenchRangeSize - 1
		op(t, id, id2)
	}
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L443
func sysbenchExecuteSimpleRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.SimpleRange, sysbenchSimpleRanges, rng)
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L447
func sysbenchExecuteSumRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.SumRange, sysbenchSumRanges, rng)
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L451
func sysbenchExecuteOrderRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.OrderRange, sysbenchOrderRanges, rng)
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L455
func sysbenchExecuteDistinctRanges(s sysbenchDriver, rng *rand.Rand) {
	sysbenchExecuteRange(s.DistinctRange, sysbenchDistinctRanges, rng)
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L459
func sysbenchExecuteIndexUpdates(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchIndexUpdates {
		s.IndexUpdate(t, randRowID(rng))
	}
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L469
func sysbenchExecuteNonIndexUpdates(s sysbenchDriver, rng *rand.Rand) {
	t := randTableNum(rng)
	for range sysbenchNonIndexUpdates {
		s.NonIndexUpdate(t, randRowID(rng), randCValue(rng))
	}
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_common.lua#L480
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

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_read_only.lua
func sysbenchOltpReadOnly(s sysbenchDriver, rng *rand.Rand) {
	s.Begin()
	sysbenchExecutePointSelects(s, rng)
	sysbenchExecuteSimpleRanges(s, rng)
	sysbenchExecuteSumRanges(s, rng)
	sysbenchExecuteOrderRanges(s, rng)
	sysbenchExecuteDistinctRanges(s, rng)
	s.Commit()
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_write_only.lua
func sysbenchOltpWriteOnly(s sysbenchDriver, rng *rand.Rand) {
	s.Begin()
	sysbenchExecuteIndexUpdates(s, rng)
	sysbenchExecuteNonIndexUpdates(s, rng)
	sysbenchExecuteDeleteInserts(s, rng)
	s.Commit()
}

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_read_write.lua#L44
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

// https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_point_select.lua#L32
func sysbenchOltpPointSelect(s sysbenchDriver, rng *rand.Rand) {
	// Run one point select per transaction, regardless of sysbenchPointSelects,
	// just like https://github.com/akopytov/sysbench/blob/de18a036cc65196b1a4966d305f33db3d8fa6f8e/src/lua/oltp_point_select.lua#L25-L26
	s.PointSelect(randTableNum(rng), randRowID(rng))
}

func sysbenchOltpBeginCommit(s sysbenchDriver, _ *rand.Rand) {
	s.Begin()
	s.Commit()
}

func BenchmarkSysbench(b *testing.B) {
	defer log.Scope(b).Close(b)

	drivers := []struct {
		name          string
		constructorFn sysbenchDriverConstructor
	}{
		{"SQL/1node_local", newSysbenchSQL(1, true)},
		{"SQL/1node_remote", newSysbenchSQL(1, false)},
		{"SQL/3node", newSysbenchSQL(3, false)},
		{"KV/1node_local", newSysbenchKV(1, true)},
		{"KV/1node_remote", newSysbenchKV(1, false)},
		{"KV/3node", newSysbenchKV(3, false)},
	}
	workloads := []struct {
		name string
		opFn sysbenchWorkload
	}{
		{"oltp_read_only", sysbenchOltpReadOnly},
		{"oltp_write_only", sysbenchOltpWriteOnly},
		{"oltp_read_write", sysbenchOltpReadWrite},
		{"oltp_point_select", sysbenchOltpPointSelect},
		{"oltp_begin_commit", sysbenchOltpBeginCommit},
	}
	for _, driver := range drivers {
		b.Run(driver.name, func(b *testing.B) {
			for _, workload := range workloads {
				b.Run(workload.name, func(b *testing.B) {
					defer func() {
						if r := recover(); r != nil {
							b.Fatalf("%+v", r)
						}
					}()

					ctx := context.Background()
					sys, cleanup := driver.constructorFn(ctx, b)
					defer cleanup()

					rng := rand.New(rand.NewSource(0))
					sys.prep(rng)

					defer startAllocsProfile(b).Stop(b)
					defer b.StopTimer()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						workload.opFn(sys, rng)
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

type doneFn func(testing.TB)

func (f doneFn) Stop(b testing.TB) {
	f(b)
}

func startAllocsProfile(b testing.TB) doneFn {
	out := benchmemFile(b)
	if out == "" {
		return func(tb testing.TB) {}
	}

	// The below is essentially cribbed from pprof.go in net/http/pprof.
	p := runtimepprof.Lookup("allocs")
	var buf bytes.Buffer
	runtime.GC()
	require.NoError(b, p.WriteTo(&buf, 0))
	pBase, err := profile.ParseData(buf.Bytes())
	require.NoError(b, err)

	return func(b testing.TB) {
		runtime.GC()
		var buf bytes.Buffer
		require.NoError(b, p.WriteTo(&buf, 0))
		pNew, err := profile.ParseData(buf.Bytes())
		require.NoError(b, err)
		pBase.Scale(-1)
		pMerged, err := profile.Merge([]*profile.Profile{pBase, pNew})
		require.NoError(b, err)
		pMerged.TimeNanos = pNew.TimeNanos
		pMerged.DurationNanos = pNew.TimeNanos - pBase.TimeNanos

		buf = bytes.Buffer{}
		require.NoError(b, pMerged.Write(&buf))
		require.NoError(b, os.WriteFile(out, buf.Bytes(), 0644))
	}
}

// If -test.benchmem is passed, also write a base alloc profile when the
// setup is done. This can be used via `pprof -base` to show only the
// allocs during run (excluding the setup).
//
// The file name for the base profile will be derived from -test.memprofile, and
// will contain it as a prefix (mod the file extension).
func benchmemFile(b testing.TB) string {
	b.Helper()
	var benchMemFile string
	var outputDir string
	pf := pflag.NewFlagSet("test", pflag.ContinueOnError)
	pf.StringVar(&benchMemFile, "test.memprofile", "", "")
	pf.StringVar(&outputDir, "test.outputdir", "", "")
	var args []string
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-") {
			if !strings.HasPrefix(arg, "-test.memprofile") && !strings.HasPrefix(arg, "-test.outputdir") {
				continue
			}
			arg = "-" + arg
		}
		args = append(args, arg)
	}
	require.NoError(b, pf.Parse(args))

	if benchMemFile == "" {
		return ""
	}

	saniRE := regexp.MustCompile(`\W+`)
	saniName := saniRE.ReplaceAllString(strings.TrimPrefix(b.Name(), "Benchmark"), "_")
	dest := strings.Replace(benchMemFile, ".", "_"+saniName+".", 1)
	if outputDir != "" {
		dest = filepath.Join(outputDir, dest)
	}
	return dest
}
