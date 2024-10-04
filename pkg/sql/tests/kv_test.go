// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	kv2 "github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type kvInterface interface {
	Insert(rows, run int) error
	Update(rows, run int) error
	Delete(rows, run int) error
	Scan(rows, run int) error

	prep(rows int) error
	done()
}

// disableBackgroundWork disables background work in the cluster settings to
// make the benchmarks more predictable.
func disableBackgroundWork(st *cluster.Settings) {
	ts.TimeseriesStorageEnabled.Override(context.Background(), &st.SV, false)
	stats.AutomaticStatisticsClusterMode.Override(context.Background(), &st.SV, false)
}

// kvNative uses the native client package to implement kvInterface.
type kvNative struct {
	db     *kv2.DB
	epoch  int
	prefix string
	doneFn func()
}

func newKVNative(b *testing.B) kvInterface {
	st := cluster.MakeTestingClusterSettings()
	disableBackgroundWork(st)
	s, _, db := serverutils.StartServer(b, base.TestServerArgs{Settings: st})

	// Note that using the local client.DB isn't a strictly fair
	// comparison with SQL as we want these client requests to be sent
	// over the network.
	return &kvNative{
		db: db,
		doneFn: func() {
			s.Stopper().Stop(context.Background())
		},
	}
}

func newKVNativeAndEngine(tb testing.TB) (*kvNative, storage.Engine) {
	st := cluster.MakeTestingClusterSettings()
	disableBackgroundWork(st)
	s, _, db := serverutils.StartServer(tb, base.TestServerArgs{Settings: st})
	engines := s.Engines()
	if len(engines) != 1 {
		tb.Fatalf("unexpected number of engines %d", len(engines))
	}
	return &kvNative{
		db: db,
		doneFn: func() {
			s.Stopper().Stop(context.Background())
		},
	}, engines[0]
}

func (kv *kvNative) Insert(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(context.Background(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) Update(rows, run int) error {
	err := kv.db.Txn(context.Background(), func(ctx context.Context, txn *kv2.Txn) error {
		// Read all values in a batch.
		b := txn.NewBatch()
		// Don't permute the rows, to be similar to SQL which sorts the spans in a
		// batch.
		for i := 0; i < rows; i++ {
			b.GetForUpdate(fmt.Sprintf("%s%08d", kv.prefix, i), kvpb.BestEffort)
		}
		if err := txn.Run(ctx, b); err != nil {
			return err
		}
		// Now add one to each value and add as puts to write batch.
		wb := txn.NewBatch()
		for i, result := range b.Results {
			v := result.Rows[0].ValueInt()
			wb.Put(fmt.Sprintf("%s%08d", kv.prefix, i), v+1)
		}
		return txn.CommitInBatch(ctx, wb)
	})
	return err
}

func (kv *kvNative) Delete(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(context.Background(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Del(fmt.Sprintf("%s%08d", kv.prefix, i))
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) Scan(rows, run int) error {
	return kv.scanWithRowCountExpectation(rows, rows)
}

func (kv *kvNative) scanWithRowCountExpectation(rows, expectedRows int) error {
	var kvs []kv2.KeyValue
	err := kv.db.Txn(context.Background(), func(ctx context.Context, txn *kv2.Txn) error {
		var err error
		kvs, err = txn.Scan(ctx, fmt.Sprintf("%s%08d", kv.prefix, 0), fmt.Sprintf("%s%08d", kv.prefix, rows), int64(rows))
		return err
	})
	if len(kvs) != expectedRows {
		return errors.Errorf("expected %d rows; got %d", rows, len(kvs))
	}
	return err
}

func (kv *kvNative) prepWithAdditionalLength(rows int, addKeyLen int) error {
	kv.epoch++
	addKeyLenPrefix := make([]byte, addKeyLen)
	for i := range addKeyLenPrefix {
		addKeyLenPrefix[i] = byte(i % 128)
	}
	var addKeyLenPrefixStr string
	if len(addKeyLenPrefix) > 0 {
		addKeyLenPrefixStr = string(addKeyLenPrefix)
	}
	kv.prefix = fmt.Sprintf("%d/%s", kv.epoch, addKeyLenPrefixStr)
	if rows == 0 {
		return nil
	}
	err := kv.db.Txn(context.Background(), func(ctx context.Context, txn *kv2.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(ctx, b)
	})
	return err
}

func (kv *kvNative) prep(rows int) error {
	return kv.prepWithAdditionalLength(rows, 0)
}

func (kv *kvNative) done() {
	kv.doneFn()
}

// kvSQL is a SQL-based implementation of the KV interface.
type kvSQL struct {
	db     *gosql.DB
	buf    bytes.Buffer
	doneFn func()
}

func newKVSQL(b *testing.B) kvInterface {
	st := cluster.MakeTestingClusterSettings()
	disableBackgroundWork(st)
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{Settings: st, UseDatabase: "bench"})

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	kv := &kvSQL{}
	kv.db = db
	kv.doneFn = func() {
		s.Stopper().Stop(context.Background())
	}
	return kv
}

func (kv *kvSQL) Insert(rows, run int) error {
	firstRow := rows * run
	defer kv.buf.Reset()
	kv.buf.WriteString(`INSERT INTO bench.kv VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, "('%08d', %d)", i+firstRow, i)
	}
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) Update(rows, run int) error {
	return kv.UpdateWithShift(rows, 0)
}

func (kv *kvSQL) UpdateWithShift(rows, run int) error {
	startRow := rows * run
	perm := rand.Perm(rows)
	defer kv.buf.Reset()
	kv.buf.WriteString(`UPDATE bench.kv SET v = v + 1 WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, `'%08d'`, startRow+perm[j])
	}
	kv.buf.WriteString(`)`)
	_, err := kv.db.Exec(kv.buf.String())
	return err
}

func (kv *kvSQL) Delete(rows, run int) error {
	firstRow := rows * run
	defer kv.buf.Reset()
	kv.buf.WriteString(`DELETE FROM bench.kv WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, `'%08d'`, j+firstRow)
	}
	kv.buf.WriteString(`)`)
	res, err := kv.db.Exec(kv.buf.String())
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != int64(rows) {
		return errors.Errorf("expected %d rows affected, found %d", rows, rowsAffected)
	}
	return nil
}

func (kv *kvSQL) Scan(count, run int) error {
	rows, err := kv.db.Query(fmt.Sprintf("SELECT * FROM bench.kv LIMIT %d", count))
	if err != nil {
		return err
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if n != count {
		return errors.Errorf("unexpected result count: %d (expected %d)", n, count)
	}
	return nil
}

func (kv *kvSQL) prep(rows int) error {
	if _, err := kv.db.Exec(`DROP TABLE IF EXISTS bench.kv`); err != nil {
		return err
	}
	schema := `
CREATE TABLE IF NOT EXISTS bench.kv (
  k STRING PRIMARY KEY,
  v INT,
  FAMILY (k, v)
)
`
	if _, err := kv.db.Exec(schema); err != nil {
		return err
	}
	if rows == 0 {
		return nil
	}
	defer kv.buf.Reset()
	kv.buf.WriteString(`INSERT INTO bench.kv VALUES `)
	numRowsInBatch := 0
	for i := 0; i < rows; i++ {
		if numRowsInBatch > 0 {
			kv.buf.WriteString(", ")
		}
		fmt.Fprintf(&kv.buf, "('%08d', %d)", i, i)
		numRowsInBatch++
		// Break initial inserts into batches of 1000 rows, since some tests can
		// overflow the batch limits.
		if numRowsInBatch > 1000 {
			if _, err := kv.db.Exec(kv.buf.String()); err != nil {
				return err
			}
			kv.buf.Reset()
			kv.buf.WriteString(`INSERT INTO bench.kv VALUES `)
			numRowsInBatch = 0
		}
	}
	var err error
	if numRowsInBatch > 0 {
		if _, err = kv.db.Exec(kv.buf.String()); err != nil {
			return err
		}
	}
	// Ensure stats are up-to-date.
	_, err = kv.db.Exec("ANALYZE bench.kv")
	return err
}

func (kv *kvSQL) done() {
	kv.doneFn()
}

func BenchmarkKV(b *testing.B) {
	defer log.Scope(b).Close(b)
	for i, opFn := range []func(kvInterface, int, int) error{
		kvInterface.Insert,
		kvInterface.Update,
		kvInterface.Delete,
		kvInterface.Scan,
	} {
		opName := runtime.FuncForPC(reflect.ValueOf(opFn).Pointer()).Name()
		opName = strings.TrimPrefix(opName, "github.com/cockroachdb/cockroach/pkg/sql/tests_test.kvInterface.")
		b.Run(opName, func(b *testing.B) {
			for _, kvFn := range []func(*testing.B) kvInterface{
				newKVNative,
				newKVSQL,
			} {
				kvTyp := runtime.FuncForPC(reflect.ValueOf(kvFn).Pointer()).Name()
				kvTyp = strings.TrimPrefix(kvTyp, "github.com/cockroachdb/cockroach/pkg/sql/tests_test.newKV")
				b.Run(kvTyp, func(b *testing.B) {
					for _, rows := range []int{1, 10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
							kv := kvFn(b)
							defer kv.done()

							var prepRows int
							switch i {
							case 0: // Insert
								prepRows = 0
							case 1: // Update
								prepRows = rows
							case 2: // Delete
								prepRows = rows * b.N
							case 3: // Scan
								prepRows = rows
							default:
								b.Fatal("unexpected op")
							}

							if err := kv.prep(prepRows); err != nil {
								b.Fatal(err)
							}
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if err := opFn(kv, rows, i); err != nil {
									b.Fatal(err)
								}
							}
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}

// This is a narrower and tweaked version of BenchmarkKV/Update/SQL that does
// SQL queries of the form UPDATE bench.kv SET v = v + 1 WHERE k IN (...).
// This will eventually be merged back into BenchmarkKV above, but we are
// keeping it separate for now since changing BenchmarkKV reasonably for all
// numbers of rows is tricky due to the optimization that switches scans for a
// large set specified by the SQL IN operator to full table scans that are not
// bounded by the [smallest, largest] key in the set.
//
// TODO(sumeer): The wide scan issue seems fixed by running "ANALYZE bench.kv"
// in kvSQL.prep. Confirm that this is indeed sufficient, and also change
// kvNative to be consistent with kvSQL.
//
// This benchmark does updates to 100 existing rows in a table. The number of
// versions is limited, to be more realistic (unlike BenchmarkKV which keeps
// updating the same rows for all benchmark iterations).
//
// The benchmarks stresses KV and storage performance involving medium size
// batches of work (in this case batches of 100, due to the 100 rows being
// updated), and can be considered a reasonable proxy for KV and storage
// performance (and to target improvements in those layers). We are not
// focusing on smaller batches because latency improvements for queries that
// are small and already fast are not really beneficial to the user.
// Specifically, these transactional update queries run as a 1PC transaction
// and do two significant pieces of work in storage:
//   - A read-only batch with 100 ScanRequests (this should eventually be
//     optimized by SQL to 100 GetRequests
//     https://github.com/cockroachdb/cockroach/issues/46758). The spans in the
//     batch are in sorted order. At the storage layer, the same iterator is
//     reused across the requests in a batch, and results in the following
//     sequence of calls repeated a 100 times: SetBounds, SeekGE, <iterate>.
//     The <iterate> part is looking for the next MVCCKey (not version) within
//     the span, and will not find such a key, but needs to step over the
//     versions of the key that it did find. This exercises the
//     pebbleMVCCScanner's itersBeforeSeek optimization, and will only involve
//     Next calls if the versions are <= 5. Else it will Seek after doing Next 5
//     times. That is, if there are k version per key and k <= 5, <Iterate> will
//     be k Next calls. If k > 5, there will be 5 Next calls followed by a
//     SeekGE. The maxVersions=8 benchmark below has some iterations that will
//     need to do this seek.
//   - A write batch with 100 PutRequests, again in sorted order. At
//     the storage layer, the same iterator will get reused across the requests
//     in a batch, and results in 100 SeekPrefixGE calls to that iterator.
//     Note that in this case the Distinct batch optimization is not being used.
//     Even the experimental approach in
//     https://github.com/sumeerbhola/cockroach/commit/eeeec51bd40ef47e743dc0c9ca47cf15710bae09
//     indicates that we cannot use an unindexed Pebble batch (which would have
//     been an optimization).
//
// This workload has keys that are clustered in the storage key space. Also,
// the volume of data is small, so the Pebble iterator stack is not deep. Both
// these things may not be representative of the real world. I like to run
// this with -benchtime 5000x which increases the amount of data in the
// engine, and causes data to be spilled from the memtable into sstables, and
// keeps the engine size consistent across runs.
//
// TODO(sumeer): consider disabling load-based splitting so that tests with
// large number of iterations, and hence large volume of data, stay
// predictable.
func BenchmarkKVAndStorageUsingSQL(b *testing.B) {
	defer log.Scope(b).Close(b)
	const rowsToUpdate = 100
	for _, maxVersions := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("versions=%d", maxVersions), func(b *testing.B) {
			kv := newKVSQL(b).(*kvSQL)
			defer kv.done()
			numUpdatesToRow := maxVersions - 1
			// We only need ceil(b.N/numUpdatesToRow) * rowsToUpdate, but create the
			// maximum number of rows needed by the smallest setting of maxVersions
			// so that the comparison across settings is not as affected by the size
			// of the engine. Otherwise the benchmark with maxVersions=2 creates
			// more keys, resulting in more files in the engine, which makes it
			// slower.
			rowsToInit := b.N * rowsToUpdate
			if err := kv.prep(rowsToInit); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kv.UpdateWithShift(rowsToUpdate, i/numUpdatesToRow); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// TODO(sumeer): also benchmark via SQL like the other benchmarks in this
// file.
//
// Benchmarks scanning a span of 1000 keys, where the keys are ~50 bytes in
// length and values are tiny (integers), with a varying number of versions
// per key, and the latest version being alive or a tombstone. It only
// measures the CPU effect since the total amount of data is tiny. The
// underlying Pebble engine is varied in the sstable format, with or without
// value blocks.
func BenchmarkKVAndStorageMultipleVersions(b *testing.B) {
	defer log.Scope(b).Close(b)
	const numRows = 1000
	const additionalLen = 50
	for _, numVersions := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, lastVersionIsTombstone := range []bool{false, true} {
				if numVersions == 1 && lastVersionIsTombstone == true {
					// Invalid combination of params.
					continue
				}
				b.Run(fmt.Sprintf("last-is-tombstone=%t", lastVersionIsTombstone), func(b *testing.B) {
					kv, eng := newKVNativeAndEngine(b)
					defer kv.done()
					if err := kv.prepWithAdditionalLength(numRows, additionalLen); err != nil {
						b.Fatal(err)
					}
					for i := 1; i < numVersions-1; i++ {
						require.NoError(b, kv.Update(numRows, 0))
					}
					expectedRows := numRows
					if numVersions > 1 {
						if lastVersionIsTombstone {
							require.NoError(b, kv.Delete(numRows, 0))
							expectedRows = 0
						} else {
							require.NoError(b, kv.Update(numRows, 0))
						}
					}
					require.NoError(b, eng.Flush())
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := kv.scanWithRowCountExpectation(numRows, expectedRows); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
				})
			}
		})
	}
}
