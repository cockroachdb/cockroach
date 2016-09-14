// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/ccl"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/pkg/errors"
)

const (
	backupRestoreDefaultRanges  = 10
	backupRestoreRowPayloadSize = 100
)

func testingTempDir(t testing.TB, depth int) (string, func()) {
	_, _, name := caller.Lookup(depth)
	dir, err := ioutil.TempDir("", name)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	return dir, cleanup
}

// setupBackupRestoreDB creates a table and inserts `count` rows. It then splits
// the kv ranges for the table as evenly as possible into `rangeCount` ranges.
func setupBackupRestoreDB(
	t testing.TB,
	ctx context.Context,
	tc *testcluster.TestCluster,
	count int,
	rangeCount int,
) []*roachpb.RangeDescriptor {
	rng, _ := randutil.NewPseudoRand()

	sqlDB := tc.Conns[0]
	kvDB := tc.Server(0).KVClient().(*client.DB)
	if _, err := sqlDB.Exec(`CREATE DATABASE bench`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`CREATE TABLE bench.bank (
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`); err != nil {
		t.Fatal(err)
	}
	var insert bytes.Buffer
	for i := 0; i < count; i += 1000 {
		insert.Reset()
		insert.WriteString(`INSERT INTO bench.bank VALUES `)
		for j := i; j < i+1000 && j < count; j++ {
			if j != i {
				insert.WriteRune(',')
			}
			payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
			fmt.Fprintf(&insert, `(%d, %d, '%s')`, j, 0, payload)
		}
		if _, err := sqlDB.Exec(insert.String()); err != nil {
			t.Fatal(err)
		}
	}

	if rangeCount > count {
		t.Fatalf("cannot create %d ranges out of %d rows", rangeCount, count)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "bench", "bank")
	tablePrimaryKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	tableColMap := map[sqlbase.ColumnID]int{tableDesc.Columns[0].ID: 0}
	row := make([]parser.Datum, 1)
	ranges := make([]*roachpb.RangeDescriptor, rangeCount)
	for i, incr := 1, count/rangeCount; i < rangeCount; i++ {
		row[0] = parser.NewDInt(parser.DInt(i * incr))
		splitKey, _, err := sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, tableColMap, row, tablePrimaryKeyPrefix)
		if err != nil {
			t.Fatal(err)
		}
		splitKey = keys.MakeRowSentinelKey(splitKey)
		util.SucceedsSoon(t, func() error {
			log.Infof(ctx, "splitting at %x %s", splitKey, roachpb.Key(splitKey))
			ranges[i-1], ranges[i], err = tc.SplitRange(splitKey)
			if err != nil {
				log.Infof(ctx, "error splitting: %s", err)
			}
			return err
		})
	}
	return ranges
}

func setupReplicationAndLeases(
	t testing.TB,
	tc *testcluster.TestCluster,
	ranges []*roachpb.RangeDescriptor,
	clusterSize int,
) {
	targets := make([]testcluster.ReplicationTarget, clusterSize-1)
	for i := 1; i < clusterSize; i++ {
		targets[i-1] = tc.Target(i)
	}

	for _, r := range ranges {
		if _, err := tc.AddReplicas(r.StartKey.AsRawKey(), targets...); err != nil {
			t.Fatal(err)
		}
		if err := tc.TransferRangeLease(r, tc.Target(int(r.RangeID)%clusterSize)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ccl.SkipIfNoCCL(t)
	ctx := context.Background()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()

	dir, cleanupFn := testingTempDir(t, 1)
	defer cleanupFn()

	// TODO(dan): Increase this clusterSize to 3. It works if the test timeout
	// is raised and with a util.SucceedsSoon wrapped around Backup, Restore,
	// and every sql Exec, but it seems like we should be fixing the underlying
	// issues.
	const clusterSize = 1
	const count = 1000

	{
		tc := testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      base.TestServerArgs{},
		})
		defer tc.Stopper().Stop()
		sqlDB := tc.Conns[0]
		kvDB := tc.Server(0).KVClient().(*client.DB)

		_ = setupBackupRestoreDB(t, ctx, tc, count, backupRestoreDefaultRanges)

		desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
		if err != nil {
			t.Fatal(err)
		}
		approxDataSize := int64(backupRestoreRowPayloadSize) * count
		if max := approxDataSize * 2; desc.DataSize < approxDataSize || desc.DataSize > 2*max {
			t.Errorf("expected data size in [%d,%d] but was %d", approxDataSize, max, desc.DataSize)
		}

		if _, err := sqlDB.Exec(`TRUNCATE bench.bank`); err != nil {
			t.Fatal(err)
		}

		var rowCount int
		if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount); err != nil {
			t.Fatal(err)
		}

		if rowCount != 0 {
			t.Fatalf("expected 0 rows but found %d", rowCount)
		}
	}

	// Start a new cluster to restore into.
	{
		tc := testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      base.TestServerArgs{},
		})
		defer tc.Stopper().Stop()
		sqlDB := tc.Conns[0]
		kvDB := tc.Server(0).KVClient().(*client.DB)

		// Force the Restore to rekey.
		if _, err := sqlDB.Exec(`CREATE DATABASE bench; CREATE TABLE bench.bank (a INT);`); err != nil {
			t.Fatal(err)
		}

		table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
		if _, err := sql.Restore(ctx, *kvDB, dir, table); err != nil {
			t.Fatal(err)
		}

		var rowCount int
		if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount); err != nil {
			t.Fatal(err)
		}

		if rowCount != count {
			t.Fatalf("expected %d rows but found %d", count, rowCount)
		}
	}
}

func startBankTransfers(t testing.TB, stopper *stop.Stopper, sqlDB *gosql.DB, numAccounts int) {
	const maxTransfer = 999
	for {
		select {
		case <-stopper.ShouldQuiesce():
			return // All done.
		default:
			// Keep going.
		}

		from := rand.Intn(numAccounts)
		to := rand.Intn(numAccounts - 1)
		for from == to {
			to = numAccounts - 1
		}

		amount := rand.Intn(maxTransfer)

		const update = `UPDATE bench.bank
				SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
				WHERE id IN ($1, $2)`
		util.SucceedsSoon(t, func() error {
			select {
			case <-stopper.ShouldQuiesce():
				return nil // All done.
			default:
				// Keep going.
			}
			_, err := sqlDB.Exec(update, from, to, amount)
			return err
		})
	}
}

func TestBackupRestoreBank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ccl.SkipIfNoCCL(t)
	ctx := context.Background()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()

	baseDir, cleanupFn := testingTempDir(t, 1)
	defer cleanupFn()

	// TODO(dan): Increase this clusterSize to 3. It works if the test timeout
	// is raised and with a util.SucceedsSoon wrapped around Backup, Restore,
	// and every sql Exec, but it seems like we should be fixing the underlying
	// issues.
	const clusterSize = 1
	const numAccounts = 10
	const backupRestoreIterations = 10

	tc := testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs:      base.TestServerArgs{},
	})
	stopper := tc.Stopper()
	defer stopper.Stop()
	sqlDB := tc.Conns[0]
	kvDB := tc.Server(0).KVClient().(*client.DB)

	_ = setupBackupRestoreDB(t, ctx, tc, numAccounts, backupRestoreDefaultRanges)

	stopper.RunWorker(func() {
		startBankTransfers(t, stopper, sqlDB, numAccounts)
	})

	// Loop continually doing backup and restores while the bank transfers are
	// running in a goroutine. After each iteration, check the invariant that
	// all balances sum to zero. Make sure the data changes a bit between each
	// backup and restore as well as after the restore before checking the
	// invariant by checking the sum of squares of balances, which is chosen to
	// be likely to change if any balances change.
	var squaresSum int64
	table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
	for i := 0; i < backupRestoreIterations; i++ {
		dir := filepath.Join(baseDir, strconv.Itoa(i))

		_, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
		if err != nil {
			t.Fatal(err)
		}

		var newSquaresSum int64
		util.SucceedsSoon(t, func() error {
			if err := sqlDB.QueryRow(`SELECT SUM(balance*balance) FROM bench.bank`).Scan(&newSquaresSum); err != nil {
				return err
			}

			if squaresSum == newSquaresSum {
				return errors.Errorf("squared deviation didn't change, still %d", newSquaresSum)
			}
			return nil
		})
		squaresSum = newSquaresSum

		if _, err := sql.Restore(ctx, *kvDB, dir, table); err != nil {
			t.Fatal(err)
		}

		util.SucceedsSoon(t, func() error {
			if err := sqlDB.QueryRow(`SELECT SUM(balance*balance) FROM bench.bank`).Scan(&newSquaresSum); err != nil {
				return err
			}

			if squaresSum == newSquaresSum {
				return errors.Errorf("squared deviation didn't change, still %d", newSquaresSum)
			}
			return nil
		})
		squaresSum = newSquaresSum

		var sum int64
		if err := sqlDB.QueryRow(`SELECT SUM(balance) FROM bench.bank`).Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != 0 {
			t.Fatalf("The bank is not in good order. Total value: %d", sum)
		}
	}
}

// TODO(dan): count=10000 has some issues replicating. Investigate.
func BenchmarkClusterBackup_10(b *testing.B)   { runBenchmarkClusterBackup(b, 3, 10) }
func BenchmarkClusterBackup_100(b *testing.B)  { runBenchmarkClusterBackup(b, 3, 100) }
func BenchmarkClusterBackup_1000(b *testing.B) { runBenchmarkClusterBackup(b, 3, 1000) }
func runBenchmarkClusterBackup(b *testing.B, clusterSize int, count int) {
	defer tracing.Disable()()
	ccl.SkipIfNoCCL(b)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(b, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)
	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	dir, cleanupFn := testingTempDir(b, 2)
	defer cleanupFn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(desc.DataSize)
	}
}

func BenchmarkClusterRestore_10(b *testing.B)    { runBenchmarkClusterRestore(b, 3, 10) }
func BenchmarkClusterRestore_100(b *testing.B)   { runBenchmarkClusterRestore(b, 3, 100) }
func BenchmarkClusterRestore_1000(b *testing.B)  { runBenchmarkClusterRestore(b, 3, 1000) }
func BenchmarkClusterRestore_10000(b *testing.B) { runBenchmarkClusterRestore(b, 3, 10000) }
func runBenchmarkClusterRestore(b *testing.B, clusterSize int, count int) {
	defer tracing.Disable()()
	ccl.SkipIfNoCCL(b)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(b, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)

	dir, cleanupFn := testingTempDir(b, 1)
	defer cleanupFn()

	desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(desc.DataSize)

	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	b.ResetTimer()
	table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
	for i := 0; i < b.N; i++ {
		if _, err := sql.Restore(ctx, *kvDB, dir, table); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSstRekey(b *testing.B) {
	// TODO(dan): DRY this with BenchmarkRocksDBSstFileReader.

	dir, cleanupFn := testingTempDir(b, 1)
	defer cleanupFn()

	sstPath := filepath.Join(dir, "sst")
	{
		const maxEntries = 100000
		const keyLen = 10
		const valLen = 100
		b.SetBytes(keyLen + valLen)

		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		kv := engine.MVCCKeyValue{
			Key:   engine.MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts},
			Value: make([]byte, valLen),
		}

		sst := engine.MakeRocksDBSstFileWriter()
		if err := sst.Open(sstPath); err != nil {
			b.Fatal(sst)
		}
		var entries = b.N
		if entries > maxEntries {
			entries = maxEntries
		}
		for i := 0; i < entries; i++ {
			payload := []byte(fmt.Sprintf("%09d", i))
			kv.Key.Key = kv.Key.Key[:0]
			kv.Key.Key = encoding.EncodeUvarintAscending(kv.Key.Key, uint64(i)) // tableID
			kv.Key.Key = encoding.EncodeUvarintAscending(kv.Key.Key, 0)         // indexID
			kv.Key.Key = encoding.EncodeBytesAscending(kv.Key.Key, payload)
			kv.Key.Key = keys.MakeRowSentinelKey(kv.Key.Key)
			copy(kv.Value, payload)
			if err := sst.Add(kv); err != nil {
				b.Fatal(err)
			}
		}
		if err := sst.Close(); err != nil {
			b.Fatal(err)
		}
	}

	const newTableID = 100

	b.ResetTimer()
	sst, err := engine.MakeRocksDBSstFileReader()
	if err != nil {
		b.Fatal(err)
	}
	if err := sst.AddFile(sstPath); err != nil {
		b.Fatal(err)
	}
	defer sst.Close()
	count := 0
	iterateFn := sql.MakeRekeyMVCCKeyValFunc(newTableID, func(kv engine.MVCCKeyValue) (bool, error) {
		count++
		if count >= b.N {
			return true, nil
		}
		return false, nil
	})
	for {
		if err := sst.Iterate(engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}, iterateFn); err != nil {
			b.Fatal(err)
		}
		if count >= b.N {
			break
		}
	}
}
