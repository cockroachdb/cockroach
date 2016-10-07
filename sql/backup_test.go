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
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/pkg/errors"
)

const (
	backupRestoreClusterSize    = 3
	backupRestoreDefaultRanges  = 10
	backupRestoreRowPayloadSize = 100

	bankCreateDatabase = `CREATE DATABASE bench`
	bankCreateTable    = `CREATE TABLE bench.bank (
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`
)

func testingTempDir(t testing.TB, depth int) (string, func()) {
	_, _, name := caller.Lookup(depth + 1)
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

func TestIntersectHalfOpen(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b1 := []byte{1}
	b2 := []byte{2}
	b3 := []byte{3}
	b4 := []byte{4}

	tests := []struct {
		start1 []byte
		end1   []byte
		start2 []byte
		end2   []byte
		starti []byte
		endi   []byte
	}{
		{b1, b2, b2, b3,
			nil, nil},
		{b1, b2, b3, b4,
			nil, nil},
		{b1, b3, b2, b3,
			b2, b3},
		{b1, b4, b2, b3,
			b2, b3},

		{b2, b3, b1, b2,
			nil, nil},
		{b3, b4, b1, b2,
			nil, nil},
		{b2, b3, b1, b3,
			b2, b3},
		{b2, b3, b1, b4,
			b2, b3},

		{b1, b4, b1, b4,
			b1, b4},
	}

	for i, test := range tests {
		s, e := sql.IntersectHalfOpen(test.start1, test.end1, test.start2, test.end2)
		if !bytes.Equal(s, test.starti) || !bytes.Equal(e, test.endi) {
			t.Errorf("%d: got (%x, %x) expected (%x, %x)", i, s, e, test.starti, test.endi)
		}
	}
}

func bankDataInsertStmts(count int) []string {
	rng, _ := randutil.NewPseudoRand()

	var statements []string
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
		statements = append(statements, insert.String())
	}
	return statements
}

func bankSplitStmts(numAccounts int, numRanges int) []string {
	var statements []string
	for i, incr := 1, numAccounts/numRanges; i < numRanges; i++ {
		s := fmt.Sprintf(`ALTER TABLE bench.bank SPLIT AT (%d)`, i*incr)
		statements = append(statements, s)
	}
	return statements
}

func rebalanceLeases(t testing.TB, tc *testcluster.TestCluster) {
	kvDB := tc.Server(0).KVClient().(*client.DB)
	txn := client.NewTxn(context.Background(), *kvDB)
	rangeDescs, err := sql.AllRangeDescriptors(txn)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rangeDescs {
		target := tc.Target(int(r.RangeID) % tc.NumServers())
		if err := tc.TransferRangeLease(&r, target); err != nil {
			t.Fatal(err)
		}
	}
}

func backupRestoreTestSetup(
	t testing.TB, numAccounts int,
) (
	ctx context.Context,
	tempDir string,
	tc *testcluster.TestCluster,
	kvDB *client.DB,
	sqlDB *sqlutils.SQLRunner,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testingTempDir(t, 1)

	// Use ReplicationManual so we can force full replication, which is needed
	// to later move the leases around.
	tc = testcluster.StartTestCluster(t, backupRestoreClusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	sqlDB = sqlutils.MakeSQLRunner(t, tc.Conns[0])
	kvDB = tc.Server(0).KVClient().(*client.DB)

	sqlDB.Exec(bankCreateDatabase)
	sqlDB.Exec(bankCreateTable)
	for _, insert := range bankDataInsertStmts(numAccounts) {
		sqlDB.Exec(insert)
	}
	for _, split := range bankSplitStmts(numAccounts, backupRestoreDefaultRanges) {
		sqlDB.Exec(split)
	}

	targets := make([]testcluster.ReplicationTarget, backupRestoreClusterSize-1)
	for i := 1; i < backupRestoreClusterSize; i++ {
		targets[i-1] = tc.Target(i)
	}
	txn := client.NewTxn(ctx, *kvDB)
	rangeDescs, err := sql.AllRangeDescriptors(txn)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rangeDescs {
		if _, err := tc.AddReplicas(r.StartKey.AsRawKey(), targets...); err != nil {
			t.Fatal(err)
		}
	}

	cleanupFn := func() {
		tc.Stopper().Stop()
		dirCleanupFn()
	}

	return ctx, dir, tc, kvDB, sqlDB, cleanupFn
}

func TestBackupRestoreOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, dir, tc, kvDB, _, cleanupFn := backupRestoreTestSetup(t, numAccounts)
	defer cleanupFn()

	{
		desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
		if err != nil {
			t.Fatal(err)
		}
		approxDataSize := int64(backupRestoreRowPayloadSize) * numAccounts
		if max := approxDataSize * 2; desc.DataSize < approxDataSize || desc.DataSize > 2*max {
			t.Errorf("expected data size in [%d,%d] but was %d", approxDataSize, max, desc.DataSize)
		}
	}

	// Start a new cluster to restore into.
	{
		tcRestore := testcluster.StartTestCluster(t, backupRestoreClusterSize, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		kvDBRestore := tcRestore.Server(0).KVClient().(*client.DB)

		// Restore assumes the database exists.
		sqlDBRestore.Exec(bankCreateDatabase)

		table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
		if _, err := sql.Restore(ctx, *kvDBRestore, dir, table); err != nil {
			t.Fatal(err)
		}

		var rowCount int
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
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
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()

	const numAccounts = 10
	const backupRestoreIterations = 10

	ctx, baseDir, tc, kvDB, sqlDB, cleanupFn := backupRestoreTestSetup(t, numAccounts)
	defer cleanupFn()

	tc.Stopper().RunWorker(func() {
		// Use a different sql gateway to make sure leasing is right.
		startBankTransfers(t, tc.Stopper(), tc.Conns[len(tc.Conns)-1], numAccounts)
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
			sqlDB.QueryRow(`SELECT SUM(balance*balance) FROM bench.bank`).Scan(&newSquaresSum)
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
			sqlDB.QueryRow(`SELECT SUM(balance*balance) FROM bench.bank`).Scan(&newSquaresSum)
			if squaresSum == newSquaresSum {
				return errors.Errorf("squared deviation didn't change, still %d", newSquaresSum)
			}
			return nil
		})
		squaresSum = newSquaresSum

		var sum int64
		sqlDB.QueryRow(`SELECT SUM(balance) FROM bench.bank`).Scan(&sum)
		if sum != 0 {
			t.Fatalf("The bank is not in good order. Total value: %d", sum)
		}
	}
}

func BenchmarkClusterBackup(b *testing.B) {
	defer tracing.Disable()()

	for _, numAccounts := range []int{10, 100, 1000, 10000} {
		b.Run(strconv.Itoa(numAccounts), func(b *testing.B) {
			ctx, dir, tc, kvDB, _, cleanupFn := backupRestoreTestSetup(b, numAccounts)
			defer cleanupFn()
			rebalanceLeases(b, tc)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(desc.DataSize)
			}
		})
	}
}

func BenchmarkClusterRestore(b *testing.B) {
	defer tracing.Disable()()

	// TODO(dan): count=10000 has some issues replicating. Investigate.
	for _, numAccounts := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(numAccounts), func(b *testing.B) {
			ctx, dir, tc, kvDB, _, cleanupFn := backupRestoreTestSetup(b, numAccounts)
			defer cleanupFn()

			// TODO(dan): Once mjibson's sql -> kv function is committed, use it
			// here on the output of bankDataInsert to generate the backup data
			// instead of this call.
			desc, err := sql.Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(desc.DataSize)

			rebalanceLeases(b, tc)

			b.ResetTimer()
			table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
			for i := 0; i < b.N; i++ {
				if _, err := sql.Restore(ctx, *kvDB, dir, table); err != nil {
					b.Fatal(err)
				}
			}
		})
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
