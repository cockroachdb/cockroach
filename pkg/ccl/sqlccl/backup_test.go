// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	multiNode                   = 3
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
		s, e := IntersectHalfOpen(test.start1, test.end1, test.start2, test.end2)
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
	// Asking for more splits than ranges doesn't make any sense. Because of the
	// way go benchmarks work, split each row into a range instead of erroring.
	if numRanges > numAccounts {
		numRanges = numAccounts
	}
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
	rangeDescs, err := AllRangeDescriptors(txn)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rangeDescs {
		target := tc.Target(int(r.RangeID) % tc.NumServers())
		if err := tc.TransferRangeLease(r, target); err != nil {
			t.Fatal(err)
		}
	}
	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}
}

func backupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int,
) (
	ctx context.Context,
	tempDir string,
	tc *testcluster.TestCluster,
	kvDB *client.DB,
	sqlDB *sqlutils.SQLRunner,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t, 1)

	tc = testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{})
	sqlDB = sqlutils.MakeSQLRunner(t, tc.Conns[0])
	kvDB = tc.Server(0).KVClient().(*client.DB)

	sqlDB.Exec(bankCreateDatabase)
	if numAccounts > 0 {
		sqlDB.Exec(bankCreateTable)
		for _, insert := range bankDataInsertStmts(numAccounts) {
			sqlDB.Exec(insert)
		}
		for _, split := range bankSplitStmts(numAccounts, backupRestoreDefaultRanges) {
			_, _ = sqlDB.DB.Exec(split)
		}
	}

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
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

	ctx, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()
	backupAndRestore(ctx, t, sqlDB, dir, numAccounts)
}

func backupAndRestore(
	ctx context.Context, t *testing.T, sqlDB *sqlutils.SQLRunner, dest string, numAccounts int64,
) {
	{
		var unused string
		var dataSize int64
		sqlDB.QueryRow(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dest)).Scan(
			&unused, &unused, &unused, &dataSize,
		)
		approxDataSize := int64(backupRestoreRowPayloadSize) * numAccounts
		if max := approxDataSize * 2; dataSize < approxDataSize || dataSize > 2*max {
			t.Errorf("expected data size in [%d,%d] but was %d", approxDataSize, max, dataSize)
		}
	}

	// Start a new cluster to restore into.
	{
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])

		// Restore assumes the database exists.
		sqlDBRestore.Exec(bankCreateDatabase)

		// Force the ID of the restored bank table to be different.
		sqlDBRestore.Exec(`CREATE TABLE bench.empty (a INT PRIMARY KEY)`)

		sqlDBRestore.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dest))

		var rowCount int64
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}
	}
}

func startBankTransfers(stopper *stop.Stopper, sqlDB *gosql.DB, numAccounts int) error {
	rng, _ := randutil.NewPseudoRand()

	for {
		select {
		case <-stopper.ShouldQuiesce():
			return nil // All done.
		default:
			// Keep going.
		}

		account := rand.Intn(numAccounts)
		payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)

		updateFn := func() error {
			select {
			case <-stopper.ShouldQuiesce():
				return nil // All done.
			default:
				// Keep going.
			}
			_, err := sqlDB.Exec(`UPDATE bench.bank SET payload = $1 WHERE id = $2`,
				payload, account)
			return err
		}
		if err := util.RetryForDuration(testutils.DefaultSucceedsSoonDuration, updateFn); err != nil {
			return err
		}
	}
}

func TestBackupRestoreBank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()

	const numAccounts = 10
	const numTransferTasks = multiNode
	const backupRestoreIterations = 3

	_, baseDir, tc, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()

	for i := 0; i < numTransferTasks; i++ {
		taskNum := i
		tc.Stopper().RunWorker(func() {
			// Use different sql gateways to make sure leasing is right.
			err := startBankTransfers(tc.Stopper(), tc.Conns[taskNum%len(tc.Conns)], numAccounts)
			if err != nil {
				t.Error(err)
			}
		})
	}

	waitForChecksumChange := func(currentChecksum uint32) uint32 {
		var newChecksum uint32
		testutils.SucceedsSoon(t, func() error {
			crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
			rows := sqlDB.Query(`SELECT payload FROM bench.bank`)
			defer rows.Close()
			var payload []byte
			for rows.Next() {
				if err := rows.Scan(&payload); err != nil {
					t.Fatal(err)
				}
				if _, err := crc.Write(payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			newChecksum = crc.Sum32()
			if newChecksum == currentChecksum {
				return errors.Errorf("waiting for checksum to change %d", newChecksum)
			}
			return nil
		})
		return newChecksum
	}

	// Use the bench.bank table as a key (id), value (balance) table with a
	// payload. Repeatedly run backups and restores while the transfer tasks are
	// mutating the table concurrently. Wait in between each backup and each
	// restore for the data to change.
	var checksum uint32
	for i := 0; i < backupRestoreIterations; i++ {
		dir := filepath.Join(baseDir, strconv.Itoa(i))

		// Set the id=balance invariant on each row and back up the table.
		checksum = waitForChecksumChange(checksum)
		_ = sqlDB.Exec(`UPDATE bench.bank SET balance = id`)
		_ = sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir))

		// Break the id=balance invariant on every row in the current table.
		// When we restore, it'll blow away this data, but if restore doesn't
		// work we can tell by looking for these.
		checksum = waitForChecksumChange(checksum)
		_ = sqlDB.Exec(`UPDATE bench.bank SET balance = -1`)

		// Restore and wait for the new descriptor to be gossiped out. We can
		// tell that this happened when id=balance holds again.
		checksum = waitForChecksumChange(checksum)
		_ = sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))
		testutils.SucceedsSoon(t, func() error {
			var failures int
			sqlDB.QueryRow(`SELECT COUNT(id) FROM bench.bank WHERE id != balance`).Scan(&failures)
			if failures > 0 {
				return errors.Errorf("The bank is not in good order. Total failures: %d", failures)
			}
			return nil
		})
	}
}

func BenchmarkClusterBackup(b *testing.B) {
	defer tracing.Disable()()

	for _, numAccounts := range []int{10, 100, 1000, 10000} {
		b.Run(strconv.Itoa(numAccounts), func(b *testing.B) {
			ctx, dir, tc, kvDB, _, cleanupFn := backupRestoreTestSetup(b, multiNode, numAccounts)
			defer cleanupFn()
			rebalanceLeases(b, tc)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desc, err := Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
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
			ctx, dir, tc, kvDB, _, cleanupFn := backupRestoreTestSetup(b, multiNode, numAccounts)
			defer cleanupFn()

			// TODO(dan): Once mjibson's sql -> kv function is committed, use it
			// here on the output of bankDataInsert to generate the backup data
			// instead of this call.
			desc, err := Backup(ctx, *kvDB, dir, tc.Server(0).Clock().Now())
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(desc.DataSize)

			rebalanceLeases(b, tc)

			b.ResetTimer()
			table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
			for i := 0; i < b.N; i++ {
				if _, err := Restore(ctx, *kvDB, dir, table); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSstRekey(b *testing.B) {
	// TODO(dan): DRY this with BenchmarkRocksDBSstFileReader.

	dir, cleanupFn := testutils.TempDir(b, 1)
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

		sst := engineccl.MakeRocksDBSstFileWriter()
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
	sst, err := engineccl.MakeRocksDBSstFileReader()
	if err != nil {
		b.Fatal(err)
	}
	if err := sst.AddFile(sstPath); err != nil {
		b.Fatal(err)
	}
	defer sst.Close()
	count := 0
	iterateFn := MakeRekeyMVCCKeyValFunc(newTableID, func(kv engine.MVCCKeyValue) (bool, error) {
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
