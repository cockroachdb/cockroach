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
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/rlmcpherson/s3gof3r"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
	singleNode                  = 1
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
	bankDataInsertRows = 1000
)

func bankDataInsertStmts(count int) []string {
	rng, _ := randutil.NewPseudoRand()

	var statements []string
	var insert bytes.Buffer
	for i := 0; i < count; i += bankDataInsertRows {
		insert.Reset()
		insert.WriteString(`INSERT INTO bench.bank VALUES `)
		for j := i; j < i+bankDataInsertRows && j < count; j++ {
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

// allRangeDescriptors fetches all meta2 RangeDescriptors using the given txn.
func allRangeDescriptors(txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

func rebalanceLeases(t testing.TB, tc *testcluster.TestCluster) {
	kvDB := tc.Server(0).KVClient().(*client.DB)
	txn := client.NewTxn(context.Background(), *kvDB)
	rangeDescs, err := allRangeDescriptors(txn)
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

	// TODO(dan): Some tests don't need multiple nodes, but the test setup
	// hangs with 1. Investigate.
	if numAccounts == 0 && clusterSize < multiNode {
		clusterSize = multiNode
		t.Logf("setting cluster size to %d due to a bug", clusterSize)
	}

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
			// This occasionally flakes, so ignore errors.
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

func TestBackupRestoreLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()
	backupAndRestore(ctx, t, sqlDB, dir, numAccounts)
}

// TestBackupRestoreS3 hits the real S3 and so could occasionally be flaky. It's
// only run if the AWS_S3_BUCKET environment var is set.
func TestBackupRestoreS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s3Keys, err := s3gof3r.EnvKeys()
	if err != nil {
		s3Keys, err = s3gof3r.InstanceKeys()
		if err != nil {
			t.Skip("No AWS keys instance or env keys")
		}
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		// CRL uses a bucket `cockroach-backup-tests` that has a 24h TTL policy.
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, _, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "s3", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(storageccl.S3AccessKeyParam, s3Keys.AccessKey)
	values.Add(storageccl.S3SecretParam, s3Keys.SecretKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, sqlDB, uri.String(), numAccounts)
}

// TestBackupRestoreGoogleCloudStorage hits the real GCS and so could
// occasionally be flaky. It's only run if the GS_BUCKET environment var is set.
func TestBackupRestoreGoogleCloudStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	ctx, _, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreGoogleCloudStorage-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "gs", Host: bucket, Path: prefix}
	backupAndRestore(ctx, t, sqlDB, uri.String(), numAccounts)
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

func TestBackupRestoreInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 10

	ctx, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()

	// TODO(dan): The INTERLEAVE IN PARENT clause currently doesn't allow the
	// `db.table` syntax. Fix that and use it here instead of `SET DATABASE`.
	_ = sqlDB.Exec(`SET DATABASE = bench`)
	_ = sqlDB.Exec(`CREATE TABLE i0 (a INT, b INT, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)
	_ = sqlDB.Exec(`CREATE TABLE i0_0 (a INT, b INT, c INT, PRIMARY KEY (a, b, c)) INTERLEAVE IN PARENT i0 (a, b)`)
	_ = sqlDB.Exec(`CREATE TABLE i1 (a INT, b INT, PRIMARY KEY (a, b)) INTERLEAVE IN PARENT bank (a)`)

	// The bank table has numAccounts accounts, put 2x that in i0, 3x in i0_0,
	// and 4x in i1.
	for i := 0; i < numAccounts; i++ {
		_ = sqlDB.Exec(fmt.Sprintf(`INSERT INTO i0 VALUES (%d, 1), (%d, 2)`, i, i))
		_ = sqlDB.Exec(fmt.Sprintf(`INSERT INTO i0_0 VALUES (%d, 1, 1), (%d, 2, 2), (%d, 3, 3)`, i, i, i))
		_ = sqlDB.Exec(fmt.Sprintf(`INSERT INTO i1 VALUES (%d, 1), (%d, 2), (%d, 3), (%d, 4)`, i, i, i, i))
	}
	_ = sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir))

	t.Run("all tables in interleave hierarchy", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		kvDBRestore := tcRestore.Server(0).KVClient().(*client.DB)
		sqlDBRestore.Exec(bankCreateDatabase)

		table := parser.TableName{DatabaseName: "bench", TableName: "*"}
		newTables, err := Restore(ctx, *kvDBRestore, []string{dir}, table)
		if err != nil {
			t.Fatal(err)
		}
		if len(newTables) != 4 {
			t.Fatalf("expected to restore 1 table, got %d", len(newTables))
		}

		var rowCount int64
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Errorf("expected %d rows but found %d", numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.i0`).Scan(&rowCount)
		if rowCount != 2*numAccounts {
			t.Errorf("expected %d rows but found %d", 2*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.i0_0`).Scan(&rowCount)
		if rowCount != 3*numAccounts {
			t.Errorf("expected %d rows but found %d", 3*numAccounts, rowCount)
		}
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.i1`).Scan(&rowCount)
		if rowCount != 4*numAccounts {
			t.Errorf("expected %d rows but found %d", 4*numAccounts, rowCount)
		}
	})

	t.Run("interleaved table without parent", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		kvDBRestore := tcRestore.Server(0).KVClient().(*client.DB)
		sqlDBRestore.Exec(bankCreateDatabase)

		table := parser.TableName{DatabaseName: "bench", TableName: "i0"}
		_, err := Restore(ctx, *kvDBRestore, []string{dir}, table)
		if !testutils.IsError(err, "without interleave parent") {
			t.Fatalf("expected 'without interleave parent' error but got: %+v", err)
		}
	})

	t.Run("interleaved table without child", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		kvDBRestore := tcRestore.Server(0).KVClient().(*client.DB)
		sqlDBRestore.Exec(bankCreateDatabase)

		table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
		_, err := Restore(ctx, *kvDBRestore, []string{dir}, table)
		if !testutils.IsError(err, "without interleave child") {
			t.Fatalf("expected 'without interleave child' error but got: %+v", err)
		}
	})
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

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	_, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()

	var ts string
	sqlDB.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts)
	sqlDB.Exec(`TRUNCATE bench.bank`)
	sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s' AS OF SYSTEM TIME %s`, dir, ts))

	var rowCount int64
	sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
	if rowCount != 0 {
		t.Fatalf("expected 0 rows but found %d", rowCount)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))
	sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
	if rowCount != numAccounts {
		t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
	}
}

func TestPresplitRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, _, _, kvDB, _, cleanupFn := backupRestoreTestSetup(t, multiNode, 0)
	defer cleanupFn()

	numRangesTests := []int{0, 1, 2, 3, 4, 10}
	for testNum, numRanges := range numRangesTests {
		t.Run(strconv.Itoa(numRanges), func(t *testing.T) {
			baseKey := keys.MakeTablePrefix(uint32(keys.MaxReservedDescID + testNum))
			var splitPoints []roachpb.Key
			for i := 0; i < numRanges; i++ {
				key := encoding.EncodeUvarintAscending(append([]byte(nil), baseKey...), uint64(i))
				splitPoints = append(splitPoints, key)
			}
			if err := presplitRanges(ctx, *kvDB, splitPoints); err != nil {
				t.Error(err)
			}
			for _, splitPoint := range splitPoints {
				// presplitRanges makes the keys into row sentinels, so we have
				// to match that behavior.
				splitKey := keys.MakeRowSentinelKey(splitPoint)
				if err := kvDB.AdminSplit(ctx, splitKey); !testutils.IsError(err, "already split") {
					t.Errorf("missing split %s: %+v", splitKey, err)
				}
			}
		})
	}
}

func TestBackupLevelDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, dir, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	_ = sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir))

	// Verify that the sstables are in LevelDB format by checking the trailer
	// magic.
	var magic = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")
	foundSSTs := 0
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".sst" {
			foundSSTs++
			data, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(data, magic) {
				t.Fatalf("trailer magic is not LevelDB sstable: %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("%+v", err)
	}
	if foundSSTs == 0 {
		t.Fatal("found no sstables")
	}
}

func BenchmarkClusterBackup(b *testing.B) {
	defer tracing.Disable()()

	ctx, dir, tc, _, sqlDB, cleanupFn := backupRestoreTestSetup(b, multiNode, 0)
	defer cleanupFn()

	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	if _, err := Load(ctx, sqlDB.DB, bankStatementBuf(b.N), "bench", dir, ts, 0); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))

	for _, split := range bankSplitStmts(b.N, backupRestoreDefaultRanges) {
		sqlDB.Exec(split)
	}
	rebalanceLeases(b, tc)

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir)).Scan(
		&unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
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
			desc, err := Backup(ctx, *kvDB, dir, hlc.ZeroTimestamp, tc.Server(0).Clock().Now())
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(desc.DataSize)

			rebalanceLeases(b, tc)

			b.ResetTimer()
			table := parser.TableName{DatabaseName: "bench", TableName: "bank"}
			for i := 0; i < b.N; i++ {
				if _, err := Restore(ctx, *kvDB, []string{dir}, table); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
