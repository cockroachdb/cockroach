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
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
			fmt.Fprintf(&insert, `(%d, %d, 'initial-%s')`, j, 0, payload)
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

func backupRestoreTestSetup(
	t testing.TB, clusterSize int, numAccounts int,
) (
	ctx context.Context,
	tempDir string,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	cleanup func(),
) {
	ctx = context.Background()

	dir, dirCleanupFn := testutils.TempDir(t, 1)

	tc = testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{})
	sqlDB = sqlutils.MakeSQLRunner(t, tc.Conns[0])

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

	return ctx, dir, tc, sqlDB, cleanupFn
}

func TestBackupRestoreLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
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
		if max := approxDataSize * 2; dataSize < approxDataSize || dataSize > max {
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
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 10

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
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
		sqlDBRestore.Exec(bankCreateDatabase)

		sqlDBRestore.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))

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
		sqlDBRestore.Exec(bankCreateDatabase)

		_, err := sqlDBRestore.DB.Exec(fmt.Sprintf(`RESTORE TABLE bench.i0 FROM '%s'`, dir))
		if !testutils.IsError(err, "without interleave parent") {
			t.Fatalf("expected 'without interleave parent' error but got: %+v", err)
		}
	})

	t.Run("interleaved table without child", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		sqlDBRestore.Exec(bankCreateDatabase)

		_, err := sqlDBRestore.DB.Exec(fmt.Sprintf(`RESTORE TABLE bench.bank FROM '%s'`, dir))
		if !testutils.IsError(err, "without interleave child") {
			t.Fatalf("expected 'without interleave child' error but got: %+v", err)
		}
	})
}

func startBackgroundWrites(
	stopper *stop.Stopper, sqlDB *gosql.DB, wake chan struct{}, maxID int,
) error {
	rng, _ := randutil.NewPseudoRand()

	for {
		select {
		case <-stopper.ShouldQuiesce():
			return nil // All done.
		default:
			// Keep going.
		}

		id := rand.Intn(maxID)
		payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)

		updateFn := func() error {
			select {
			case <-stopper.ShouldQuiesce():
				return nil // All done.
			default:
				// Keep going.
			}
			_, err := sqlDB.Exec(`UPDATE bench.bank SET payload = $1 WHERE id = $2`, payload, id)
			return err
		}
		if err := util.RetryForDuration(testutils.DefaultSucceedsSoonDuration, updateFn); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func TestBackupRestoreBank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()

	const rows = 10
	const numBackgroundTasks = multiNode
	const backupRestoreIterations = 3

	_, baseDir, tc, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, rows)
	defer cleanupFn()

	bgActivity := make(chan struct{})

	for i := 0; i < numBackgroundTasks; i++ {
		taskNum := i
		tc.Stopper().RunWorker(func() {
			// Use different sql gateways to make sure leasing is right.
			err := startBackgroundWrites(tc.Stopper(), tc.Conns[taskNum%len(tc.Conns)], bgActivity, rows)
			if err != nil {
				t.Error(err)
			}
		})
	}

	// Use the bench.bank table as a key (id), value (balance) table with a
	// payload. Repeatedly run backups and restores while the background tasks are
	// mutating the table concurrently. Wait in between each backup and each
	// restore for the data to change.
	for i := 0; i < backupRestoreIterations; i++ {
		dir := filepath.Join(baseDir, strconv.Itoa(i))

		<-bgActivity

		// Set the id=balance invariant on each row and back up the table.
		_ = sqlDB.Exec(`UPDATE bench.bank SET balance = id`)

		_ = sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir))

		<-bgActivity

		// Break the id=balance invariant on every row in the current table.
		// When we restore, it'll blow away this data, but if restore doesn't
		// work we can tell by looking for these.
		_ = sqlDB.Exec(`UPDATE bench.bank SET balance = -1`)

		<-bgActivity

		// Restore and check id=balance holds again.
		_ = sqlDB.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dir))

		bad := sqlDB.QueryStr(`SELECT id, balance, writes, payload FROM bench.bank WHERE id != balance`)
		for _, r := range bad {
			t.Errorf("bad row ID %s = bal %s (payload (%s): %q)", r[0], r[1], r[2], r[3])
		}
		if len(bad) > 0 {
			break
		}
	}
}

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
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

	ctx, _, tc, _, cleanupFn := backupRestoreTestSetup(t, multiNode, 0)
	defer cleanupFn()
	kvDB := tc.Server(0).KVClient().(*client.DB)

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

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
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
