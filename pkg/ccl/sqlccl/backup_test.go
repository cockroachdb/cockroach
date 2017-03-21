// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	const numAccounts = 1000

	ctx, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()
	backupAndRestore(ctx, t, sqlDB, dir, numAccounts)
}

func backupAndRestore(
	ctx context.Context, t *testing.T, sqlDB *sqlutils.SQLRunner, dest string, numAccounts int64,
) {
	{
		sqlDB.Exec(`CREATE INDEX balance_idx ON bench.bank (balance)`)
		testutils.SucceedsSoon(t, func() error {
			var unused string
			var createTable string
			sqlDB.QueryRow(`SHOW CREATE TABLE bench.bank`).Scan(&unused, &createTable)
			if !strings.Contains(createTable, "balance_idx") {
				return errors.New("expected a balance_idx index")
			}
			return nil
		})

		var unused string
		var dataSize int64
		sqlDB.QueryRow(`BACKUP DATABASE bench TO $1`, dest).Scan(
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

		// Create some other descriptors to change up IDs
		sqlDBRestore.Exec(`CREATE DATABASE other`)
		sqlDBRestore.Exec(`CREATE TABLE other.empty (a INT PRIMARY KEY)`)

		// Restore assumes the database exists.
		sqlDBRestore.Exec(bankCreateDatabase)

		// Force the ID of the restored bank table to be different.
		sqlDBRestore.Exec(`CREATE TABLE bench.empty (a INT PRIMARY KEY)`)

		sqlDBRestore.Exec(`RESTORE bench.* FROM $1`, dest)

		var rowCount int64
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}

		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank@balance_idx`).Scan(&rowCount)
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

	const numAccounts = 10

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
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
		_ = sqlDB.Exec(`INSERT INTO i0 VALUES ($1, 1), ($1, 2)`, i)
		_ = sqlDB.Exec(`INSERT INTO i0_0 VALUES ($1, 1, 1), ($1, 2, 2), ($1, 3, 3)`, i)
		_ = sqlDB.Exec(`INSERT INTO i1 VALUES ($1, 1), ($1, 2), ($1, 3), ($1, 4)`, i)
	}
	_ = sqlDB.Exec(`BACKUP DATABASE bench TO $1`, dir)

	t.Run("all tables in interleave hierarchy", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		sqlDBRestore.Exec(bankCreateDatabase)

		sqlDBRestore.Exec(`RESTORE bench.* FROM $1`, dir)

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
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		sqlDBRestore.Exec(bankCreateDatabase)

		_, err := sqlDBRestore.DB.Exec(`RESTORE TABLE bench.i0 FROM $1`, dir)
		if !testutils.IsError(err, "without interleave parent") {
			t.Fatalf("expected 'without interleave parent' error but got: %+v", err)
		}
	})

	t.Run("interleaved table without child", func(t *testing.T) {
		tcRestore := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])
		sqlDBRestore.Exec(bankCreateDatabase)

		_, err := sqlDBRestore.DB.Exec(`RESTORE TABLE bench.bank FROM $1`, dir)
		if !testutils.IsError(err, "without interleave child") {
			t.Fatalf("expected 'without interleave child' error but got: %+v", err)
		}
	})
}

func TestBackupRestoreFKs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 30
	const createStore = "CREATE DATABASE store"

	_, dir, _, origDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()

	// Generate some testdata and back it up.
	{
		origDB.Exec(createStore)

		// customers has multiple inbound FKs, to different indexes.
		origDB.Exec(`CREATE TABLE store.customers (
			id INT PRIMARY KEY,
			email STRING UNIQUE
		)`)

		// orders has both in and outbound FKs (receipts and customers).
		// the index on placed makes indexIDs non-contiguous.
		origDB.Exec(`CREATE TABLE store.orders (
			id INT PRIMARY KEY,
			placed TIMESTAMP,
			INDEX (placed DESC),
			customerid INT REFERENCES store.customers
		)`)

		// unused makes our table IDs non-contiguous.
		origDB.Exec(`CREATE TABLE bench.unused (id INT PRIMARY KEY)`)

		// receipts is has a self-referential FK.
		origDB.Exec(`CREATE TABLE store.receipts (
			id INT PRIMARY KEY,
			reissue INT REFERENCES store.receipts(id),
			dest STRING REFERENCES store.customers(email),
			orderid INT REFERENCES store.orders
		)`)

		for i := 0; i < numAccounts; i++ {
			origDB.Exec(`INSERT INTO store.customers VALUES ($1, $1::string)`, i)
		}
		// Each even customerID gets 3 orders, with predictable order and receipt IDs.
		for cID := 0; cID < numAccounts; cID += 2 {
			for i := 0; i < 3; i++ {
				oID := cID*100 + i
				rID := oID * 10
				origDB.Exec(`INSERT INTO store.orders VALUES ($1, NOW(), $2)`, oID, cID)
				origDB.Exec(`INSERT INTO store.receipts VALUES ($1, NULL, $2, $3)`, rID, cID, oID)
				if i > 1 {
					origDB.Exec(`INSERT INTO store.receipts VALUES ($1, $2, $3, $4)`, rID+1, rID, cID, oID)
				}
			}
		}
		_ = origDB.Exec(`BACKUP DATABASE store TO $1`, dir)
	}

	origCustomers := origDB.QueryStr(`SHOW CONSTRAINTS FROM store.customers`)
	origOrders := origDB.QueryStr(`SHOW CONSTRAINTS FROM store.orders`)
	origReceipts := origDB.QueryStr(`SHOW CONSTRAINTS FROM store.receipts`)

	t.Run("restore everything to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		db := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		db.Exec(createStore)
		db.Exec(`RESTORE store.* FROM $1`, dir)
		// Restore's Validate checks all the tables point to each other correctly.

		db.CheckQueryResults(`SHOW CONSTRAINTS FROM store.customers`, origCustomers)
		db.CheckQueryResults(`SHOW CONSTRAINTS FROM store.orders`, origOrders)
		db.CheckQueryResults(`SHOW CONSTRAINTS FROM store.receipts`, origReceipts)

		// FK validation on customers from receipts is preserved.
		if _, err := db.DB.Exec(
			`UPDATE store.customers SET email = CONCAT(id::string, 'nope')`,
		); !testutils.IsError(err, "foreign key violation.* referenced in table \"receipts\"") {
			t.Fatal(err)
		}

		// FK validation on customers from orders is preserved.
		if _, err := db.DB.Exec(
			`UPDATE store.customers SET id = id * 1000`,
		); !testutils.IsError(err, "foreign key violation.* referenced in table \"orders\"") {
			t.Fatal(err)
		}

		// FK validation of customer id is preserved.
		if _, err := db.DB.Exec(
			`INSERT INTO store.orders VALUES (999, NULL, 999)`,
		); !testutils.IsError(err, "foreign key violation.* in customers@primary") {
			t.Fatal(err)
		}

		// FK validation of self-FK is preserved.
		if _, err := db.DB.Exec(
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		); !testutils.IsError(err, "foreign key violation: value .999. not found in receipts@primary") {
			t.Fatal(err)
		}

	})

	t.Run("restore customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		db := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		db.Exec(createStore)
		db.Exec(`RESTORE store.customers, store.orders FROM $1`, dir)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation on customers from orders is preserved.
		if _, err := db.DB.Exec(
			`UPDATE store.customers SET id = id*100`,
		); !testutils.IsError(err, "foreign key violation.* referenced in table \"orders\"") {
			t.Fatal(err)
		}

		// FK validation on customers from receipts is gone.
		db.Exec(`UPDATE store.customers SET email = id::string`)
	})

	t.Run("restore orders to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		db := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		db.Exec(createStore)

		// FK validation of self-FK is preserved.
		if _, err := db.DB.Exec(
			`RESTORE store.orders FROM $1`, dir,
		); !testutils.IsError(
			err, "cannot restore table \"orders\" without referenced table 53 \\(or \"skip_missing_foreign_keys\" option\\)",
		) {
			t.Fatal(err)
		}

		db.Exec(`RESTORE store.orders FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, dir)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation is gone.
		db.Exec(`INSERT INTO store.orders VALUES (999, NULL, 999)`)
		db.Exec(`DELETE FROM store.orders`)
	})

	t.Run("restore receipts to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		db := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		db.Exec(createStore)
		db.Exec(`RESTORE store.receipts FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, dir)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders and customer is gone.
		db.Exec(`INSERT INTO store.receipts VALUES (1, NULL, '987', 999)`)

		// FK validation of self-FK is preserved.
		if _, err := db.DB.Exec(
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		); !testutils.IsError(err, "foreign key violation: value .999. not found in receipts@primary") {
			t.Fatal(err)
		}
	})

	t.Run("restore receipts and customers to new cluster", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		db := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		db.Exec(createStore)
		db.Exec(`RESTORE store.receipts, store.customers FROM $1 WITH OPTIONS ('skip_missing_foreign_keys')`, dir)
		// Restore's Validate checks all the tables point to each other correctly.

		// FK validation of orders is gone.
		db.Exec(`INSERT INTO store.receipts VALUES (1, NULL, '0', 999)`)

		// FK validation of customer email is preserved.
		if _, err := db.DB.Exec(
			`INSERT INTO store.receipts VALUES (1, NULL, '999', 999)`,
		); !testutils.IsError(err, "foreign key violation.* in customers@customers_email_key") {
			t.Fatal(err)
		}

		// FK validation on customers from receipts is preserved.
		if _, err := db.DB.Exec(
			`DELETE FROM store.customers`,
		); !testutils.IsError(err, "foreign key violation.* referenced in table \"receipts\"") {
			t.Fatal(err)
		}

		// FK validation of self-FK is preserved.
		if _, err := db.DB.Exec(
			`INSERT INTO store.receipts VALUES (1, 999, NULL, NULL)`,
		); !testutils.IsError(err, "foreign key violation: value .999. not found in receipts@primary") {
			t.Fatal(err)
		}
	})
}

func checksumBankPayload(t *testing.T, sqlDB *sqlutils.SQLRunner) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	rows := sqlDB.Query(`SELECT id, balance, payload FROM bench.bank`)
	defer rows.Close()
	var id, balance int
	var payload []byte
	for rows.Next() {
		if err := rows.Scan(&id, &balance, &payload); err != nil {
			t.Fatal(err)
		}
		if _, err := crc.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return crc.Sum32()
}

func TestBackupRestoreIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 10
	const numBackups = 4
	windowSize := int(numAccounts / 3)

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()
	rng, _ := randutil.NewPseudoRand()

	var backupDirs []string
	var checksums []uint32
	{
		sqlDB.Exec(bankCreateTable)
		for backupNum := 0; backupNum < numBackups; backupNum++ {
			// In the following, windowSize is `w` and offset is `o`. The first
			// mutation creates accounts with id [w,3w). Every mutation after
			// that deletes everything less than o, leaves [o, o+w) unchanged,
			// mutates [o+w,o+2w), and inserts [o+2w,o+3w).
			offset := windowSize * backupNum
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `DELETE FROM bench.bank WHERE id < %d; `, offset)
			buf.WriteString(`UPSERT INTO bench.bank VALUES `)
			for j := 0; j < windowSize*2; j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				id := offset + windowSize + j
				payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
				fmt.Fprintf(&buf, `(%d, %d, '%s')`, id, backupNum, payload)
			}
			sqlDB.Exec(buf.String())

			checksums = append(checksums, checksumBankPayload(t, sqlDB))

			backupDir := filepath.Join(dir, strconv.Itoa(backupNum))
			var from string
			if backupNum > 0 {
				from = fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`))
			}
			sqlDB.Exec(fmt.Sprintf(`BACKUP TABLE bench.bank TO '%s' %s`, backupDir, from))

			backupDirs = append(backupDirs, fmt.Sprintf(`'%s'`, backupDir))
		}

		// Test a regression in RESTORE where the WriteBatch end key was not
		// being set correctly in Import: make an incremental backup such that
		// the greatest key in the diff is less than the previous backups.
		sqlDB.Exec(`INSERT INTO bench.bank VALUES (0, -1, 'final')`)
		checksums = append(checksums, checksumBankPayload(t, sqlDB))
		finalBackupDir := filepath.Join(dir, "final")
		sqlDB.Exec(fmt.Sprintf(`BACKUP TABLE bench.bank TO '%s' %s`,
			finalBackupDir, fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`)),
		))
		backupDirs = append(backupDirs, fmt.Sprintf(`'%s'`, finalBackupDir))
	}

	// Start a new cluster to restore into.
	{
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		sqlDBRestore.Exec(`CREATE DATABASE bench`)

		for i := len(backupDirs); i > 0; i-- {
			sqlDBRestore.Exec(`DROP TABLE IF EXISTS bench.bank`)
			from := strings.Join(backupDirs[:i], `,`)
			sqlDBRestore.Exec(fmt.Sprintf(`RESTORE bench.bank FROM %s`, from))

			testutils.SucceedsSoon(t, func() error {
				checksum := checksumBankPayload(t, sqlDBRestore)
				if checksum != checksums[i-1] {
					return errors.Errorf("checksum mismatch at index %d: got %d expected %d",
						i-1, checksum, checksums[i])
				}
				return nil
			})

		}
	}
}

// a bg worker is intended to write to the bank table concurrent with other
// operations (writes, backups, restores), mutating the payload on rows-maxID.
// it notified the `wake` channel (to allow ensuring bg activity has occurred)
// and can be informed when errors are allowable (e.g. when the bank table is
// unavailable between a drop and restore) via the atomic "bool" allowErrors.
func startBackgroundWrites(
	stopper *stop.Stopper, sqlDB *gosql.DB, maxID int, wake chan<- struct{}, allowErrors *int32,
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
			if atomic.LoadInt32(allowErrors) == 1 {
				return nil
			}
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

func TestBackupRestoreWithConcurrentWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const rows = 10
	const numBackgroundTasks = multiNode

	_, baseDir, tc, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, rows)
	defer cleanupFn()

	bgActivity := make(chan struct{})
	// allowErrors is used as an atomic bool to tell bg workers when to allow
	// errors, between dropping and restoring the table they are using.
	var allowErrors int32
	for task := 0; task < numBackgroundTasks; task++ {
		taskNum := task
		tc.Stopper().RunWorker(func() {
			conn := tc.Conns[taskNum%len(tc.Conns)]
			// Use different sql gateways to make sure leasing is right.
			if err := startBackgroundWrites(tc.Stopper(), conn, rows, bgActivity, &allowErrors); err != nil {
				t.Error(err)
			}
		})
	}

	// Use the bench.bank table as a key (id), value (balance) table with a
	// payload.The background tasks are mutating the table concurrently while we
	// backup and restore.
	<-bgActivity

	// Set, break, then reset the id=balance invariant -- while doing concurrent
	// writes -- to get multiple MVCC revisions as well as txn conflicts.
	sqlDB.Exec(`UPDATE bench.bank SET balance = id`)
	<-bgActivity
	sqlDB.Exec(`UPDATE bench.bank SET balance = -1`)
	<-bgActivity
	sqlDB.Exec(`UPDATE bench.bank SET balance = id`)
	<-bgActivity

	// Backup DB while concurrent writes continue.
	sqlDB.Exec(`BACKUP DATABASE bench TO $1`, baseDir)

	// Drop the table and restore from backup and check our invariant.
	atomic.StoreInt32(&allowErrors, 1)
	sqlDB.Exec(`DROP TABLE bench.bank`)
	sqlDB.Exec(`RESTORE bench.* FROM $1`, baseDir)
	atomic.StoreInt32(&allowErrors, 0)

	bad := sqlDB.QueryStr(`SELECT id, balance, payload FROM bench.bank WHERE id != balance`)
	for _, r := range bad {
		t.Errorf("bad row ID %s = bal %s (payload: %q)", r[0], r[1], r[2])
	}
}

func TestBackupAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 1000

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts)
	defer cleanupFn()

	var ts string
	var rowCount int64

	sqlDB.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&ts)
	sqlDB.Exec(`TRUNCATE bench.bank`)

	sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
	if rowCount != 0 {
		t.Fatalf("expected 0 rows but found %d", rowCount)
	}

	sqlDB.Exec(fmt.Sprintf(`BACKUP DATABASE bench TO '%s' AS OF SYSTEM TIME %s`, dir, ts))

	sqlDB.Exec(`DROP TABLE bench.bank`)

	sqlDB.Exec(`RESTORE bench.* FROM $1`, dir)

	sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
	if rowCount != numAccounts {
		t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
	}
}

func TestBackupRestoreChecksum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 1000
	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()

	sqlDB.Exec(`BACKUP DATABASE bench TO $1`, dir)

	var backupDesc BackupDescriptor
	{
		backupDescBytes, err := ioutil.ReadFile(filepath.Join(dir, BackupDescriptorName))
		if err != nil {
			t.Fatalf("%+v", err)
		}
		if err := backupDesc.Unmarshal(backupDescBytes); err != nil {
			t.Fatalf("%+v", err)
		}
	}

	// Corrupt one of the files in the backup.
	f, err := os.OpenFile(filepath.Join(dir, backupDesc.Files[1].Path), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer f.Close()
	// The last eight bytes of an SST file store a nonzero magic number. We can
	// blindly null out those bytes and guarantee that the checksum will change.
	if _, err := f.Seek(-8, io.SeekEnd); err != nil {
		t.Fatalf("%+v", err)
	}
	if _, err := f.Write(make([]byte, 8)); err != nil {
		t.Fatalf("%+v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("%+v", err)
	}

	sqlDB.Exec(`DROP TABLE bench.bank`)
	_, err = sqlDB.DB.Exec(`RESTORE bench.* FROM $1`, dir)
	if !testutils.IsError(err, "checksum mismatch") {
		t.Fatalf("expected 'checksum mismatch' error got: %+v", err)
	}
}

func TestTimestampMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numAccounts = 1

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()
	sqlDB.Exec(`CREATE TABLE bench.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(`INSERT INTO bench.t2 VALUES (1)`)

	fullBackup := filepath.Join(dir, "0")
	incrementalT1FromFull := filepath.Join(dir, "1")
	incrementalT2FromT1 := filepath.Join(dir, "2")
	incrementalT3FromT1OneTable := filepath.Join(dir, "3")

	sqlDB.Exec(`BACKUP DATABASE bench TO $1`,
		fullBackup)
	sqlDB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2`,
		incrementalT1FromFull, fullBackup)
	sqlDB.Exec(`BACKUP TABLE bench.bank TO $1 INCREMENTAL FROM $2`,
		incrementalT3FromT1OneTable, fullBackup)
	sqlDB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2, $3`,
		incrementalT2FromT1, fullBackup, incrementalT1FromFull)

	t.Run("Backup", func(t *testing.T) {
		// Missing the initial full backup.
		_, err := sqlDB.DB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2`,
			dir, incrementalT1FromFull)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}

		// Missing an intermediate incremental backup.
		_, err = sqlDB.DB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2, $3`,
			dir, fullBackup, incrementalT2FromT1)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}

		// Backups specified out of order.
		_, err = sqlDB.DB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2, $3`,
			dir, incrementalT1FromFull, fullBackup)
		if !testutils.IsError(err, "out of order") {
			t.Errorf("expected 'out of order' error got: %+v", err)
		}

		// Missing data for one table in the most recent backup.
		_, err = sqlDB.DB.Exec(`BACKUP DATABASE bench TO $1 INCREMENTAL FROM $2, $3`,
			dir, fullBackup, incrementalT3FromT1OneTable)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}
	})

	sqlDB.Exec(`DROP TABLE bench.bank`)
	sqlDB.Exec(`DROP TABLE bench.t2`)
	t.Run("Restore", func(t *testing.T) {
		// Missing the initial full backup.
		_, err := sqlDB.DB.Exec(`RESTORE bench.* FROM $1`,
			incrementalT1FromFull)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}

		// Missing an intermediate incremental backup.
		_, err = sqlDB.DB.Exec(`RESTORE bench.* FROM $1, $2`,
			fullBackup, incrementalT2FromT1)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}

		// Backups specified out of order.
		_, err = sqlDB.DB.Exec(`RESTORE bench.* FROM $1, $2`,
			incrementalT1FromFull, fullBackup)
		if !testutils.IsError(err, "out of order") {
			t.Errorf("expected 'out of order' error got: %+v", err)
		}

		// Missing data for one table in the most recent backup.
		_, err = sqlDB.DB.Exec(`RESTORE bench.* FROM $1, $2`,
			fullBackup, incrementalT3FromT1OneTable)
		if !testutils.IsError(err, "no backup covers time") {
			t.Errorf("expected 'no backup covers time' error got: %+v", err)
		}
	})
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

			// Verify that the splits exist.
			// Note that presplitRanges adds the row sentinel to make a valid table
			// key, but AdminSplit internally removes it (via EnsureSafeSplitKey). So
			// we expect splits that match the splitPoints exactly.
			for _, splitKey := range splitPoints {
				// Scan the meta range for splitKey.
				rk, err := keys.Addr(splitKey)
				if err != nil {
					t.Fatal(err)
				}

				startKey := keys.RangeMetaKey(rk)
				endKey := keys.Meta2Prefix.PrefixEnd()

				kvs, err := kvDB.Scan(context.Background(), startKey, endKey, 1)
				if err != nil {
					t.Fatal(err)
				}
				if len(kvs) != 1 {
					t.Fatalf("expected 1 KV, got %v", kvs)
				}
				desc := &roachpb.RangeDescriptor{}
				if err := kvs[0].ValueProto(desc); err != nil {
					t.Fatal(err)
				}
				if !desc.EndKey.Equal(rk) {
					t.Errorf(
						"missing split %s: range %s to %s",
						keys.PrettyPrint(splitKey),
						keys.PrettyPrint(desc.StartKey.AsRawKey()),
						keys.PrettyPrint(desc.EndKey.AsRawKey()),
					)
				}
			}
		})
	}
}

func TestBackupLevelDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, 0)
	defer cleanupFn()

	_ = sqlDB.Exec(`BACKUP DATABASE bench TO $1`, dir)

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

func TestRestoredPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 1
	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()

	rootOnly := sqlDB.QueryStr(`SHOW GRANTS ON bench.bank`)

	sqlDB.Exec(`CREATE USER someone`)
	sqlDB.Exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON bench.bank TO someone`)

	withGrants := sqlDB.QueryStr(`SHOW GRANTS ON bench.bank`)

	sqlDB.Exec(`BACKUP DATABASE bench TO $1`, dir)
	sqlDB.Exec(`DROP TABLE bench.bank`)

	t.Run("into fresh db", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		sqlDBRestore.Exec(`CREATE DATABASE bench`)
		sqlDBRestore.Exec(`RESTORE bench.bank FROM $1`, dir)
		sqlDBRestore.CheckQueryResults(`SHOW GRANTS ON bench.bank`, rootOnly)
	})

	t.Run("into db with added grants", func(t *testing.T) {
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
		defer tc.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tc.Conns[0])
		sqlDBRestore.Exec(`CREATE DATABASE bench`)
		sqlDBRestore.Exec(`CREATE USER someone`)
		sqlDBRestore.Exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE bench TO someone`)
		sqlDBRestore.Exec(`RESTORE bench.bank FROM $1`, dir)
		sqlDBRestore.CheckQueryResults(`SHOW GRANTS ON bench.bank`, withGrants)
	})
}

func TestRestoreInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 1
	_, dir, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()

	sqlDB.Exec(`BACKUP DATABASE bench TO $1`, dir)

	restoreStmt := fmt.Sprintf(`RESTORE bench.bank FROM '%s' WITH OPTIONS ('into_db'='bench2')`, dir)

	_, err := sqlDB.DB.Exec(restoreStmt)
	if !testutils.IsError(err, "a database named \"bench2\" needs to exist") {
		t.Fatal(err)
	}

	sqlDB.Exec(`CREATE DATABASE bench2`)
	sqlDB.Exec(restoreStmt)

	expected := sqlDB.QueryStr(`SELECT * FROM bench.bank`)
	sqlDB.CheckQueryResults(`SELECT * FROM bench2.bank`, expected)
}

func TestBackupRestorePermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if !storage.ProposerEvaluatedKVEnabled() {
		t.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}

	const numAccounts = 1
	_, dir, tc, sqlDB, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts)
	defer cleanupFn()

	sqlDB.Exec(`CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingAddr(), "TestBackupRestorePermissions-testuser", url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	backupStmt := fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dir)

	t.Run("root-only", func(t *testing.T) {
		if _, err := testuser.Exec(backupStmt); !testutils.IsError(
			err, "only root is allowed to BACKUP",
		) {
			t.Fatal(err)
		}
		if _, err := testuser.Exec(`RESTORE blah FROM 'blah'`); !testutils.IsError(
			err, "only root is allowed to RESTORE",
		) {
			t.Fatal(err)
		}
	})

	t.Run("privs-required", func(t *testing.T) {
		sqlDB.Exec(backupStmt)
		// Root doesn't have CREATE on `system` DB, so that should fail. Still need
		// a valid `dir` though, since descriptors are always loaded first.
		if _, err := sqlDB.DB.Exec(
			`RESTORE bench.bank FROM $1 WITH OPTIONS ('into_db'='system')`, dir,
		); !testutils.IsError(err, "user root does not have CREATE privilege") {
			t.Fatal(err)
		}
	})
}
