// Copyright 2015 The Cockroach Authors.
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func setupBackupRestoreDB(t testing.TB, count int) (func(), *gosql.DB, *client.DB, string) {
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})

	dir, err := ioutil.TempDir("", "TestBackupRestore")
	if err != nil {
		s.Stopper().Stop()
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		s.Stopper().Stop()
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE d`); err != nil {
		s.Stopper().Stop()
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`CREATE TABLE d.foo (a INT PRIMARY KEY, b STRING, c DECIMAL)`); err != nil {
		s.Stopper().Stop()
		t.Fatal(err)
	}
	var insert bytes.Buffer
	insert.WriteString(`INSERT INTO d.foo VALUES `)
	for i := 0; i < count; i++ {
		if i != 0 {
			insert.WriteRune(',')
		}
		fmt.Fprintf(&insert, `(%d, '%d', %d)`, i, i, i)
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		s.Stopper().Stop()
		t.Fatal(err)
	}

	cleanupFn := func() {
		s.Stopper().Stop()
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	return cleanupFn, sqlDB, kvDB, dir
}

func TestBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	count := 1000
	cleanupFn, sqlDB, kvDB, dir := setupBackupRestoreDB(t, count)
	defer cleanupFn()

	if err := sql.Backup(context.Background(), *kvDB, dir); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`TRUNCATE d.foo`); err != nil {
		t.Fatal(err)
	}

	var rowCount int
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM d.foo`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}

	if rowCount != 0 {
		t.Fatalf("expected 0 rows but found %d", rowCount)
	}

	// TODO(dan): Shut down the cluster and restore into a fresh one.
	if err := sql.Restore(context.Background(), *kvDB, dir, "foo", true); err != nil {
		t.Fatal(err)
	}

	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM d.foo`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}

	if rowCount != count {
		t.Fatalf("expected %d rows but found %d", count, rowCount)
	}
}

func runBenchmarkBackup(b *testing.B, count int) {
	cleanupFn, _, kvDB, dirPrefix := setupBackupRestoreDB(b, count)
	defer cleanupFn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := filepath.Join(dirPrefix, strconv.Itoa(i))
		if err := sql.Backup(context.Background(), *kvDB, dir); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBackup_1(b *testing.B)    { runBenchmarkBackup(b, 1) }
func BenchmarkBackup_1000(b *testing.B) { runBenchmarkBackup(b, 1000) }

func runBenchmarkRestore(b *testing.B, count int) {
	cleanupFn, sqlDB, kvDB, dir := setupBackupRestoreDB(b, count)
	defer cleanupFn()

	if err := sql.Backup(context.Background(), *kvDB, dir); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if _, err := sqlDB.Exec(`TRUNCATE d.foo`); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := sql.Restore(context.Background(), *kvDB, dir, "foo", true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRestore_1(b *testing.B)    { runBenchmarkRestore(b, 1) }
func BenchmarkRestore_1000(b *testing.B) { runBenchmarkRestore(b, 1000) }
