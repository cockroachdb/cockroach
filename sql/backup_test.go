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
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

const (
	backupRestoreDefaultRanges = 10
	backupRestoreApproxRowSize = 100
)

func testingTempDir(t testing.TB, depth int) (string, func()) {
	pc, _, _, ok := runtime.Caller(depth)
	if !ok {
		t.Fatalf("invalid depth %d", depth)
		return "", nil
	}
	s := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	dir, err := ioutil.TempDir("", s[len(s)-1])
	if err != nil {
		t.Fatal(err)
		return "", nil
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	return dir, cleanup
}

func setupBackupRestoreDB(
	t testing.TB,
	ctx context.Context,
	tc *testcluster.TestCluster,
	count int,
	minRanges int,
) []*roachpb.RangeDescriptor {
	sqlDB := tc.Conns[0]
	kvDB := tc.Server(0).KVClient().(*client.DB)
	if _, err := sqlDB.Exec(`CREATE DATABASE bench`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`CREATE TABLE bench.foo (a INT PRIMARY KEY, b STRING, c DECIMAL, FAMILY(a, b, c))`); err != nil {
		t.Fatal(err)
	}
	var insert bytes.Buffer
	insert.WriteString(`INSERT INTO bench.foo VALUES `)
	for i := 0; i < count; i++ {
		if i%1000 == 0 {
			if i != 0 {
				if _, err := sqlDB.Exec(insert.String()); err != nil {
					t.Fatal(err)
				}
				insert.Reset()
				insert.WriteString(`INSERT INTO bench.foo VALUES `)
			}
		} else {
			insert.WriteRune(',')
		}
		fmt.Fprintf(&insert, `(%d, '%s', %d)`, i, strings.Repeat("i", backupRestoreApproxRowSize), i)
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		t.Fatal(err)
	}

	if minRanges > count {
		t.Fatalf("cannot create %d ranges out of %d rows", minRanges, count)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "bench", "foo")
	tablePrimaryKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	tableColMap := map[sqlbase.ColumnID]int{tableDesc.Columns[0].ID: 0}
	row := make([]parser.Datum, 1)
	ranges := make([]*roachpb.RangeDescriptor, minRanges)
	for i, incr := 1, count/minRanges; i < minRanges; i++ {
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

	var wg sync.WaitGroup
	for _, r := range ranges {
		wg.Add(1)
		// TODO(dan): Run these in parallel to reduce setup time a bit. Doing
		// this leads to too many reservations at once and the requests get
		// throttled. This can be fixed by setting COCKROACH_MAX_RESERVATIONS
		// in the environment. Figure out if it's worth it.
		func(r *roachpb.RangeDescriptor) {
			defer wg.Done()
			if _, err := tc.AddReplicas(r.StartKey.AsRawKey(), targets...); err != nil {
				t.Fatal(err)
			}
			if err := tc.TransferRangeLease(r, tc.Target(int(r.RangeID)%clusterSize)); err != nil {
				t.Fatal(err)
			}
		}(r)
	}
	wg.Wait()
}

func TestClusterBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	const clusterSize = 3
	const count = 1000

	tc := testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs:      base.TestServerArgs{},
	})
	defer tc.Stopper().Stop()
	sqlDB := tc.Conns[0]
	kvDB := tc.Server(0).KVClient().(*client.DB)

	_ = setupBackupRestoreDB(t, ctx, tc, count, backupRestoreDefaultRanges)
	dir, cleanupFn := testingTempDir(t, 2)
	defer cleanupFn()

	if _, err := sql.Backup(context.Background(), *kvDB, dir); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`TRUNCATE bench.foo`); err != nil {
		t.Fatal(err)
	}

	var rowCount int
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.foo`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}

	if rowCount != 0 {
		t.Fatalf("expected 0 rows but found %d", rowCount)
	}

	// TODO(dan): Shut down the cluster and restore into a fresh one.
	if err := sql.Restore(ctx, *kvDB, dir, "foo", true); err != nil {
		t.Fatal(err)
	}

	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.foo`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}

	if rowCount != count {
		t.Fatalf("expected %d rows but found %d", count, rowCount)
	}
}

// TODO(dan): count=10000 has some issues replicating. Investigate.
func BenchmarkClusterBackup_10(b *testing.B)   { runBenchmarkClusterBackup(b, 3, 10) }
func BenchmarkClusterBackup_100(b *testing.B)  { runBenchmarkClusterBackup(b, 3, 100) }
func BenchmarkClusterBackup_1000(b *testing.B) { runBenchmarkClusterBackup(b, 3, 1000) }
func runBenchmarkClusterBackup(b *testing.B, clusterSize int, count int) {
	defer tracing.Disable()()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(b, clusterSize,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      base.TestServerArgs{},
		})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)
	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	dir, cleanupFn := testingTempDir(b, 2)
	defer cleanupFn()

	// TODO(dan): Return the actual sizes from Backup.
	b.SetBytes(int64(count) * backupRestoreApproxRowSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sql.Backup(ctx, *kvDB, dir); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterRestore_10(b *testing.B)    { runBenchmarkClusterRestore(b, 3, 10) }
func BenchmarkClusterRestore_100(b *testing.B)   { runBenchmarkClusterRestore(b, 3, 100) }
func BenchmarkClusterRestore_1000(b *testing.B)  { runBenchmarkClusterRestore(b, 3, 1000) }
func BenchmarkClusterRestore_10000(b *testing.B) { runBenchmarkClusterRestore(b, 3, 10000) }
func runBenchmarkClusterRestore(b *testing.B, clusterSize int, count int) {
	defer tracing.Disable()()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(b, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      base.TestServerArgs{},
	})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)

	dir, cleanupFn := testingTempDir(b, 2)
	defer cleanupFn()

	if _, err := sql.Backup(ctx, *kvDB, dir); err != nil {
		b.Fatal(err)
	}

	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	b.SetBytes(int64(count) * backupRestoreApproxRowSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sql.Restore(ctx, *kvDB, dir, "foo", true); err != nil {
			b.Fatal(err)
		}
	}
}
