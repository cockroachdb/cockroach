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
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/tracing"
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
		return "", nil
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

func TestClusterBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// TODO(dan): Increase this clusterSize to 3. It works if the test timeout
	// is raised and with a util.SucceedsSoon wrapped around Backup, Restore,
	// and every sql Exec, but it seems like we should be fixing the underlying
	// issues.
	const clusterSize = 1
	const count = 1000

	tc := testcluster.StartTestCluster(t, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
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

	// TODO(dan): Shut down the cluster and restore into a fresh one.
	if err := sql.Restore(ctx, *kvDB, dir, "bank", true); err != nil {
		t.Fatal(err)
	}

	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount); err != nil {
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

	tc := testcluster.StartTestCluster(b, clusterSize, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)
	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	dir, cleanupFn := testingTempDir(b, 2)
	defer cleanupFn()

	// TODO(dan): Return the actual sizes from Backup.
	b.SetBytes(int64(count) * backupRestoreRowPayloadSize)
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
	})
	defer tc.Stopper().Stop()
	kvDB := tc.Server(0).KVClient().(*client.DB)

	ranges := setupBackupRestoreDB(b, ctx, tc, count, backupRestoreDefaultRanges)

	dir, cleanupFn := testingTempDir(b, 1)
	defer cleanupFn()

	if _, err := sql.Backup(ctx, *kvDB, dir); err != nil {
		b.Fatal(err)
	}

	setupReplicationAndLeases(b, tc, ranges, clusterSize)

	b.SetBytes(int64(count) * backupRestoreRowPayloadSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sql.Restore(ctx, *kvDB, dir, "bank", true); err != nil {
			b.Fatal(err)
		}
	}
}
