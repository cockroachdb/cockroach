// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sampledataccl

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestInsertBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		rows      int
		batchSize int
	}{
		{10, 1},
		{10, 9},
		{10, 10},
		{10, 100},
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/batch=%d", test.rows, test.batchSize), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(t, db)

			data := Bank(test.rows, 0, bankConfigDefault)
			sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS data`)
			sqlDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, data.Name()))
			sqlDB.Exec(fmt.Sprintf(`CREATE TABLE %s %s`, data.Name(), data.Schema()))

			if err := InsertBatched(sqlDB.DB, data, test.batchSize); err != nil {
				t.Fatalf("%+v", err)
			}

			var rowCount int
			sqlDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, data.Name())).Scan(&rowCount)
			if rowCount != test.rows {
				t.Errorf("got %d rows expected %d", rowCount, test.rows)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		rows           int
		ranges         int
		expectedRanges int
	}{
		{10, 0, 1}, // We always have at least one range.
		{10, 1, 1},
		{10, 9, 9},
		{10, 10, 10},
		{10, 100, 10}, // Don't make more ranges than rows.
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/ranges=%d", test.rows, test.ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(t, db)

			data := Bank(test.rows, bankConfigDefault, test.ranges)
			sqlDB.Exec(`CREATE DATABASE IF NOT EXISTS data`)
			sqlDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, data.Name()))
			sqlDB.Exec(fmt.Sprintf(`CREATE TABLE %s %s`, data.Name(), data.Schema()))

			if err := Split(sqlDB.DB, data); err != nil {
				t.Fatalf("%+v", err)
			}

			var rangeCount int
			sqlDB.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*) FROM [SHOW TESTING_RANGES FROM TABLE %s]`, data.Name()),
			).Scan(&rangeCount)
			if rangeCount != test.expectedRanges {
				t.Errorf("got %d ranges expected %d", rangeCount, test.expectedRanges)
			}
		})
	}
}

func TestToBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const payloadBytes = 100
	chunkBytesSizes := []int64{
		0,                // 0 means default ~32MB
		3 * payloadBytes, // a number that will not evently divide the number of rows
	}

	for _, rows := range []int{1, 10} {
		for _, chunkBytes := range chunkBytesSizes {
			t.Run(fmt.Sprintf("rows=%d/chunk=%d", rows, chunkBytes), func(t *testing.T) {
				dir, dirCleanupFn := testutils.TempDir(t)
				defer dirCleanupFn()

				data := Bank(rows, payloadBytes, bankConfigDefault)
				backup, err := toBackup(t, data, dir, chunkBytes)
				if err != nil {
					t.Fatalf("%+v", err)
				}

				t.Run("Restore", func(t *testing.T) {
					sqlDB := sqlutils.MakeSQLRunner(t, db)
					sqlDB.Exec(`DROP DATABASE IF EXISTS data`)
					sqlDB.Exec(`CREATE DATABASE data`)
					sqlDB.Exec(`RESTORE data.* FROM $1`, `nodelocal://`+dir)

					var rowCount int
					sqlDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, data.Name())).Scan(&rowCount)
					if rowCount != rows {
						t.Errorf("got %d rows expected %d", rowCount, rows)
					}
				})

				t.Run("NextKeyValues", func(t *testing.T) {
					for _, requestedKVs := range []int{2, 3} {
						newTableID := sqlbase.ID(keys.MaxReservedDescID + requestedKVs)
						newTablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(newTableID)))

						keys := make(map[string]struct{}, rows)
						for {
							kvs, span, err := backup.NextKeyValues(requestedKVs, newTableID)
							if err != nil && err != io.EOF {
								t.Fatalf("%+v", err)
							}
							if len(kvs) < requestedKVs && err != io.EOF {
								t.Errorf("got %d kvs requested at least %d", len(kvs), requestedKVs)
							}
							for k := range keys {
								key := roachpb.Key(k)
								if key.Compare(span.Key) >= 0 && key.Compare(span.EndKey) < 0 {
									t.Errorf("previous key %s overlaps this span %s", key, span)
								}
							}
							for _, kv := range kvs {
								key := kv.Key.Key
								if key.Compare(span.Key) < 0 || key.Compare(span.EndKey) >= 0 {
									t.Errorf("key %s is not in %s", kv.Key, span)
								}
								if _, ok := keys[string(key)]; ok {
									t.Errorf("key %s was output twice", key)
								}
								keys[string(key)] = struct{}{}
								if !bytes.HasPrefix(key, newTablePrefix) {
									t.Errorf("key %s is not for table %d", key, newTableID)
								}
							}
							if err == io.EOF {
								break
							}
						}

						if len(keys) != rows {
							t.Errorf("got %d kvs expected %d", len(keys), rows)
						}
						// Don't reconstruct the backup between runs so we can
						// test the reset.
						backup.ResetKeyValueIteration()
					}
				})
			})
		}
	}
}
