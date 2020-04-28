// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sampledataccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func TestToBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	outerDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	ctx := context.Background()
	args := base.TestServerArgs{
		ExternalIODir: outerDir,
		UseDatabase:   "data",
	}
	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	const payloadBytes, ranges = 100, 10
	chunkBytesSizes := []int64{
		0,                // 0 means default ~32MB
		3 * payloadBytes, // a number that will not evently divide the number of rows
	}

	for _, rows := range []int{1, 10} {
		for _, chunkBytes := range chunkBytesSizes {
			t.Run(fmt.Sprintf("rows=%d/chunk=%d", rows, chunkBytes), func(t *testing.T) {
				dir := fmt.Sprintf("%d-%d", rows, chunkBytes)
				data := bank.FromConfig(rows, rows, payloadBytes, ranges).Tables()[0]
				backup, err := toBackup(t, data, filepath.Join(outerDir, dir), chunkBytes)
				if err != nil {
					t.Fatalf("%+v", err)
				}

				t.Run("Restore", func(t *testing.T) {
					sqlDB := sqlutils.MakeSQLRunner(db)
					sqlDB.Exec(t, `DROP DATABASE IF EXISTS data CASCADE`)
					sqlDB.Exec(t, `CREATE DATABASE data`)
					sqlDB.Exec(t, `RESTORE data.* FROM $1`, `nodelocal://0/`+dir)

					var rowCount int
					sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s`, data.Name)).Scan(&rowCount)
					if rowCount != rows {
						t.Errorf("got %d rows expected %d", rowCount, rows)
					}
				})

				t.Run("NextKeyValues", func(t *testing.T) {
					for _, requestedKVs := range []int{2, 3} {
						newTableID := sqlbase.ID(keys.MaxReservedDescID + requestedKVs)
						newTablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(newTableID))

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
