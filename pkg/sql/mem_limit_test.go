// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMemoryLimit verifies that we're hitting memory budget errors when reading
// large blobs.
//
// The original goal of this test was to check that the aggregate memory usage
// across concurrent KV requests hits a memory budget error if --max-sql-memory
// pool is used up, but we have improved the memory accounting since then, and
// we are now likely to hit the error in the SQL layer, so this test doesn't
// require for the memory budget error come from pebble.
func TestMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We choose values for --max-sql-memory and the size of a single large blob
	// to be such that a single value could be read without hitting the memory
	// limit, but multiple blobs in a single BatchResponse do hit the memory
	// limit.
	//
	// Originally, when the Streamer API was not used, then SQL created a single
	// BatchRequest for both blobs without setting TargetBytes, so the pebble
	// was happy to get two blobs only to hit the memory limit. A concurrency of
	// 1 was sufficient.
	//
	// Now that the Streamer API is used, TargetBytes are set, so each blob gets
	// a separate BatchResponse. Thus, we need concurrency of 2 so that the
	// aggregate memory usage exceeds the memory limit. It's also likely that
	// the error is encountered in the SQL layer when performing accounting for
	// the read datums.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 5 << 20, /* 5MiB */
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE foo (id INT PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO foo SELECT 1, 10, repeat('a', 3000000)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO foo SELECT 2, 10, repeat('a', 3000000)")
	require.NoError(t, err)

	// We'll use a query that performs an index join to read the large blobs.
	query := "SELECT * FROM foo@foo_attribute_idx WHERE attribute=10 AND blob LIKE 'blah%'"

	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		vectorizeMode := "off"
		if vectorize {
			vectorizeMode = "on"
		}
		_, err = db.Exec("SET vectorize = " + vectorizeMode)
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			const numGoroutines = 2
			var wg sync.WaitGroup
			wg.Add(numGoroutines)
			errCh := make(chan error, numGoroutines)
			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					_, err := db.Exec(query)
					if err != nil {
						errCh <- err
					}
				}()
			}
			wg.Wait()
			close(errCh)

			memoryErrorFound := false
			for err := range errCh {
				if strings.Contains(err.Error(), "memory budget exceeded") {
					memoryErrorFound = true
				} else {
					return err
				}
			}
			if !memoryErrorFound {
				return errors.New("memory budget exceeded error wasn't hit")
			}
			return nil
		})
	})
}
