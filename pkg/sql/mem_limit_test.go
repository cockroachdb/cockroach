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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMemoryLimit verifies that we're hitting memory budget errors when reading
// large blobs.
func TestMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderMetamorphic(t)

	// We choose values for --max-sql-memory and the size of a single large blob
	// to be such that a single value could be read without hitting the memory
	// limit, but multiple blobs in a single BatchResponse or across multiple
	// BatchResponses do hit the memory limit.
	//
	// When the Streamer API is not used, then SQL creates a single BatchRequest
	// for both blobs without setting TargetBytes, so the storage layer is happy
	// to get two blobs only to hit the memory limit. A concurrency of 1 is
	// sufficient.
	//
	// When the Streamer API is used, TargetBytes are set, so each blob gets a
	// separate BatchResponse. Thus, we need concurrency of 2 so that the
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

	for _, tc := range []struct {
		query       string
		concurrency int
	}{
		// Simple read without using the Streamer API - no need for concurrency.
		{
			query:       "SELECT * FROM foo WHERE blob LIKE 'blah%'",
			concurrency: 1,
		},
		// Perform an index join to read large blobs. It is done via the
		// Streamer API, so we need the concurrency to hit the memory error.
		{
			query:       "SELECT * FROM foo@foo_attribute_idx WHERE attribute=10 AND blob LIKE 'blah%'",
			concurrency: 2,
		},
	} {
		testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
			vectorizeMode := "off"
			if vectorize {
				vectorizeMode = "on"
			}
			_, err = db.Exec("SET vectorize = " + vectorizeMode)
			require.NoError(t, err)

			testutils.SucceedsSoon(t, func() error {
				var wg sync.WaitGroup
				wg.Add(tc.concurrency)
				errCh := make(chan error, tc.concurrency)
				for i := 0; i < tc.concurrency; i++ {
					go func() {
						defer wg.Done()
						_, err := db.Exec(tc.query)
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
						switch tc.concurrency {
						case 1:
							// Currently in this case we're not using the
							// Streamer API, so we expect to hit the error when
							// performing the memory accounting when evaluating
							// MVCC requests.
							if strings.Contains(err.Error(), "scan with start key") {
								memoryErrorFound = true
							}
						case 2:
							// Here we are using the Streamer API and are ok
							// with the error being encountered at any point.
							memoryErrorFound = true
						default:
							return errors.Newf("unexpected concurrency %d", tc.concurrency)
						}

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
}
