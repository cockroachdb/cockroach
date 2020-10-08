// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampsDuringImportInto ensures that the timestamp at which
// a table is taken offline is protected during an IMPORT INTO job to ensure
// that if data is imported into a range it can be reverted in the case of
// cancelation or failure.
func TestProtectedTimestampsDuringImportInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// A sketch of the test is as follows:
	//
	//  * Create a table foo to import into.
	//  * Set a 1 second gcttl for foo.
	//  * Start an import into with two HTTP backed CSV files where
	//    one server will serve a row and the other will block until
	//    it's signaled.
	//  * Manually enqueue the ranges for GC and ensure that at least one
	//    range ran the GC.
	//  * Force the IMPORT to fail.
	//  * Ensure that it was rolled back.
	//  * Ensure that we can GC after the job has finished.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")
	runner.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	rRand, _ := randutil.NewPseudoRand()
	writeGarbage := func(from, to int) {
		for i := from; i < to; i++ {
			runner.Exec(t, "UPSERT INTO foo VALUES ($1, $2)", i, randutil.RandBytes(rRand, 1<<10))
		}
	}
	writeGarbage(3, 10)
	rowsBeforeImportInto := runner.QueryStr(t, "SELECT * FROM foo")

	mkServer := func(method string, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == method {
				handler(w, r)
			}
		}))
	}
	srv1 := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	})
	defer srv1.Close()
	// Let's start an import into this table of ours.
	allowResponse := make(chan struct{})
	srv2 := mkServer("GET", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		w.WriteHeader(500)
	})
	defer srv2.Close()

	importErrCh := make(chan error, 1)
	go func() {
		_, err := conn.Exec(`IMPORT INTO foo (k, v) CSV DATA ($1, $2)`,
			srv1.URL, srv2.URL)
		importErrCh <- err
	}()

	var jobID string
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT job_id FROM [SHOW JOBS] ORDER BY created DESC LIMIT 1")
		return row.Scan(&jobID)
	})

	time.Sleep(3 * time.Second) // Wait for the data to definitely be expired and GC to run.
	gcTable := func(skipShouldQueue bool) (traceStr string) {
		rows := runner.Query(t, "SELECT start_key"+
			" FROM crdb_internal.ranges_no_leases"+
			" WHERE table_name = $1"+
			" AND database_name = current_database()"+
			" ORDER BY start_key ASC", "foo")
		var traceBuf strings.Builder
		for rows.Next() {
			var startKey roachpb.Key
			require.NoError(t, rows.Scan(&startKey))
			r := tc.LookupRangeOrFatal(t, startKey)
			l, _, err := tc.FindRangeLease(r, nil)
			require.NoError(t, err)
			lhServer := tc.Server(int(l.Replica.NodeID) - 1)
			s, repl := getFirstStoreReplica(t, lhServer, startKey)
			trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, skipShouldQueue)
			require.NoError(t, err)
			fmt.Fprintf(&traceBuf, "%s\n", trace.String())
		}
		require.NoError(t, rows.Err())
		return traceBuf.String()
	}

	// We should have refused to GC over the timestamp which we needed to protect.
	gcTable(true /* skipShouldQueue */)

	// Unblock the blocked import request.
	close(allowResponse)

	require.Regexp(t, "error response from server: 500 Internal Server Error", <-importErrCh)

	runner.CheckQueryResultsRetry(t, "SELECT * FROM foo", rowsBeforeImportInto)

	// Write some fresh garbage.

	// Wait for the ranges to learn about the removed record and ensure that we
	// can GC from the range soon.
	// This regex matches when all float priorities other than 0.00000. It does
	// this by matching either a float >= 1 (e.g. 1230.012) or a float < 1 (e.g.
	// 0.000123).
	matchNonZero := "[1-9]\\d*\\.\\d+|0\\.\\d*[1-9]\\d*"
	nonZeroProgressRE := regexp.MustCompile(fmt.Sprintf("priority=(%s)", matchNonZero))
	testutils.SucceedsSoon(t, func() error {
		writeGarbage(3, 10)
		if trace := gcTable(false /* skipShouldQueue */); !nonZeroProgressRE.MatchString(trace) {
			return fmt.Errorf("expected %v in trace: %v", nonZeroProgressRE, trace)
		}
		return nil
	})
}

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}
