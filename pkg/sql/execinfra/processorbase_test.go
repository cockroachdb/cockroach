// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// Test that processors swallow ReadWithinUncertaintyIntervalErrors once they
// started draining. The code that this test is checking is in ProcessorBase.
// The test is written using high-level interfaces since what's truly
// interesting to test is the integration between DistSQL and KV.
func TestDrainingProcessorSwallowsUncertaintyError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We're going to test by running a query that selects rows 1..10 with limit
	// 5. Out of these, rows 1..5 are on node 1, 6..10 on node 2. We're going to
	// block the read on node 1 until the client gets the 5 rows from node 2. Then
	// we're going to inject an uncertainty error in the blocked read. The point
	// of the test is to check that the error is swallowed, because the processor
	// on the gateway is already draining.
	// We need to construct this scenario with multiple nodes since you can't have
	// uncertainty errors if all the data is on the gateway. Then, we'll use a
	// UNION query to force DistSQL to plan multiple TableReaders (otherwise it
	// plans just one for LIMIT queries). The point of the test is to force one
	// extra batch to be read without its rows actually being needed. This is not
	// entirely easy to cause given the current implementation details.

	var (
		// trapRead is set, atomically, once the test wants to block a read on the
		// first node.
		trapRead    int64
		blockedRead struct {
			syncutil.Mutex
			unblockCond   *sync.Cond
			shouldUnblock bool
		}
	)

	blockedRead.unblockCond = sync.NewCond(&blockedRead.Mutex)

	tc := serverutils.StartTestCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Knobs: base.TestingKnobs{
						Store: &storage.StoreTestingKnobs{
							TestingRequestFilter: func(ba roachpb.BatchRequest) *roachpb.Error {
								if atomic.LoadInt64(&trapRead) == 0 {
									return nil
								}
								// We're going to trap a read for the rows [1,5].
								req, ok := ba.GetArg(roachpb.Scan)
								if !ok {
									return nil
								}
								key := req.(*roachpb.ScanRequest).Key.String()
								endKey := req.(*roachpb.ScanRequest).EndKey.String()
								if strings.Contains(key, "/1") && strings.Contains(endKey, "5/") {
									blockedRead.Lock()
									for !blockedRead.shouldUnblock {
										blockedRead.unblockCond.Wait()
									}
									blockedRead.Unlock()
									return roachpb.NewError(
										roachpb.NewReadWithinUncertaintyIntervalError(
											ba.Timestamp,           /* readTs */
											ba.Timestamp.Add(1, 0), /* existingTs */
											ba.Txn))
								}
								return nil
							},
						},
					},
					UseDatabase: "test",
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	origDB0 := tc.ServerConn(0)
	sqlutils.CreateTable(t, origDB0, "t",
		"x INT PRIMARY KEY",
		10, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// Split the table and move half of the rows to the 2nd node. We'll block the
	// read on the first node, and so the rows we're going to be expecting are the
	// ones from the second node.
	_, err := origDB0.Exec(fmt.Sprintf(`
	ALTER TABLE "t" SPLIT AT VALUES (6);
	ALTER TABLE "t" EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 6);
	`,
		tc.Server(0).GetFirstStoreID(),
		tc.Server(1).GetFirstStoreID()))
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that the range cache is populated.
	if _, err = origDB0.Exec(`SELECT count(1) FROM t`); err != nil {
		t.Fatal(err)
	}

	// Disable results buffering - we want to ensure that the server doesn't do
	// any automatic retries, and also we use the client to know when to unblock
	// the read.
	if _, err := origDB0.Exec(
		`SET CLUSTER SETTING sql.defaults.results_buffer.size = '0'`,
	); err != nil {
		t.Fatal(err)
	}

	pgURL, cleanup := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	pgURL.Path = `test`
	pgxConfig, err := pgx.ParseConnectionString(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(pgxConfig)
	if err != nil {
		t.Fatal(err)
	}

	atomic.StoreInt64(&trapRead, 1)

	// Run with the default vectorize mode and the explicit "on" mode.
	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		// We're going to run the test twice in each vectorize configuration. Once
		// in "dummy" node, which just verifies that the test is not fooling itself
		// by increasing the limit from 5 to 6 and checking that we get the injected
		// error in that case.
		testutils.RunTrueAndFalse(t, "dummy", func(t *testing.T, dummy bool) {
			// Force DistSQL to distribute the query. Otherwise, as of Nov 2018, it's hard
			// to convince it to distribute a query that uses an index.
			if _, err := conn.Exec("set distsql='always'"); err != nil {
				t.Fatal(err)
			}
			if vectorize {
				if _, err := conn.Exec("set vectorize='experimental_on'"); err != nil {
					t.Fatal(err)
				}
			}
			limit := 5
			if dummy {
				limit = 6
			}
			query := fmt.Sprintf(
				"select x from t where x <= 5 union all select x from t where x > 5 limit %d",
				limit)
			rows, err := conn.Query(query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			i := 6
			for rows.Next() {
				var n int
				if err := rows.Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != i {
					t.Fatalf("expected row: %d but got: %d", i, n)
				}
				i++
				// After we've gotten all the rows from the second node, let the first node
				// return an uncertainty error.
				if n == 10 {
					blockedRead.Lock()
					// Set shouldUnblock to true to have any reads that would block return
					// an uncertainty error. Signal the cond to wake up any reads that have
					// already been blocked.
					blockedRead.shouldUnblock = true
					blockedRead.unblockCond.Signal()
					blockedRead.Unlock()
				}
			}
			err = rows.Err()
			if !dummy {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, "ReadWithinUncertaintyIntervalError") {
					t.Fatalf("expected injected error, got: %v", err)
				}
			}
		})
	})
}

// Test that processors executing concurrently with a processor encountering a
// retriable error don't execute at the updated timestamp. Executing at the
// updated timestamp would cause them to miss seeing preceeding writes from the
// same transaction.
// The race whose handling we test is the following:
// Processors p1 and p2 execute concurrently on the same node (the gateway).
// p1 encouters a retriable error which causes the RootTxn to bump the epoch
// from 1 to 2. Were we not careful to avoid it, p2 could then start a read at
// epoch 2, and that'd be bad. See #25329.
// The race is handled by making sure that some processors use the RootTxn and
// others executing concurrently with them use the LeafTxn.
func TestConcurrentProcessorsReadEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	joinerBlock := make(chan struct{})
	maxSeq := 10
	// trapRead is set, atomically, once the test wants to block a read on the
	// first node.
	injectErr := true
	trapRead := true
	const errKey = "_err"
	const dummyKey = "_dummy"
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &tree.EvalContextTestingKnobs{
				CallbackGenerators: map[string]*tree.CallbackValueGenerator{
					"dummy_read": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, txn *client.Txn) (int, error) {
							if txn.Type() != client.LeafTxn {
								return -1, errors.Errorf("expected LeafTxn for dummy_read")
							}
							log.Infof(ctx, "!!! doing dummy ")
							_, err := txn.Get(ctx, dummyKey)
							return -1, err
						}),
					"block": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, txn *client.Txn) (int, error) {
							if prev == 0 {
								<-joinerBlock
							}
							if prev == maxSeq {
								// End of generator.
								return -1, nil
							}
							return prev + 1, nil
						}),
					"inject_err": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, txn *client.Txn) (int, error) {
							if txn.Type() != client.RootTxn {
								return -1, errors.Errorf("expected RootTxn for inject_err")
							}
							if !injectErr {
								return -1, nil
							}
							// !!! time.Sleep(time.Second) // !!!
							log.Infof(context.TODO(), "!!! doing err read in txn: %d", txn.Type())
							_, err := txn.Get(ctx, errKey)
							if err != nil {
								return -1, errors.Errorf("expected injected error")
							}
							injectErr = false
							return -1, nil
							//
							//txn := ctx.Txn.GetTxnCoordMeta(evalCtx.Ctx()).Txn
							//
							//readTS := txn.Timestamp
							//if readTS.Less(txn.RefreshedTimestamp) {
							//	readTS = txn.RefreshedTimestamp
							//}
							//return 0, roachpb.NewReadWithinUncertaintyIntervalError(
							//	readTS,
							//	readTS.Add(1, 0), /* existingTs */
							//	&txn)
						}),
				},
			},
			Store: &storage.StoreTestingKnobs{
				TestingRequestFilter: func(ba roachpb.BatchRequest) *roachpb.Error {
					req, ok := ba.GetArg(roachpb.Get)
					if !ok {
						return nil
					}
					key := req.(*roachpb.GetRequest).Key.String()
					if strings.Contains(key, dummyKey) {
						log.Infof(context.TODO(), "!!! trapped dummy read at ts: %s", ba.Timestamp)
					}
					if !trapRead {
						return nil
					}
					if strings.Contains(key, errKey) {
						log.Infof(context.TODO(), "!!! trapped read")
						trapRead = false
						return roachpb.NewError(
							roachpb.NewReadWithinUncertaintyIntervalError(
								ba.Timestamp,           /* readTs */
								ba.Timestamp.Add(1, 0), /* existingTs */
								ba.Txn))
					}
					return nil
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// !!! _, err := db.Exec("select * from testing_callback('inject_err')")
	_, err := db.Exec(`
		select l.x from 
			(select * from generate_series(10, 15) as l(x) union 
		   select * from testing_callback('dummy_read')) 
		  as l(x) 
		inner join 
		  (select * from testing_callback('inject_err')) 
		  as r(x) 
		on l.x = r.x;
		`)
	log.Infof(ctx, "!!! err: %v", err)

	//	select l.x from (select * from generate_series(10,15) as l(x) union values(100))
	//	as l(x) inner join (select * from (values (1))) r(x) on l.x = r.x;

	//	select l.x from (select * from generate_series(10,15) as l(x) union values(100)) as l(x) inner join (select * from testing_callback('inject_err')) r(x) on l.x = r.x;
}
