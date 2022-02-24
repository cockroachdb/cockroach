// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestPostProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	// We run the same input rows through various PostProcessSpecs.
	input := rowenc.EncDatumRows{
		{v[0], v[1], v[2]},
		{v[0], v[1], v[3]},
		{v[0], v[1], v[4]},
		{v[0], v[2], v[3]},
		{v[0], v[2], v[4]},
		{v[0], v[3], v[4]},
		{v[1], v[2], v[3]},
		{v[1], v[2], v[4]},
		{v[1], v[3], v[4]},
		{v[2], v[3], v[4]},
	}

	testCases := []struct {
		post          execinfrapb.PostProcessSpec
		outputTypes   []*types.T
		expNeededCols []int
		expected      string
	}{
		{
			post:          execinfrapb.PostProcessSpec{},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Projection.
		{
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			outputTypes:   types.TwoIntCols,
			expNeededCols: []int{0, 2},
			expected:      "[[0 2] [0 3] [0 4] [0 3] [0 4] [0 4] [1 3] [1 4] [1 4] [2 4]]",
		},

		// Rendering.
		{
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1},
			expected:      "[[0 1 1] [0 1 1] [0 1 1] [0 2 2] [0 2 2] [0 3 3] [1 2 3] [1 2 3] [1 3 4] [2 3 5]]",
		},

		// More complex rendering expressions.
		{
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{
					{Expr: "@1 - @2"},
					{Expr: "@1 + @2 * @3"},
					{Expr: "@1 >= 2"},
					{Expr: "((@1 = @2 - 1))"},
					{Expr: "@1 = @2 - 1 OR @1 = @3 - 2"},
					{Expr: "@1 = @2 - 1 AND @1 = @3 - 2"},
				},
			},
			outputTypes:   []*types.T{types.Int, types.Int, types.Bool, types.Bool, types.Bool, types.Bool},
			expNeededCols: []int{0, 1, 2},
			expected: "[" + strings.Join([]string{
				/* 0 1 2 */ "[-1 2 false true true true]",
				/* 0 1 3 */ "[-1 3 false true true false]",
				/* 0 1 4 */ "[-1 4 false true true false]",
				/* 0 2 3 */ "[-2 6 false false false false]",
				/* 0 2 4 */ "[-2 8 false false false false]",
				/* 0 3 4 */ "[-3 12 false false false false]",
				/* 1 2 3 */ "[-1 7 false true true true]",
				/* 1 2 4 */ "[-1 9 false true true false]",
				/* 1 3 4 */ "[-2 13 false false false false]",
				/* 2 3 4 */ "[-1 14 true true true true]",
			}, " ") + "]",
		},

		// Offset.
		{
			post:          execinfrapb.PostProcessSpec{Offset: 3},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Limit.
		{
			post:          execinfrapb.PostProcessSpec{Limit: 3},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Limit: 9},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Limit: 10},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Limit: 11},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Offset + limit.
		{
			post:          execinfrapb.PostProcessSpec{Offset: 3, Limit: 2},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Offset: 3, Limit: 6},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Offset: 3, Limit: 7},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:          execinfrapb.PostProcessSpec{Offset: 3, Limit: 8},
			outputTypes:   types.ThreeIntCols,
			expNeededCols: []int{0, 1, 2},
			expected:      "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(strconv.Itoa(tcIdx), func(t *testing.T) {
			inBuf := distsqlutils.NewRowBuffer(types.ThreeIntCols, input, distsqlutils.RowBufferArgs{})
			outBuf := &distsqlutils.RowBuffer{}

			var out execinfra.ProcOutputHelper
			semaCtx := tree.MakeSemaContext()
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			if err := out.Init(&tc.post, inBuf.OutputTypes(), &semaCtx, evalCtx); err != nil {
				t.Fatal(err)
			}

			// Verify NeededColumns().
			count := 0
			neededCols := out.NeededColumns()
			neededCols.ForEach(func(_ int) {
				count++
			})
			if count != len(tc.expNeededCols) {
				t.Fatalf("invalid neededCols length %d, expected %d", count, len(tc.expNeededCols))
			}
			for _, col := range tc.expNeededCols {
				if !neededCols.Contains(col) {
					t.Errorf("column %d not found in neededCols", col)
				}
			}
			// Run the rows through the helper.
			for i := range input {
				status, err := out.EmitRow(context.Background(), input[i], outBuf)
				if err != nil {
					t.Fatal(err)
				}
				if status != execinfra.NeedMoreRows {
					outBuf.ProducerDone()
					break
				}
			}
			var res rowenc.EncDatumRows
			for {
				row := outBuf.NextNoMeta(t)
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if str := res.String(tc.outputTypes); str != tc.expected {
				t.Errorf("expected output:\n    %s\ngot:\n    %s\n", tc.expected, str)
			}
		})
	}
}

func TestAggregatorSpecAggregationEquals(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Used for FilterColIdx *uint32.
	colIdx1 := uint32(0)
	colIdx2 := uint32(1)

	for i, tc := range []struct {
		a, b     execinfrapb.AggregatorSpec_Aggregation
		expected bool
	}{
		// Func tests.
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.Avg},
			expected: false,
		},

		// ColIdx tests.
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1, 2}},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1, 2}},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1}},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1, 3}},
			expected: false,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1, 2}},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, ColIdx: []uint32{1, 3}},
			expected: false,
		},

		// FilterColIdx tests.
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, FilterColIdx: &colIdx1},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, FilterColIdx: &colIdx1},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, FilterColIdx: &colIdx1},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			expected: false,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, FilterColIdx: &colIdx1},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, FilterColIdx: &colIdx2},
			expected: false,
		},

		// Distinct tests.
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: true},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: true},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: false},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: false},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: false},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			expected: true,
		},
		{
			a:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull, Distinct: true},
			b:        execinfrapb.AggregatorSpec_Aggregation{Func: execinfrapb.AnyNotNull},
			expected: false,
		},
	} {
		if actual := tc.a.Equals(tc.b); tc.expected != actual {
			t.Fatalf("case %d: incorrect result from %#v.Equals(%#v), expected %t, actual %t", i, tc.a, tc.b, tc.expected, actual)
		}

		// Reflexive case.
		if actual := tc.b.Equals(tc.a); tc.expected != actual {
			t.Fatalf("case %d: incorrect result from %#v.Equals(%#v), expected %t, actual %t", i, tc.b, tc.a, tc.expected, actual)
		}
	}
}

func TestProcessorBaseContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	runTest := func(t *testing.T, f func(noop *noopProcessor)) {
		evalCtx := tree.MakeTestingEvalContext(st)
		flowCtx := &execinfra.FlowCtx{
			Cfg:     &execinfra.ServerConfig{Settings: st},
			EvalCtx: &evalCtx,
		}
		defer flowCtx.EvalCtx.Stop(ctx)

		input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(10, 1))
		noop, err := newNoopProcessor(flowCtx, 0 /* processorID */, input, &execinfrapb.PostProcessSpec{}, &rowDisposer{})
		if err != nil {
			t.Fatal(err)
		}
		noop.Start(ctx)
		origCtx := noop.Ctx

		// The context should be valid after Start but before Next is called in case
		// ConsumerDone or ConsumerClosed are called without calling Next.
		if noop.Ctx == nil {
			t.Fatalf("ProcessorBase.ctx not initialized")
		}
		f(noop)
		// The context should be reset after ConsumerClosed is called so that any
		// subsequent logging calls will not operate on closed spans.
		if noop.Ctx != origCtx {
			t.Fatalf("ProcessorBase.ctx not reset on close")
		}
	}

	t.Run("next-close", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// The normal case: a call to Next followed by the processor being closed.
			noop.Next()
			noop.ConsumerClosed()
		})
	})

	t.Run("close-without-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// A processor can be closed without Next ever being called.
			noop.ConsumerClosed()
		})
	})

	t.Run("close-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// After the processor is closed, it can't be opened via a call to Next.
			noop.ConsumerClosed()
			noop.Next()
		})
	})

	t.Run("next-close-next", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// A spurious call to Next after the processor is closed.
			noop.Next()
			noop.ConsumerClosed()
			noop.Next()
		})
	})

	t.Run("next-close-close", func(t *testing.T) {
		runTest(t, func(noop *noopProcessor) {
			// Close should be idempotent.
			noop.Next()
			noop.ConsumerClosed()
			noop.ConsumerClosed()
		})
	})
}

func getPGXConnAndCleanupFunc(
	ctx context.Context, t *testing.T, servingSQLAddr string,
) (*pgx.Conn, func()) {
	t.Helper()
	pgURL, cleanup := sqlutils.PGUrl(t, servingSQLAddr, t.Name(), url.User(security.RootUser))
	pgURL.Path = "test"
	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	defaultConn, err := pgx.ConnectConfig(ctx, pgxConfig)
	require.NoError(t, err)
	_, err = defaultConn.Exec(ctx, "set distsql='always'")
	require.NoError(t, err)
	return defaultConn, cleanup
}

func populateRangeCacheAndDisableBuffering(t *testing.T, db *gosql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec("SELECT count(1) FROM " + tableName)
	require.NoError(t, err)
	// Disable results buffering - we want to ensure that the server doesn't do
	// any automatic retries, and also we use the client to know when to unblock
	// the read.
	_, err = db.Exec("SET CLUSTER SETTING sql.defaults.results_buffer.size = '0'")
	require.NoError(t, err)
}

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

	tc := serverutils.StartNewTestCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							TestingRequestFilter: func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
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
								if strings.Contains(key, "/1") && strings.Contains(endKey, "/6") {
									blockedRead.Lock()
									for !blockedRead.shouldUnblock {
										blockedRead.unblockCond.Wait()
									}
									blockedRead.Unlock()
									return roachpb.NewError(
										roachpb.NewReadWithinUncertaintyIntervalError(
											ba.Timestamp,           /* readTs */
											ba.Timestamp.Add(1, 0), /* existingTs */
											hlc.Timestamp{},        /* localUncertaintyLimit */
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
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

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

	populateRangeCacheAndDisableBuffering(t, origDB0, "t")
	defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.Server(0).ServingSQLAddr())
	defer cleanup()

	atomic.StoreInt64(&trapRead, 1)

	// Run with the vectorize off and on.
	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		// We're going to run the test twice in each vectorize configuration. Once
		// in "dummy" node, which just verifies that the test is not fooling itself
		// by increasing the limit from 5 to 6 and checking that we get the injected
		// error in that case.
		testutils.RunTrueAndFalse(t, "dummy", func(t *testing.T, dummy bool) {
			// Reset the blocking condition.
			blockedRead.Lock()
			blockedRead.shouldUnblock = false
			blockedRead.Unlock()
			// Force DistSQL to distribute the query. Otherwise, as of Nov 2018, it's hard
			// to convince it to distribute a query that uses an index.
			if _, err := defaultConn.Exec(ctx, "set distsql='always'"); err != nil {
				t.Fatal(err)
			}
			vectorizeMode := "off"
			if vectorize {
				vectorizeMode = "on"
			}

			if _, err := defaultConn.Exec(ctx, fmt.Sprintf("set vectorize='%s'", vectorizeMode)); err != nil {
				t.Fatal(err)
			}

			limit := 5
			if dummy {
				limit = 6
			}
			query := fmt.Sprintf(
				"select x from t where x <= 5 union all select x from t where x > 5 limit %d",
				limit)
			rows, err := defaultConn.Query(ctx, query)
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

// TestUncertaintyErrorIsReturned was added because the vectorized engine would
// previously buffer errors and return them once the flow started draining.
// This was fine for the majority of errors apart from
// ReadWithinUncertaintyIntervalError, which is swallowed if the error is not
// returned immediately during execution.
// This test aims to check that the error is returned using queries that use
// the set of components that handle/forward metadata.
// This test is a more generalized version of
// TestDrainingProcessorSwallowsUncertaintyError and specifically tests the case
// in which an UncertaintyError is expected.
func TestUncertaintyErrorIsReturned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	var (
		// trapRead is set atomically to return an error.
		trapRead        int64
		testClusterArgs = base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
		}
		rng, _ = randutil.NewTestRand()
	)

	filters := make([]struct {
		syncutil.Mutex
		enabled          bool
		tableIDsToFilter []int
	}, numNodes)

	var allNodeIdxs []int

	testClusterArgs.ServerArgsPerNode = make(map[int]base.TestServerArgs)
	for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
		allNodeIdxs = append(allNodeIdxs, nodeIdx)
		func(node int) {
			testClusterArgs.ServerArgsPerNode[node] = base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
							if atomic.LoadInt64(&trapRead) == 0 {
								return nil
							}
							filters[node].Lock()
							enabled := filters[node].enabled
							tableIDsToFilter := filters[node].tableIDsToFilter
							filters[node].Unlock()
							if !enabled {
								return nil
							}

							if req, ok := ba.GetArg(roachpb.Scan); !ok {
								return nil
							} else if tableIDsToFilter != nil {
								shouldReturnUncertaintyError := false
								for _, tableID := range tableIDsToFilter {
									if strings.Contains(req.(*roachpb.ScanRequest).Key.String(), fmt.Sprintf("/Table/%d", tableID)) {
										shouldReturnUncertaintyError = true
										break
									}
								}
								if !shouldReturnUncertaintyError {
									return nil
								}
							}

							return roachpb.NewError(
								roachpb.NewReadWithinUncertaintyIntervalError(
									ba.Timestamp,
									ba.Timestamp.Add(1, 0),
									hlc.Timestamp{},
									ba.Txn,
								),
							)
						},
					},
				},
				UseDatabase: "test",
			}
		}(nodeIdx)
	}

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, numNodes, testClusterArgs)
	defer tc.Stopper().Stop(ctx)

	// Create a 30-row table, split and scatter evenly across the numNodes nodes.
	dbConn := tc.ServerConn(0)
	sqlutils.CreateTable(t, dbConn, "t", "x INT, y INT, INDEX (y)", 30, sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn))
	// onerow is a table created to test #51458. The value of the only row in this
	// table is explicitly set to 2 so that it is routed by hash to a desired
	// destination.
	sqlutils.CreateTable(t, dbConn, "onerow", "x INT", 1, sqlutils.ToRowFn(func(_ int) tree.Datum { return tree.NewDInt(tree.DInt(2)) }))
	_, err := dbConn.Exec(fmt.Sprintf(`
	ALTER TABLE t SPLIT AT VALUES (10), (20);
	ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[%[1]d], 0), (ARRAY[%[2]d], 10), (ARRAY[%[3]d], 20);
	`,
		tc.Server(0).GetFirstStoreID(),
		tc.Server(1).GetFirstStoreID(),
		tc.Server(2).GetFirstStoreID(),
	))
	require.NoError(t, err)
	populateRangeCacheAndDisableBuffering(t, dbConn, "t")
	defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.Server(0).ServingSQLAddr())
	defer cleanup()

	// errorOriginSpec is a way for test cases to enable a request filter on the
	// node index provided for the given tableNames.
	type errorOriginSpec struct {
		nodeIdx    int
		tableNames []string
	}

	testCases := []struct {
		query           string
		expectedPlanURL string
		// overrideErrorOrigin if non-nil, defines special request filtering
		// behavior.
		// The default behavior is to enable uncertainty errors for a single random
		// node and for all scan requests.
		overrideErrorOrigin []errorOriginSpec
		// if non-empty, this test will be skipped.
		skip string
	}{
		{
			query:           "SELECT * FROM t AS t1 JOIN t AS t2 ON t1.x = t2.x",
			expectedPlanURL: "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJzElGFr2zwQx98_n0IcPLQdSm3JadoYCi6tR91lSZcUNihmuLGamDmWJ8mQEvLdh-1AlpDI8dxl73LR_e7-p_9ZC5A_Y7Bh5Pbc2ycUJa8cfRwOPqNn99tj78bro9M7b_Q0-tI7Q6ucD2WCQjcjpAh6GHj9VUDRoI8UOZ-ja6To-dxHX-_doVtW7XmfXHRyFwUTEczs_08AQ8JD1g9mTIL9DAQwUMBggY8hFXzMpOQiP1oUiV44B9vEECVppvK_fQxjLhjYC1CRihnY8BS8xGzIgpAJwwQMIVNBFBfllaO-pz_YG2C45XE2S6SN5hjl8SgN8qhlEBP8JQaeqVWLdeWXNzQN5HSzpkPAX_oYpAomDGyyxH8m1Tq6VLpX6rpOlnARMsHCjUp-Tlal7Jj3PpDTBx4lTBidTWkxe1WnDjm7FtFkWvwCDINM2cgh2KHYsbDT3pp2PYnVYJIdMvu8xVOjuz3yztbtjdbkcL9JXb8NYrYM-p7bWUNt-1-opXvVHmFBL__egtLD753Wvndqtt5xRWpIvTi6VLpX6hH24-o4D9gOEUMmU55IdtD7ZOZjsHDCymuRPBNj9ij4uGhThoOCK76wkElVntIy8JLyKBd4ONxpAnf1MKkhm9aDO03grh6m27D5O2zpZ7a0MDE3aHObbjcxWg9XGK2HK4y-aGK0Hq4wWg9XGN1pYvRlE6v0cIVVerjCqqsmVunhCqv0cIVV3VpW-cv_fgUAAP__x69Mdw==",
		},
		{
			query:           "SELECT * FROM t AS t1 INNER LOOKUP JOIN t AS t2 ON t1.x = t2.y",
			expectedPlanURL: "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJzElHFr2kAUwP_fp3g8GN3GWXMXtRoYWNqMpc0Sp44NSiiZuUlWzWW5C1PE7z6SCNVWr3ED9-fLez_fz3vvboXy1wwtHNmufTWGOPkh4MPQ_wR39reBe-l48ObaGY1Hn923sKl5VxUouByBouB4nj0E1_dvvwzgxne8TYaB74Gi5wt4D4qdLwP4-tEe2lUL17m14ew6DqdZOLdenyHBRETcC-dconWHFAkyJGhiQDDNxIRLKbIitSoLnWiBlkEwTtJcFZ8DghORcbRWqGI142jhOPw-40MeRjxrGkgw4iqMZ-XPq766Tx_4EgleiVk-T6QFCwJFPErDImo0qYHBmqDI1WMLqcIpR4uuSX2NGxEnGwvzucXyPo4WSNAV4iFP4aeIExCJBX267bYkkInfcXTQiP2lUefguTwTau0cFhL0c1V4kj4j_TbpmwflzINyj055IrKIZzzaEQrWe_Q90RBps_ekcH_r1k5rWn9h6LEL06RGo8nq7swLJlsTap1oZ-obXZx-Z1j9wbGjB8eMRs2pvaCxdUbtE02tvlH3_970PXJDLlORSF7rIhvFS8CjKa-eDSnybMIHmZiUbarQL7nyQ8SlqrK0CpykShWC2zDVwkwPMy1s7sD0KWzqtQ1965aWbuvhthbu6OHOv_zpCy3c1XfuauGeHu4dpR2sX_0JAAD__8XU8Oo=",
		},
		{
			// This test reproduces 51458 and should be enabled once that issue is
			// fixed.
			query:           "SELECT * FROM t JOIN onerow ON t.x = onerow.x",
			expectedPlanURL: "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJy8lPFr2kAUx3_fX3E8GG3H2eSirSNQyGgzms5pp8IGJYyredWwmMvuLswi_u8jiWDjbDSk8yd93n3efb_f590S1O8IbBi5Pfd6TML4SZDPw8FX8uD-uO998vrk9MYbjUffemdkvedDsUGTu4HXJyJGKf6QQZ_o8wW5WtfnC598v3WHbtGx531xyclNyKeSz-33J0AhFgH2-RwV2A_AgIIFFNrgU0ikmKBSQmZLy3yjFyzANimEcZLq7GefwkRIBHsJOtQRgg1j_hjhEHmA0jCBQoCah1HeXjv6Z_ILn4HCtYjSeaxssqAkq0cJz6qWwUzwVxREqtdHbDo_PpMZV7NyT4eBv_IpKM2nCDZb0VekbvqksZABSgxKnfyM3Ldlh99brmZ3IoxRGp2ytAif9KnDzq5kOJ3l34DCINU2cRh1LOq0t6xubLQb2NihsS9aIjG62353Ht0pHc0OHzarO2yDmS3Dest5s-PO--I_zds6PHSrduiW2XrDxGtIbZfbFg-UU3z8K_rFm9BIrvWq3CP8QS6P8CDsUDBElYhY4UH33cw8YDDFIhMlUjnBeykm-TFFOci5_H4FqHSxui68uFjKBB4Od5rA3WqYbcPmS9iqhq1K-GMJNrfhdpPAquE9gVXDewLrNAnsoonnaniP52p4j-fLGrKtenCnCdythru1RuWv3v0NAAD__2MQZAI=",
			overrideErrorOrigin: []errorOriginSpec{
				{
					nodeIdx:    0,
					tableNames: []string{"t"},
				},
			},
		},
	}

	// runQueryAndExhaust is a helper function to get an error that happens during
	// the execution of a query.
	runQueryAndExhaust := func(tx pgx.Tx, query string) error {
		res, err := tx.Query(ctx, query)
		if err != nil {
			return err
		}
		defer res.Close()
		for res.Next() {
		}
		return res.Err()
	}

	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		vectorizeOpt := "off"
		if vectorize {
			vectorizeOpt = "on"
		}
		for _, testCase := range testCases {
			t.Run(testCase.query, func(t *testing.T) {
				if testCase.skip != "" {
					skip.IgnoreLint(t, testCase.skip)
				}
				func() {
					_, err := defaultConn.Exec(ctx, fmt.Sprintf("set vectorize=%s", vectorizeOpt))
					require.NoError(t, err)
					func() {
						// Check distsql plan.
						rows, err := defaultConn.Query(
							ctx,
							fmt.Sprintf("SELECT info FROM [EXPLAIN (DISTSQL) %s] WHERE info LIKE 'Diagram:%%'", testCase.query),
						)
						require.NoError(t, err)
						defer rows.Close()
						rows.Next()
						var actualPlanURL string
						require.NoError(t, rows.Scan(&actualPlanURL))
						require.Equal(t, testCase.expectedPlanURL, actualPlanURL)
					}()

					errorOrigin := []errorOriginSpec{{nodeIdx: allNodeIdxs[rng.Intn(len(allNodeIdxs))]}}
					if testCase.overrideErrorOrigin != nil {
						errorOrigin = testCase.overrideErrorOrigin
					}
					for _, errorOriginSpec := range errorOrigin {
						nodeIdx := errorOriginSpec.nodeIdx
						filters[nodeIdx].Lock()
						filters[nodeIdx].enabled = true
						for _, tableName := range errorOriginSpec.tableNames {
							filters[nodeIdx].tableIDsToFilter = append(
								filters[nodeIdx].tableIDsToFilter,
								int(desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), keys.SystemSQLCodec, "test", tableName).GetID()),
							)
						}
						filters[nodeIdx].Unlock()
					}
					// Reset all filters for the next test case.
					defer func() {
						for i := range filters {
							filters[i].Lock()
							filters[i].enabled = false
							filters[i].tableIDsToFilter = nil
							filters[i].Unlock()
						}
					}()
					// Begin a transaction to issue multiple statements. The first dummy
					// statement is issued so that some results are returned and auto
					// retries are therefore disabled for the rest of the transaction.
					tx, err := defaultConn.Begin(ctx)
					defer func() {
						require.Error(t, tx.Commit(ctx))
					}()
					require.NoError(t, err)
					require.NoError(t, runQueryAndExhaust(tx, "SELECT count(*) FROM t"))

					// Trap all reads.
					atomic.StoreInt64(&trapRead, 1)
					defer atomic.StoreInt64(&trapRead, 0)

					err = runQueryAndExhaust(tx, testCase.query)
					require.True(t, testutils.IsError(err, "ReadWithinUncertaintyIntervalError"), "unexpected error: %v running query %s", err, testCase.query)
				}()
			})
		}
	})
}

// TestFlowConcurrenTxnUse tests that FlowBase correctly returns whether there
// will be concurrent txn usage. This test lives in this package for easier
// instantiation of processors.
func TestFlowConcurrentTxnUse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("TestSingleGoroutine", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		flow.SetProcessors([]execinfra.Processor{
			// samplerProcessor is used here and the other subtests because it does
			// not implement RowSource and so must be run in a separate goroutine (it
			// cannot be fused).
			&samplerProcessor{
				input: &tableReader{},
			},
		})
		require.False(t, flow.ConcurrentTxnUse(), "expected no concurrent txn use because there is only one goroutine")
	})
	t.Run("TestMultipleGoroutinesWithNoConcurrentTxnUse", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		// This is a common plan for stats collection. Neither processor implements
		// RowSource, so the sampleAggregator must be run in a separate goroutine
		// with a RowChannel connecting the two.
		flow.SetProcessors([]execinfra.Processor{
			&samplerProcessor{
				input: &tableReader{},
			},
			&sampleAggregator{
				input: &execinfra.RowChannel{},
			},
		})
		require.False(t, flow.ConcurrentTxnUse(), "expected no concurrent txn use because the tableReader should be the only txn user")
	})
	t.Run("TestMultipleGoroutinesWithConcurrentTxnUse", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		// This is a scenario that should never happen, but is useful for testing
		// (multiple concurrent samplerProcessors).
		flow.SetProcessors([]execinfra.Processor{
			&samplerProcessor{
				input: &tableReader{},
			},
			&samplerProcessor{
				input: &tableReader{},
			},
		})
		require.True(t, flow.ConcurrentTxnUse(), "expected concurrent txn use given that there are two tableReaders each in a separate goroutine")
	})
}

// testReaderProcessorDrain tests various scenarios in which a processor's
// consumer is closed. It makes sure that that the trace metadata and the leaf
// txn final state metadata are propagated, and it is the caller's
// responsibility to set up all the necessary infrastructure. This method is
// intended to be used by "reader" processors - those that read data from disk.
func testReaderProcessorDrain(
	ctx context.Context,
	t *testing.T,
	processorConstructor func(out execinfra.RowReceiver) (execinfra.Processor, error),
) {
	// ConsumerClosed verifies that when a processor's consumer is closed, the
	// processor finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		out := &distsqlutils.RowBuffer{}
		out.ConsumerClosed()
		p, err := processorConstructor(out)
		if err != nil {
			t.Fatal(err)
		}
		p.Run(ctx)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		out := &distsqlutils.RowBuffer{}
		out.ConsumerDone()
		p, err := processorConstructor(out)
		if err != nil {
			t.Fatal(err)
		}
		p.Run(ctx)
		var traceSeen, txnFinalStateSeen bool
		for {
			row, meta := out.Next()
			if row != nil {
				t.Fatalf("row was pushed unexpectedly")
			}
			if meta == nil {
				break
			}
			if meta.TraceData != nil {
				traceSeen = true
			}
			if meta.LeafTxnFinalState != nil {
				txnFinalStateSeen = true
			}
		}
		if !traceSeen {
			t.Fatal("missing tracing trailing metadata")
		}
		if !txnFinalStateSeen {
			t.Fatal("missing txn final state")
		}
	})
}
