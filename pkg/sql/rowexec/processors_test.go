// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"bytes"
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPostProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
		post        execinfrapb.PostProcessSpec
		outputTypes []*types.T
		expected    string
	}{
		{
			post:        execinfrapb.PostProcessSpec{},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Projection.
		{
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			outputTypes: types.TwoIntCols,
			expected:    "[[0 2] [0 3] [0 4] [0 3] [0 4] [0 4] [1 3] [1 4] [1 4] [2 4]]",
		},

		// Rendering.
		{
			post: execinfrapb.PostProcessSpec{
				RenderExprs: []execinfrapb.Expression{{Expr: "@1"}, {Expr: "@2"}, {Expr: "@1 + @2"}},
			},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 1] [0 1 1] [0 1 1] [0 2 2] [0 2 2] [0 3 3] [1 2 3] [1 2 3] [1 3 4] [2 3 5]]",
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
			outputTypes: []*types.T{types.Int, types.Int, types.Bool, types.Bool, types.Bool, types.Bool},
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
			post:        execinfrapb.PostProcessSpec{Offset: 3},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Limit.
		{
			post:        execinfrapb.PostProcessSpec{Limit: 3},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 2] [0 1 3] [0 1 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Limit: 9},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Limit: 10},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Limit: 11},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 2] [0 1 3] [0 1 4] [0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},

		// Offset + limit.
		{
			post:        execinfrapb.PostProcessSpec{Offset: 3, Limit: 2},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 2 3] [0 2 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Offset: 3, Limit: 6},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Offset: 3, Limit: 7},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
		{
			post:        execinfrapb.PostProcessSpec{Offset: 3, Limit: 8},
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 2 3] [0 2 4] [0 3 4] [1 2 3] [1 2 4] [1 3 4] [2 3 4]]",
		},
	}

	ctx := context.Background()
	flowCtx := &execinfra.FlowCtx{}
	for tcIdx, tc := range testCases {
		t.Run(strconv.Itoa(tcIdx), func(t *testing.T) {
			inBuf := distsqlutils.NewRowBuffer(types.ThreeIntCols, input, distsqlutils.RowBufferArgs{})
			outBuf := &distsqlutils.RowBuffer{}

			var out execinfra.ProcOutputHelper
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(ctx)
			if err := out.Init(ctx, &tc.post, inBuf.OutputTypes(), &semaCtx, evalCtx, flowCtx); err != nil {
				t.Fatal(err)
			}

			// Run the rows through the helper.
			for i := range input {
				status, err := out.EmitRow(ctx, input[i], outBuf)
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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

	// Use a custom context to distinguish it from the background one.
	ctx := context.WithValue(context.Background(), contextKey{}, struct{}{})
	st := cluster.MakeTestingClusterSettings()

	runTest := func(t *testing.T, f func(noop *noopProcessor)) {
		evalCtx := eval.MakeTestingEvalContext(st)
		flowCtx := &execinfra.FlowCtx{
			Cfg:     &execinfra.ServerConfig{Settings: st},
			EvalCtx: &evalCtx,
			Mon:     evalCtx.TestingMon,
		}
		defer flowCtx.EvalCtx.Stop(ctx)

		input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(10, 1))
		noop, err := newNoopProcessor(ctx, flowCtx, 0 /* processorID */, input, &execinfrapb.PostProcessSpec{})
		if err != nil {
			t.Fatal(err)
		}
		// Before Start we should get the background context.
		if noop.Ctx() != context.Background() {
			t.Fatalf("ProcessorBase.Ctx() didn't return the background context before Start")
		}
		noop.Start(ctx)
		origCtx := noop.Ctx()

		// The context should be valid after Start but before Next is called in case
		// ConsumerDone or ConsumerClosed are called without calling Next.
		if noop.Ctx() == context.Background() {
			t.Fatalf("ProcessorBase.ctx not initialized")
		}
		f(noop)
		// The context should be reset after ConsumerClosed is called so that any
		// subsequent logging calls will not operate on closed spans.
		if noop.Ctx() != origCtx {
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
	pgURL, cleanup := pgurlutils.PGUrl(t, servingSQLAddr, t.Name(), url.User(username.RootUser))
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
	defer log.Scope(t).Close(t)

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
		// We will want to block exactly one read to the first node.
		blockOneRead atomic.Bool
		blockedRead  struct {
			syncutil.Mutex
			unblockCond   *sync.Cond
			shouldUnblock bool
		}
		endKeyToBlock string
	)

	blockedRead.unblockCond = sync.NewCond(&blockedRead.Mutex)

	tc := serverutils.StartCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
								if !blockOneRead.Load() {
									return nil
								}
								// We're going to trap a read for the rows [1,5].
								req, ok := ba.GetArg(kvpb.Scan)
								if !ok {
									return nil
								}
								endKey := req.(*kvpb.ScanRequest).EndKey.String()
								if !strings.Contains(endKey, endKeyToBlock) {
									// Either a request for a different table or
									// for a different part of the target table.
									return nil
								}
								// Since we're blocking this read, the caller
								// must set it again if it wants to block
								// another read.
								blockOneRead.Store(false)
								blockedRead.Lock()
								for !blockedRead.shouldUnblock {
									blockedRead.unblockCond.Wait()
								}
								blockedRead.Unlock()
								return kvpb.NewError(
									kvpb.NewReadWithinUncertaintyIntervalError(
										ba.Timestamp,           /* readTs */
										hlc.ClockTimestamp{},   /* localUncertaintyLimit */
										ba.Txn,                 /* txn */
										ba.Timestamp.Add(1, 0), /* valueTS */
										hlc.ClockTimestamp{} /* localTS */))
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

	row := origDB0.QueryRow("SELECT 't'::regclass::oid")
	var tableID int
	require.NoError(t, row.Scan(&tableID))
	// Request that we want to block is of the form
	//   Scan /Table/105/1{-/6}
	// so it'll have the end key like
	//   /Table/105/1/6.
	endKeyToBlock = fmt.Sprintf("/Table/%d/1/6", tableID)

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
	defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.Server(0).AdvSQLAddr())
	defer cleanup()

	// Run with the vectorize off and on.
	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		// We're going to run the test twice in each vectorize configuration. Once
		// in "dummy" node, which just verifies that the test is not fooling itself
		// by increasing the limit from 5 to 6 and checking that we get the injected
		// error in that case.
		testutils.RunTrueAndFalse(t, "dummy", func(t *testing.T, dummy bool) {
			blockOneRead.Store(true)
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
		enabled   bool
		keyPrefix roachpb.Key
	}, numNodes)

	var allNodeIdxs []int

	testClusterArgs.ServerArgsPerNode = make(map[int]base.TestServerArgs)
	for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
		allNodeIdxs = append(allNodeIdxs, nodeIdx)
		func(node int) {
			testClusterArgs.ServerArgsPerNode[node] = base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
							if atomic.LoadInt64(&trapRead) == 0 {
								return nil
							}
							filters[node].Lock()
							enabled := filters[node].enabled
							keyPrefix := filters[node].keyPrefix
							filters[node].Unlock()
							if !enabled {
								return nil
							}

							req, ok := ba.GetArg(kvpb.Scan)
							if !ok {
								return nil
							}
							if !bytes.HasPrefix(req.(*kvpb.ScanRequest).Key, keyPrefix) {
								return nil
							}
							return kvpb.NewError(
								kvpb.NewReadWithinUncertaintyIntervalError(
									ba.Timestamp,
									hlc.ClockTimestamp{},
									ba.Txn,
									ba.Timestamp.Add(1, 0),
									hlc.ClockTimestamp{},
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
	tc := serverutils.StartCluster(t, numNodes, testClusterArgs)
	defer tc.Stopper().Stop(ctx)

	// Create a 30-row table, split and scatter evenly across the numNodes nodes.
	dbConn := tc.ServerConn(0)
	sqlutils.CreateTable(t, dbConn, "t", "x INT, y INT, INDEX (y)", 30, sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn))
	tableID := desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), keys.SystemSQLCodec, "test", "t").GetID()
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
	defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.Server(0).AdvSQLAddr())
	defer cleanup()

	testCases := []struct {
		query           string
		expectedPlanURL string
		// overrideErrorOrigin if non-nil, defines special request filtering
		// behavior.
		// The default behavior is to enable uncertainty errors for a single random
		// node.
		overrideErrorOrigin []int
	}{
		{
			query:           "SELECT * FROM t AS t1 JOIN t AS t2 ON t1.x = t2.x",
			expectedPlanURL: "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJzEld9u2jAUh-_3FNaRpraTKbHD30iVqApT6Sh0gLRJFapcYiBqiJltVKqKd58CdC0J8UjWslwgTJwvv2N_5jyD-uWDA71Gq3HRR14wEuhrt3ONbhs_b1rnzTY6rjd7_d73Fka9y_ObxgnaTP2ynqfReQ9pgq46zfZmQFGnjTQ5XaAzpOnpYoB-XDa6jTW81fzWQEd1j40lmzqfjwBDIFzeZlOuwLkFAhgoYLBhgGEmxZArJWR463k1sekuwLEweMFsrsOfBxiGQnJwnkF72ufgQJ_d-7zLmctl3gIMLtfM81d4XdN3swf-BBguhD-fBspBC4zCcW_GwlEuTywYLDGIud684pV8_4QmTE22mTUCg-UAg9JszMEhb3I36-BYS5wtun3w6DQS3U6M_sqdB0K6XHJ3izwIn_zblB31XzI1uRJewGW-tB3V5yN9XCMnZ9IbT1bfAENnrh1UI7hGcc3GtUKk-tfK7EhlpX-obEfstsiJWb4aXYKdUQqRKNWtKGR_P0haP_LEyuXp-9pNsqYv_I_0UcELiekPIHj5IwUvb1VG998XmnpfqJV7V6Vo1ujFg0eP-lRMjH4Anyof6VMlTRfrcjUTgeJ7_R9akTflSFgnd8d8vW5KzOWQ30gxXM1dDzsr0Modlyu9vkvXg2bwcktpydn0TxPen1RKJpXSkarJJEKjKJKiPPoWZacjlZJJ1XSkajKJFKMoGkVZb1G2YaUqUZRtRBFri2UZTShkdYqkIxmcKqcjmZyKmVDM6lQhHcngFImtuRllkip2_EqZpYqpXs5qQuwgm0kGE2Kim0kmE2L7V8lqQuwgm0kmE2JnxowymRA7NNWsJlAr7DgjXzzeeS44YG2u3I6PlwvCB9hYhW2vNxGPK27_aRY2rRHzFcdwzR54nWsup17gKe0NwdFyzpfLT78DAAD__5SzAcM=",
		},
		{
			query:           "SELECT * FROM t AS t1 INNER LOOKUP JOIN t AS t2 ON t1.x = t2.y",
			expectedPlanURL: "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJzElV1v2jwUgO_fX2Ed6VW3yZTY4TPSJKqSqWlpwgjTJlWoyojLvIY4sx0VVPHfpwBdIW0iwkWbCyTHh8ePfY5PHkH9icAC3x7Y52PE4zuBvoy8a3Rj_xgOzhwXfeg7_tj_OsDIvzgb2h_RNvTTJk6jMx9pghzXtUdo4HlX34bo0nPc7QxFnos0OV2gz0jT0-UEfb-wR_ZmpYFzZaOTPg9mMphb_58AhliEzA3mTIF1AwQwUMBgwgRDIsWUKSVkNvW4DnTCBVgGBh4nqc5eTzBMhWRgPYLmOmJgwTj4GbERC0Im6wZgCJkOeLTG656-Te7ZEjCciyidx8pCC4yysZ8E2ahWJwZMVhhEqp-XUDqYMbDIjpPTB8tY4cO1LgWPt1bmS6vlLQ8XgGEgxH2aoN-Cx0jEFuqRXdclRlI88LDQkOYMzSMNW4Xn9kKwsXeYgMFLdeaNexT3mrhnFsqaOdlWoeyzYxoLGTLJwj3ByeqV7biiJpJ6Nxf4ukojp9LdUyGHFxypWnB1YtTq9PCaI1XMdjLaeLOaaxxp2H6PmmvvydLDE00rJ5oatYOzTKto7Zxh882y3DzSsPMeWe5UaYMjphIRK3ZQ4zByK9VI1opYOGObvqVEKqdsKMV0HbsZemvQ-kXIlN7Mks3AiZ-mlJYsmP_7zuySSCmJFpPMPImWksw9EtkltfIks3x3RoXtNUpRzWISyZOapaRWMamRJ7WOPah2ntQuJXWKnWie1CkldYtJzType-zuOlm530Xi4ZaHYIGxfWqv_Dw9kP0hmKnszvm_xMMaO14m2Y25CyLFMFwH96zPNJNzHnOl-RQsLVO2Wv33NwAA___OinSA",
		},
		{
			// Reproduction of not propagating errors to all outputs of the
			// hash router (#51458).
			query:               "SELECT * FROM t JOIN onerow ON t.x = onerow.x",
			expectedPlanURL:     "Diagram: https://cockroachdb.github.io/distsqlplan/decode.html#eJy8lW9P4koUxt_fTzE5yY16M0inFPA2McEIN-JFcIFkNzHEjPQAjaXDzkwjxvDdN21xpYV2Kf7pC2WY098855knhxdQPz2wYdDqtC6HxPUngvzX792Qu9aP285Fu0uOm-3BcPCtQ8ng6uK2dULWpf_EdZpc99pdInyU4on0ukSfLsn5en26HJHvV61-KwZ32v-3yFHT5VPJ5_bfR0DBFw52-RwV2HfAgIIJFCoworCQYoxKCRluvUSFbWcJtkHB9ReBDr8eURgLiWC_gHa1h2DDkD942EfuoCwbQMFBzV0vwuuGvl884jNQuBReMPeVTZaUhOvBgoerUpkZMFpREIFeH_FGfngmM65mSWaDwWg1oqA0nyLYbEN3uwm2saIZ0t-4gS-kgxKdBHkUvvmnkh39X3E1uxauj7JsJaV6ONHHDXZyLt3pLPoEFHqBtkmD0YZJG5VU629tVVJtWe9oa4fmriiJRbme7n-nFCslpZ6QwvYPBysajjIzSmXzY_PBMtV_QT6qn5aPaqItc_9LMQtfimmUPvRGzEOlV5LHxAOwEf_bbmJj5rxLvpmSX8mU_wWBqn1aoGqZA2eHoj6qhfAV7jVPjNRJJRY2ic4UY9OUCOQYb6UYR7XxsheBovA4qHS8u160_dctpSXy-e-fgf1JVjbJKkaqZ5PO0iSWJhmbJDObxMw0ysxFnSVQRq5RlUMtZ8VIOZZXi5FyLP83TbIOtrySRlUPNWrr8vJJOUbVipFyjGJbOagVaM_cRG0ZlU-yskn1YqR6NolthbN-cBCscFxNPPF07zpgg7F-Sjv-vD4QvsCnKpyZg5l4irjD50U48SbcU0jhhj9iEzXKueu7SrtjsLUMcLX661cAAAD__3vS9aQ=",
			overrideErrorOrigin: []int{0},
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
				func() {
					_, err := defaultConn.Exec(ctx, fmt.Sprintf("set vectorize=%s", vectorizeOpt))
					require.NoError(t, err)
					// We allow for the DistSQL plan to be different for some
					// time in case the range cache wasn't populated as we
					// expected (we've seen this under race in #108250).
					testutils.SucceedsSoon(t, func() error {
						rows, err := defaultConn.Query(
							ctx,
							fmt.Sprintf("SELECT info FROM [EXPLAIN (DISTSQL, SHAPE) %s] WHERE info LIKE 'Diagram:%%'", testCase.query),
						)
						require.NoError(t, err)
						defer rows.Close()
						rows.Next()
						var actualPlanURL string
						require.NoError(t, rows.Scan(&actualPlanURL))
						if testCase.expectedPlanURL != actualPlanURL {
							return errors.Newf(
								"DistSQL plans didn't match:\nexpected:%s\nactual: %s",
								testCase.expectedPlanURL, actualPlanURL,
							)
						}
						return nil
					})

					errorOrigin := []int{allNodeIdxs[rng.Intn(len(allNodeIdxs))]}
					if testCase.overrideErrorOrigin != nil {
						errorOrigin = testCase.overrideErrorOrigin
					}
					for _, nodeIdx := range errorOrigin {
						filters[nodeIdx].Lock()
						filters[nodeIdx].enabled = true
						filters[nodeIdx].keyPrefix = keys.SystemSQLCodec.TablePrefix(uint32(tableID))
						filters[nodeIdx].Unlock()
					}
					// Reset all filters for the next test case.
					defer func() {
						for i := range filters {
							filters[i].Lock()
							filters[i].enabled = false
							filters[i].keyPrefix = nil
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
	defer log.Scope(t).Close(t)

	t.Run("TestSingleGoroutine", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		require.NoError(t, flow.SetProcessorsAndOutputs([]execinfra.Processor{
			// samplerProcessor is used here and the other subtests because it does
			// not implement RowSource and so must be run in a separate goroutine (it
			// cannot be fused).
			&samplerProcessor{
				input: &tableReader{},
			},
		}, []execinfra.RowReceiver{nil}))
		require.False(t, flow.ConcurrentTxnUse(), "expected no concurrent txn use because there is only one goroutine")
	})
	t.Run("TestMultipleGoroutinesWithNoConcurrentTxnUse", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		// This is a common plan for stats collection. Neither processor implements
		// RowSource, so the sampleAggregator must be run in a separate goroutine
		// with a RowChannel connecting the two.
		require.NoError(t, flow.SetProcessorsAndOutputs([]execinfra.Processor{
			&samplerProcessor{
				input: &tableReader{},
			},
			&sampleAggregator{
				input: &execinfra.RowChannel{},
			},
		}, []execinfra.RowReceiver{nil, nil}))
		require.False(t, flow.ConcurrentTxnUse(), "expected no concurrent txn use because the tableReader should be the only txn user")
	})
	t.Run("TestMultipleGoroutinesWithConcurrentTxnUse", func(t *testing.T) {
		flow := &flowinfra.FlowBase{}
		// This is a scenario that should never happen, but is useful for testing
		// (multiple concurrent samplerProcessors).
		require.NoError(t, flow.SetProcessorsAndOutputs([]execinfra.Processor{
			&samplerProcessor{
				input: &tableReader{},
			},
			&samplerProcessor{
				input: &tableReader{},
			},
		}, []execinfra.RowReceiver{nil, nil}))
		require.True(t, flow.ConcurrentTxnUse(), "expected concurrent txn use given that there are two tableReaders each in a separate goroutine")
	})
}

// testReaderProcessorDrain tests various scenarios in which a processor's
// consumer is closed. It makes sure that that the trace metadata and the leaf
// txn final state metadata are propagated, and it is the caller's
// responsibility to set up all the necessary infrastructure. This method is
// intended to be used by "reader" processors - those that read data from disk.
func testReaderProcessorDrain(
	ctx context.Context, t *testing.T, processorConstructor func() (execinfra.Processor, error),
) {
	// ConsumerClosed verifies that when a processor's consumer is closed, the
	// processor finishes gracefully.
	t.Run("ConsumerClosed", func(t *testing.T) {
		out := &distsqlutils.RowBuffer{}
		out.ConsumerClosed()
		p, err := processorConstructor()
		if err != nil {
			t.Fatal(err)
		}
		p.Run(ctx, out)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		out := &distsqlutils.RowBuffer{}
		out.ConsumerDone()
		p, err := processorConstructor()
		if err != nil {
			t.Fatal(err)
		}
		p.Run(ctx, out)
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

type contextKey struct{}
