// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test that we don't attempt to create flows in an aborted transaction.
// Instead, a retryable error is created on the gateway. The point is to
// simulate a race where the heartbeat loop finds out that the txn is aborted
// just before a plan starts execution and check that we don't create flows in
// an aborted txn (which isn't allowed). Note that, once running, each flow can
// discover on its own that its txn is aborted - that's handled separately. But
// flows can't start in a txn that's already known to be aborted.
//
// We test this by manually aborting a txn and then attempting to execute a plan
// in it. We're careful to not use the transaction for anything but running the
// plan; planning will be performed outside of the transaction.
func TestDistSQLRunningInAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.ExecContext(
		ctx, "create database test; create table test.t(a int)"); err != nil {
		t.Fatal(err)
	}
	key := roachpb.Key("a")

	// Plan a statement.
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, s.NodeID()),
		security.RootUserName(),
		&MemoryMetrics{},
		&execCfg,
		sessiondatapb.SessionData{},
	)
	defer cleanup()
	p := internalPlanner.(*planner)
	query := "select * from test.t"
	stmt, err := parser.ParseOne(query)
	if err != nil {
		t.Fatal(err)
	}

	push := func(ctx context.Context, key roachpb.Key) error {
		// Conflicting transaction that pushes another transaction.
		conflictTxn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		// We need to explicitly set a high priority for the push to happen.
		if err := conflictTxn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
			return err
		}
		// Push through a Put, as opposed to a Get, so that the pushee gets aborted.
		if err := conflictTxn.Put(ctx, key, "pusher was here"); err != nil {
			return err
		}
		return conflictTxn.CommitOrCleanup(ctx)
	}

	// Make a db with a short heartbeat interval, so that the aborted txn finds
	// out quickly.
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			// Short heartbeat interval.
			HeartbeatInterval: time.Millisecond,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		s.DistSenderI().(*kvcoord.DistSender),
	)
	shortDB := kv.NewDB(ambient, tsf, s.Clock(), s.Stopper())

	iter := 0
	// We'll trace to make sure the test isn't fooling itself.
	tr := s.Tracer().(*tracing.Tracer)
	runningCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, tr, "test")
	defer cancel()
	err = shortDB.Txn(runningCtx, func(ctx context.Context, txn *kv.Txn) error {
		iter++
		if iter == 1 {
			// On the first iteration, abort the txn.

			if err := txn.Put(ctx, key, "val"); err != nil {
				t.Fatal(err)
			}

			if err := push(ctx, key); err != nil {
				t.Fatal(err)
			}

			// Now wait until the heartbeat loop notices that the transaction is aborted.
			testutils.SucceedsSoon(t, func() error {
				if txn.Sender().(*kvcoord.TxnCoordSender).IsTracking() {
					return fmt.Errorf("txn heartbeat loop running")
				}
				return nil
			})
		}

		// Create and run a DistSQL plan.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			stmt.AST.StatementReturnType(),
			execCfg.RangeDescriptorCache,
			txn,
			execCfg.Clock,
			p.ExtendedEvalContext().Tracing,
			execCfg.ContentionRegistry,
			nil, /* testingPushCallback */
		)

		// We need to re-plan every time, since close() below makes
		// the plan unusable across retries.
		p.stmt = makeStatement(stmt, ClusterWideID{})
		if err := p.makeOptimizerPlan(ctx); err != nil {
			t.Fatal(err)
		}
		defer p.curPlan.close(ctx)

		evalCtx := p.ExtendedEvalContext()
		// We need distribute = true so that executing the plan involves marshaling
		// the root txn meta to leaf txns. Local flows can start in aborted txns
		// because they just use the root txn.
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p, nil /* txn */, true /* distribute */)
		planCtx.stmtType = recv.stmtType

		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, txn, p.curPlan.main, recv,
		)()
		return rw.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if iter != 2 {
		t.Fatalf("expected two iterations, but txn took %d to succeed", iter)
	}
	if tracing.FindMsgInRecording(getRec(), clientRejectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", clientRejectedMsg)
	}
}

// Test that the DistSQLReceiver overwrites previous errors as "better" errors
// come along.
func TestDistSQLReceiverErrorRanking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test goes through the trouble of creating a server because it wants to
	// create a txn. It creates the txn because it wants to test an interaction
	// between the DistSQLReceiver and the TxnCoordSender: the DistSQLReceiver
	// will feed retriable errors to the TxnCoordSender which will change those
	// errors to TransactionRetryWithProtoRefreshError.
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	txn := kv.NewTxn(ctx, db, s.NodeID())

	// We're going to use a rowResultWriter to which only errors will be passed.
	rw := newCallbackResultWriter(nil /* fn */)
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		tree.Rows, /* StatementReturnType */
		nil,       /* rangeCache */
		txn,
		nil, /* clockUpdater */
		&SessionTracing{},
		nil, /* contentionRegistry */
		nil, /* testingPushCallback */
	)

	retryErr := roachpb.NewErrorWithTxn(
		roachpb.NewTransactionRetryError(
			roachpb.RETRY_SERIALIZABLE, "test err"),
		txn.TestingCloneTxn()).GoError()

	abortErr := roachpb.NewErrorWithTxn(
		roachpb.NewTransactionAbortedError(
			roachpb.ABORT_REASON_ABORTED_RECORD_FOUND),
		txn.TestingCloneTxn()).GoError()

	errs := []struct {
		err    error
		expErr string
	}{
		{
			// Initial error, retriable.
			err:    retryErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionRetryError",
		},
		{
			// A non-retriable error overwrites a retriable one.
			err:    fmt.Errorf("err1"),
			expErr: "err1",
		},
		{
			// Another non-retriable error doesn't overwrite the previous one.
			err:    fmt.Errorf("err2"),
			expErr: "err1",
		},
		{
			// A TransactionAbortedError overwrites anything.
			err:    abortErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionAbortedError",
		},
		{
			// A non-aborted retriable error does not overried the
			// TransactionAbortedError.
			err:    retryErr,
			expErr: "TransactionRetryWithProtoRefreshError: TransactionAbortedError",
		},
	}

	for i, tc := range errs {
		recv.Push(nil, /* row */
			&execinfrapb.ProducerMetadata{
				Err: tc.err,
			})
		if !testutils.IsError(rw.Err(), tc.expErr) {
			t.Fatalf("%d: expected %s, got %s", i, tc.expErr, rw.Err())
		}
	}
}

// TestDistSQLReceiverReportsContention verifies that the distsql receiver
// reports contention events via an observable metric if they occur. This test
// additionally verifies that the metric stays at zero if there is no
// contention.
func TestDistSQLReceiverReportsContention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testutils.RunTrueAndFalse(t, "contention", func(t *testing.T, contention bool) {
		// TODO(yuzefovich): add an onContentionEventCb() to
		// DistSQLRunTestingKnobs and use it here to accumulate contention
		// events.
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		sqlutils.CreateTable(
			t, db, "test", "x INT PRIMARY KEY", 1, sqlutils.ToRowFn(sqlutils.RowIdxFn),
		)

		if contention {
			// Begin a contending transaction.
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, conn.Close())
			}()
			_, err = conn.ExecContext(ctx, "BEGIN; UPDATE test.test SET x = 10 WHERE x = 1;")
			require.NoError(t, err)
		}

		metrics := s.DistSQLServer().(*distsql.ServerImpl).Metrics
		metrics.ContendedQueriesCount.Clear()
		contentionRegistry := s.ExecutorConfig().(ExecutorConfig).ContentionRegistry
		otherConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, otherConn.Close())
		}()
		// TODO(yuzefovich): turning the tracing ON won't be necessary once
		// always-on tracing is enabled.
		_, err = otherConn.ExecContext(ctx, `
SET TRACING=on;
BEGIN;
SET TRANSACTION PRIORITY HIGH;
UPDATE test.test SET x = 100 WHERE x = 1;
COMMIT;
SET TRACING=off;
`)
		require.NoError(t, err)
		const contentionEventSubstring = "tableID=53 indexID=1"
		if contention {
			// Soft check to protect against flakiness where an internal query
			// causes the contention metric to increment.
			require.GreaterOrEqual(t, metrics.ContendedQueriesCount.Count(), int64(1))
		} else {
			require.Zero(
				t,
				metrics.ContendedQueriesCount.Count(),
				"contention metric unexpectedly non-zero when no contention events are produced",
			)
		}
		require.Equal(t, contention, strings.Contains(contentionRegistry.String(), contentionEventSubstring))
	})
}

// TestDistSQLReceiverDrainsOnError is a simple unit test that asserts that the
// DistSQLReceiver transitions to execinfra.DrainRequested status if an error is
// pushed into it.
func TestDistSQLReceiverDrainsOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	recv := MakeDistSQLReceiver(
		context.Background(),
		&errOnlyResultWriter{},
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn */
		nil, /* clockUpdater */
		&SessionTracing{},
		nil, /* contentionRegistry */
		nil, /* testingPushCallback */
	)
	status := recv.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.New("some error")})
	require.Equal(t, execinfra.DrainRequested, status)
}

// TestDistSQLReceiverDrainsMeta verifies that the DistSQLReceiver drains the
// execution flow in order to retrieve the required metadata. In particular, it
// sets up a 3 node cluster which is then accessed via PGWire protocol in order
// to take advantage of the LIMIT feature of portals (pausing the execution once
// the desired number of rows have been returned to the client). The crux of the
// test is, once the portal is closed and the execution flow is shutdown, making
// sure that the receiver collects LeafTxnFinalState metadata from each of the
// nodes which is required for correctness.
func TestDistSQLReceiverDrainsMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var accumulatedMeta []execinfrapb.ProducerMetadata
	// Set up a 3 node cluster and inject a callback to accumulate all metadata
	// for the test query.
	const numNodes = 3
	const testQuery = "SELECT * FROM foo"
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &ExecutorTestingKnobs{
					DistSQLReceiverPushCallbackFactory: func(query string) func(rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
						if query != testQuery {
							return nil
						}
						return func(row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata) {
							if meta != nil {
								accumulatedMeta = append(accumulatedMeta, *meta)
							}
						}
					},
				},
			},
			Insecure: true,
		}})
	defer tc.Stopper().Stop(ctx)

	// Create a table with 30 rows, split them into 3 ranges with each node
	// having one.
	db := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		30,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)
	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT VALUES (10), (20)")
	sqlDB.Exec(
		t,
		fmt.Sprintf("ALTER TABLE test.foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		),
	)

	// Connect to the cluster via the PGWire client.
	p, err := pgtest.NewPGTest(ctx, tc.Server(0).ServingSQLAddr(), security.RootUser)
	require.NoError(t, err)

	// Execute the test query asking for at most 25 rows.
	require.NoError(t, p.SendOneLine(`Query {"String": "USE test"}`))
	require.NoError(t, p.SendOneLine(fmt.Sprintf(`Parse {"Query": "%s"}`, testQuery)))
	require.NoError(t, p.SendOneLine(`Bind`))
	require.NoError(t, p.SendOneLine(`Execute {"MaxRows": 25}`))
	require.NoError(t, p.SendOneLine(`Sync`))

	// Retrieve all of the results. We need to receive until two 'ReadyForQuery'
	// messages are returned (the first one for "USE test" query and the second
	// one is for the limited portal execution).
	until := pgtest.ParseMessages("ReadyForQuery\nReadyForQuery")
	msgs, err := p.Until(false /* keepErrMsg */, until...)
	require.NoError(t, err)
	received := pgtest.MsgsToJSONWithIgnore(msgs, &datadriven.TestData{})

	// Confirm that we did retrieve 25 rows as well as 3 metadata objects.
	require.Equal(t, 25, strings.Count(received, `"Type":"DataRow"`))
	numLeafTxnFinalMeta := 0
	for _, meta := range accumulatedMeta {
		if meta.LeafTxnFinalState != nil {
			numLeafTxnFinalMeta++
		}
	}
	require.Equal(t, numNodes, numLeafTxnFinalMeta)
}
