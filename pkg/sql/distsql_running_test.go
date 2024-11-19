// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, s.NodeID()),
		username.NodeUserName(),
		&MemoryMetrics{},
		&execCfg,
		sd,
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
		err = conflictTxn.Commit(ctx)
		require.NoError(t, err)
		t.Log(conflictTxn.Rollback(ctx))
		return err
	}

	// Make a db with a short heartbeat interval, so that the aborted txn finds
	// out quickly.
	ambient := s.AmbientCtx()
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
	tr := s.TracerI().(*tracing.Tracer)
	runningCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test")
	defer getRecAndFinish()
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
		rw := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
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
		)

		// We need to re-plan every time, since the plan is closed automatically
		// by PlanAndRun() below making it unusable across retries.
		p.stmt = makeStatement(stmt, clusterunique.ID{},
			tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&execCfg.Settings.SV)))
		if err := p.makeOptimizerPlan(ctx); err != nil {
			t.Fatal(err)
		}
		defer p.curPlan.close(ctx)

		evalCtx := p.ExtendedEvalContext()
		// We need distribute = true so that executing the plan involves marshaling
		// the root txn meta to leaf txns. Local flows can start in aborted txns
		// because they just use the root txn.
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p, nil /* txn */, FullDistribution)
		planCtx.stmtType = recv.stmtType

		execCfg.DistSQLPlanner.PlanAndRun(
			ctx, evalCtx, planCtx, txn, p.curPlan.main, recv, nil, /* finishedSetupFn */
		)
		return rw.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if iter != 2 {
		t.Fatalf("expected two iterations, but txn took %d to succeed", iter)
	}
	if tracing.FindMsgInRecording(getRecAndFinish(), clientRejectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", clientRejectedMsg)
	}
}

// TestDistSQLRunningParallelFKChecksAfterAbort simulates a SQL transaction
// that writes two rows required to validate a FK check and then proceeds to
// write a third row that would actually trigger this check. The transaction is
// aborted after the third row is written but before the FK check is performed.
// We assert that this construction doesn't throw a FK violation; instead, the
// transaction should be able to retry.
// This test serves as a regression test for the hazard identified in
// https://github.com/cockroachdb/cockroach/issues/97141.
func TestDistSQLRunningParallelFKChecksAfterAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	mu := struct {
		syncutil.Mutex
		abortTxn func(uuid uuid.UUID)
	}{}

	s, conn, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				RunBeforeCascadesAndChecks: func(txnID uuid.UUID) {
					mu.Lock()
					defer mu.Unlock()
					if mu.abortTxn != nil {
						mu.abortTxn(txnID)
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Set up schemas for the test. We want a construction that results in 2 FK
	// checks, of which 1 is done in parallel.
	sqlDB.Exec(t, "create database test")
	sqlDB.Exec(t, "create table test.parent1(a INT PRIMARY KEY)")
	sqlDB.Exec(t, "create table test.parent2(b INT PRIMARY KEY)")
	sqlDB.Exec(
		t,
		"create table test.child(a INT, b INT, FOREIGN KEY (a) REFERENCES test.parent1(a), FOREIGN KEY (b) REFERENCES test.parent2(b))",
	)
	key := roachpb.Key("a")

	setupQueries := []string{
		"insert into test.parent1 VALUES(1)",
		"insert into test.parent2 VALUES(2)",
	}
	query := "insert into test.child VALUES(1, 2)"

	createPlannerAndRunQuery := func(ctx context.Context, txn *kv.Txn, query string) error {
		execCfg := s.ExecutorConfig().(ExecutorConfig)
		// TODO(sql-queries): This sessiondata contains zero-values for most fields,
		// meaning DistSQLMode is DistSQLOff. Is this correct?
		sd := &sessiondata.SessionData{
			SessionData:   sessiondatapb.SessionData{},
			SearchPath:    sessiondata.DefaultSearchPathForUser(username.RootUserName()),
			SequenceState: sessiondata.NewSequenceState(),
			Location:      time.UTC,
		}
		// Plan the statement.
		internalPlanner, cleanup := NewInternalPlanner(
			"test",
			txn,
			username.NodeUserName(),
			&MemoryMetrics{},
			&execCfg,
			sd,
		)
		defer cleanup()
		p := internalPlanner.(*planner)
		stmt, err := parser.ParseOne(query)
		require.NoError(t, err)

		rw := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
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
		)

		p.stmt = makeStatement(stmt, clusterunique.ID{},
			tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&s.ClusterSettings().SV)))
		if err := p.makeOptimizerPlan(ctx); err != nil {
			t.Fatal(err)
		}
		defer p.curPlan.close(ctx)

		evalCtx := p.ExtendedEvalContext()
		planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p, txn, LocalDistribution)
		planCtx.stmtType = recv.stmtType

		evalCtxFactory := func(bool) *extendedEvalContext {
			factoryEvalCtx := extendedEvalContext{Tracing: evalCtx.Tracing}
			factoryEvalCtx.Context = evalCtx.Context
			return &factoryEvalCtx
		}
		err = execCfg.DistSQLPlanner.PlanAndRunAll(ctx, evalCtx, planCtx, p, recv, evalCtxFactory)
		if err != nil {
			return err
		}
		return rw.Err()
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
		err := conflictTxn.Commit(ctx)
		require.NoError(t, err)
		t.Log(conflictTxn.Rollback(ctx))
		return err
	}

	// Make a db with a short heartbeat interval, so that the aborted txn finds
	// out quickly.
	ambient := s.AmbientCtx()
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
	tr := s.TracerI().(*tracing.Tracer)
	runningCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test")
	defer getRecAndFinish()
	err := shortDB.Txn(runningCtx, func(ctx context.Context, txn *kv.Txn) error {
		iter++

		// set up the test.
		for _, query := range setupQueries {
			err := createPlannerAndRunQuery(ctx, txn, query)
			require.NoError(t, err)
		}

		if iter == 1 {
			// On the first iteration, abort the txn by setting the abortTxn function.
			mu.Lock()
			mu.abortTxn = func(txnID uuid.UUID) {
				if txnID != txn.ID() {
					return // not our txn
				}
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
			mu.Unlock()
			defer func() {
				// clear the abortTxn function before returning.
				mu.Lock()
				mu.abortTxn = nil
				mu.Unlock()
			}()
		}

		// Execute the FK checks.
		return createPlannerAndRunQuery(ctx, txn, query)
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, iter, 2)
	if tracing.FindMsgInRecording(getRecAndFinish(), clientRejectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", clientRejectedMsg)
	}
	concurrentFKChecksLogMessage := fmt.Sprintf(executingParallelAndSerialChecks, 1, 1)
	if tracing.FindMsgInRecording(getRecAndFinish(), concurrentFKChecksLogMessage) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", concurrentFKChecksLogMessage)
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

	rw := &errOnlyResultWriter{}
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		tree.Rows, /* StatementReturnType */
		nil,       /* rangeCache */
		txn,
		nil, /* clockUpdater */
		&SessionTracing{},
	)

	retryErr := kvpb.NewErrorWithTxn(
		kvpb.NewTransactionRetryError(
			kvpb.RETRY_SERIALIZABLE, "test err"),
		txn.TestingCloneTxn()).GoError()

	abortErr := kvpb.NewErrorWithTxn(
		kvpb.NewTransactionAbortedError(
			kvpb.ABORT_REASON_ABORTED_RECORD_FOUND),
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

		// Disable sampling so that only our query (below) gets a trace.
		// Otherwise, we're subject to flakes when internal queries experience contention.
		_, err := db.Exec("SET CLUSTER SETTING sql.txn_stats.sample_rate = 0")
		require.NoError(t, err)

		sqlutils.CreateTable(
			t, db, "test", "x INT PRIMARY KEY", 1, sqlutils.ToRowFn(sqlutils.RowIdxFn),
		)

		tableID := sqlutils.QueryTableID(t, db, sqlutils.TestDB, "public", "test")
		contentionEventSubstring := fmt.Sprintf("tableID=%d indexID=1", tableID)

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
		metrics.CumulativeContentionNanos.Clear()
		contentionRegistry := s.ExecutorConfig().(ExecutorConfig).ContentionRegistry
		otherConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, otherConn.Close())
		}()
		// TODO(yuzefovich): turning the tracing ON won't be necessary once
		// always-on tracing is enabled.
		_, err = otherConn.ExecContext(ctx, `SET TRACING=on;`)
		require.NoError(t, err)
		txn, err := otherConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = txn.ExecContext(ctx, `
			SET TRANSACTION PRIORITY HIGH;
			UPDATE test.test SET x = 100 WHERE x = 1;
		`)

		require.NoError(t, err)
		if contention {
			// Soft check to protect against flakiness where an internal query
			// causes the contention metric to increment.
			require.GreaterOrEqual(t, metrics.ContendedQueriesCount.Count(), int64(1))
			require.Positive(t, metrics.CumulativeContentionNanos.Count())
		} else {
			require.Zero(
				t,
				metrics.ContendedQueriesCount.Count(),
				"contention metric unexpectedly non-zero when no contention events are produced",
			)
			require.Zero(t, metrics.CumulativeContentionNanos.Count())
		}

		require.Equal(t, contention, strings.Contains(contentionRegistry.String(), contentionEventSubstring))
		err = txn.Commit()
		require.NoError(t, err)
		_, err = otherConn.ExecContext(ctx, `SET TRACING=off;`)
		require.NoError(t, err)
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
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				SQLExecutor: &ExecutorTestingKnobs{
					DistSQLReceiverPushCallbackFactory: func(_ context.Context, query string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
						if query != testQuery {
							return nil
						}
						return func(row rowenc.EncDatumRow, batch coldata.Batch, meta *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
							if meta != nil {
								accumulatedMeta = append(accumulatedMeta, *meta)
							}
							return row, batch, meta
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
	p, err := pgtest.NewPGTest(ctx, tc.Server(0).AdvSQLAddr(), username.RootUser)
	require.NoError(t, err)

	// We disable multiple active portals here as it only supports local-only plan.
	// TODO(sql-sessions): remove this line when we finish
	// https://github.com/cockroachdb/cockroach/issues/100822.
	require.NoError(t, p.SendOneLine(`Query {"String": "SET multiple_active_portals_enabled = false"}`))
	until := pgtest.ParseMessages("ReadyForQuery")
	_, err = p.Until(false /* keepErrMsg */, until...)
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
	until = pgtest.ParseMessages("ReadyForQuery\nReadyForQuery")
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

// TestCancelFlowsCoordinator performs sanity-checking of cancelFlowsCoordinator
// and that it can be safely used concurrently.
func TestCancelFlowsCoordinator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var c cancelFlowsCoordinator

	globalRng, _ := randutil.NewTestRand()
	numNodes := globalRng.Intn(16) + 2
	gatewaySQLInstanceID := base.SQLInstanceID(1)

	assertInvariants := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		// Check that the coordinator hasn't created duplicate entries for some
		// nodes.
		require.GreaterOrEqual(t, numNodes-1, c.mu.deadFlowsByNode.Len())
		seen := make(map[base.SQLInstanceID]struct{})
		for i := 0; i < c.mu.deadFlowsByNode.Len(); i++ {
			deadFlows := c.mu.deadFlowsByNode.Get(i)
			require.NotEqual(t, gatewaySQLInstanceID, deadFlows.sqlInstanceID)
			_, ok := seen[deadFlows.sqlInstanceID]
			require.False(t, ok)
			seen[deadFlows.sqlInstanceID] = struct{}{}
		}
	}

	// makeFlowsToCancel returns a fake flows map where each node in the cluster
	// has 67% probability of participating in the plan.
	makeFlowsToCancel := func(rng *rand.Rand) map[base.SQLInstanceID]*execinfrapb.FlowSpec {
		res := make(map[base.SQLInstanceID]*execinfrapb.FlowSpec)
		flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}
		for id := 1; id <= numNodes; id++ {
			if rng.Float64() < 0.33 {
				// This node wasn't a part of the current plan.
				continue
			}
			res[base.SQLInstanceID(id)] = &execinfrapb.FlowSpec{
				FlowID:  flowID,
				Gateway: gatewaySQLInstanceID,
			}
		}
		return res
	}

	var wg sync.WaitGroup
	maxSleepTime := 100 * time.Millisecond

	// Spin up some goroutines that simulate query runners, with each hitting an
	// error and deciding to cancel all scheduled dead flows.
	numQueryRunners := globalRng.Intn(8) + 1
	numRunsPerRunner := globalRng.Intn(10) + 1
	wg.Add(numQueryRunners)
	for i := 0; i < numQueryRunners; i++ {
		go func() {
			defer wg.Done()
			rng, _ := randutil.NewTestRand()
			for i := 0; i < numRunsPerRunner; i++ {
				c.addFlowsToCancel(makeFlowsToCancel(rng))
				time.Sleep(time.Duration(rng.Int63n(int64(maxSleepTime))))
			}
		}()
	}

	// Have a single goroutine that checks the internal state of the coordinator
	// and retrieves the next request to cancel some flows (in order to simulate
	// the canceling worker).
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng, _ := randutil.NewTestRand()
		done := time.After(2 * time.Second)
		for {
			select {
			case <-done:
				return
			default:
				assertInvariants()
				time.Sleep(time.Duration(rng.Int63n(int64(maxSleepTime))))
				// We're not interested in the result of this call.
				_, _ = c.getFlowsToCancel()
			}
		}
	}()

	wg.Wait()
}

// TestDistSQLRunnerCoordinator verifies that the runnerCoordinator correctly
// reacts to the changes of the corresponding setting.
func TestDistSQLRunnerCoordinator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := &s.ExecutorConfig().(ExecutorConfig).DistSQLPlanner.runnerCoordinator
	sqlDB := sqlutils.MakeSQLRunner(db)

	checkNumRunners := func(newNumRunners int64) {
		sqlDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING sql.distsql.num_runners = %d", newNumRunners))
		testutils.SucceedsSoon(t, func() error {
			numWorkers := atomic.LoadInt64(&runner.atomics.numWorkers)
			if numWorkers != newNumRunners {
				return errors.Newf("%d workers are up, want %d", numWorkers, newNumRunners)
			}
			return nil
		})
	}

	// Lower the setting to 0 and make sure that all runners exit.
	checkNumRunners(0)

	// Now bump it up to 100.
	checkNumRunners(100)
}

// TestSetupFlowRPCError verifies that the distributed query plan errors out and
// cleans up all flows if the SetupFlow RPC fails for one of the remote nodes.
// It also checks that the expected error is returned.
func TestSetupFlowRPCError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a 3 node cluster where we can inject an error for SetupFlow RPC on
	// the server side for the queries in question.
	const numNodes = 3
	ctx := context.Background()
	getError := func(nodeID base.SQLInstanceID) error {
		return errors.Newf("injected error on n%d", nodeID)
	}
	// We use different queries to simplify handling the node ID on which the
	// error should be injected (i.e. we avoid the need for synchronization in
	// the test). In particular, the difficulty comes from the fact that some of
	// the SetupFlow RPCs might not be issued at all while others are served
	// after the corresponding flow on the gateway has exited.
	queries := []string{
		"SELECT k FROM test.foo",
		"SELECT v FROM test.foo",
		"SELECT * FROM test.foo",
	}
	stmtToNodeIDForError := map[string]base.SQLInstanceID{
		queries[0]: 2, // error on n2
		queries[1]: 3, // error on n3
		queries[2]: 0, // no error
	}
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					SetupFlowCb: func(_ context.Context, nodeID base.SQLInstanceID, req *execinfrapb.SetupFlowRequest) error {
						nodeIDForError, ok := stmtToNodeIDForError[req.StatementSQL]
						if !ok || nodeIDForError != nodeID {
							return nil
						}
						return getError(nodeID)
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Create a table with 30 rows, split them into 3 ranges with each node
	// having one.
	db := tc.ServerConn(0)
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

	// assertNoRemoteFlows verifies that the remote flows exit "soon".
	//
	// Note that in practice this happens very quickly, but in an edge case it
	// could take 10s (sql.distsql.flow_stream_timeout). That edge case occurs
	// when the server-side goroutine of the SetupFlow RPC is scheduled after
	// - the gateway flow exits with an error
	// - the CancelDeadFlows RPC for the remote flow in question completes.
	// With such setup the FlowStream RPC of the outbox will time out after 10s.
	assertNoRemoteFlows := func() {
		testutils.SucceedsSoon(t, func() error {
			for i, remoteNode := range []*distsql.ServerImpl{
				tc.Server(1).DistSQLServer().(*distsql.ServerImpl),
				tc.Server(2).DistSQLServer().(*distsql.ServerImpl),
			} {
				if n := remoteNode.NumRemoteRunningFlows(); n != 0 {
					return errors.Newf("%d remote flows still running on n%d", n, i+2)
				}
			}
			return nil
		})
	}

	// Run query twice while injecting an error on the remote nodes.
	for i := 0; i < 2; i++ {
		query := queries[i]
		nodeID := stmtToNodeIDForError[query]
		t.Logf("running %q with error being injected on n%d", query, nodeID)
		_, err := db.ExecContext(ctx, query)
		require.True(t, strings.Contains(err.Error(), getError(nodeID).Error()))
		assertNoRemoteFlows()
	}

	// Sanity check that the query doesn't error out without error injection.
	t.Logf("running %q with no error injection", queries[2])
	_, err := db.ExecContext(ctx, queries[2])
	require.NoError(t, err)
	assertNoRemoteFlows()
}

// TestDistSQLPlannerParallelChecks can be used to stress the behavior of
// postquery checks when they run in parallel.
func TestDistSQLPlannerParallelChecks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	rng, _ := randutil.NewTestRand()

	sqlDB := sqlutils.MakeSQLRunner(db)
	// Set up a child table with two foreign keys into two parent tables.
	sqlDB.Exec(t, `CREATE TABLE parent1 (id1 INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE parent2 (id2 INT8 PRIMARY KEY)`)
	sqlDB.Exec(t, `
CREATE TABLE child (
    id INT8 PRIMARY KEY, parent_id1 INT8 NOT NULL, parent_id2 INT8 NOT NULL,
    FOREIGN KEY (parent_id1) REFERENCES parent1 (id1),
    FOREIGN KEY (parent_id2) REFERENCES parent2 (id2)
);`)
	// Disable the insert fast path in order for the foreign key checks to be
	// planned as parallel postqueries.
	sqlDB.Exec(t, `SET enable_insert_fast_path = false`)
	if rng.Float64() < 0.1 {
		// In 10% of the cases, set a very low workmem limit in order to force
		// the bufferNode to spill to disk.
		sqlDB.Exec(t, `SET distsql_workmem = '1KiB'`)
	}

	const numIDs = 1000
	for id := 0; id < numIDs; id++ {
		sqlDB.Exec(t, `INSERT INTO parent1 VALUES ($1)`, id)
		sqlDB.Exec(t, `INSERT INTO parent2 VALUES ($1)`, id)
		var prefix string
		if rng.Float64() < 0.5 {
			// In 50% of the cases, run the INSERT query with FK checks via
			// EXPLAIN ANALYZE (or EXPLAIN ANALYZE (DEBUG)) in order to exercise
			// the planning code paths that are only taken when the tracing is
			// enabled.
			prefix = "EXPLAIN ANALYZE "
			if rng.Float64() < 0.02 {
				// Run DEBUG flavor only in 1% of all cases since it is
				// noticeably slower.
				prefix = "EXPLAIN ANALYZE (DEBUG) "
			}
		}
		if rng.Float64() < 0.1 {
			// In 10% of the cases, run the INSERT that results in an error (we
			// don't have any negative ids in the parent tables).
			invalidID := -1
			// The FK violation occurs for both FKs, but we expect that the
			// error for parent_id1 is always chosen.
			sqlDB.ExpectErr(
				t,
				`insert on table "child" violates foreign key constraint "child_parent_id1_fkey"`,
				fmt.Sprintf(`%[1]sINSERT INTO child VALUES (%[2]d, %[2]d, %[2]d)`, prefix, invalidID),
			)
			continue
		}
		sqlDB.Exec(t, fmt.Sprintf(`%[1]sINSERT INTO child VALUES (%[2]d, %[2]d, %[2]d)`, prefix, id))
	}
}

// TestDistributedQueryErrorIsRetriedLocally verifies that if a query with a
// distributed plan results in a SQL retryable error, then it is rerun as local
// transparently.
func TestDistributedQueryErrorIsRetriedLocally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a 3 node cluster where we can inject an error for SetupFlow RPC on
	// the server side for the queries in question.
	const numNodes = 3
	getError := func(nodeID base.SQLInstanceID) error {
		return errors.Newf("connection refused: n%d", nodeID)
	}
	// Assert that the injected error is in the allow-list of errors that are
	// retried transparently.
	if err := getError(base.SQLInstanceID(0)); !pgerror.IsSQLRetryableError(err) {
		t.Fatalf("expected error to be in the allow-list for a retry: %v", err)
	}

	// We use different queries to simplify handling the node ID on which the
	// error should be injected (i.e. we avoid the need for synchronization in
	// the test). In particular, the difficulty comes from the fact that some of
	// the SetupFlow RPCs might not be issued at all while others are served
	// after the corresponding flow on the gateway has exited.
	queries := []string{
		"SELECT k FROM test.foo",
		// Run one of the queries via EXPLAIN ANALYZE (DEBUG) so that we can
		// check the contents of the bundle later.
		"EXPLAIN ANALYZE (DEBUG) SELECT v FROM test.foo",
		"SELECT * FROM test.foo",
	}
	stmtToNodeIDForError := map[string]base.SQLInstanceID{
		queries[0]: 2, // error on n2
		queries[1]: 3, // error on n3
		queries[2]: 0, // no error
	}
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					SetupFlowCb: func(_ context.Context, nodeID base.SQLInstanceID, req *execinfrapb.SetupFlowRequest) error {
						nodeIDForError, ok := stmtToNodeIDForError[req.StatementSQL]
						if !ok || nodeIDForError != nodeID {
							return nil
						}
						return getError(nodeID)
					},
				},
			},
			Insecure: true,
		},
	})
	defer tc.Stopper().Stop(context.Background())

	// Create a table with 30 rows, split them into 3 ranges with each node
	// having one.
	db := tc.ServerConn(0)
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

	var bundleRows [][]string
	for _, query := range queries {
		nodeID := stmtToNodeIDForError[query]
		injectError := nodeID != base.SQLInstanceID(0)
		if injectError {
			t.Logf("running %q with error being injected on n%d", query, nodeID)
		} else {
			t.Logf("running %q without error being injected", query)
		}
		sqlDB.Exec(t, "SET TRACING=on;")
		// We expect that the query was retried as local which should succeed,
		// so we can use the sql runner.
		rows := sqlDB.QueryStr(t, query)
		sqlDB.Exec(t, "SET TRACING=off;")
		if strings.HasPrefix(query, "EXPLAIN ANALYZE (DEBUG)") {
			bundleRows = rows
		}
		trace := sqlDB.QueryStr(t, "SELECT message FROM [SHOW TRACE FOR SESSION]")
		// Inspect the trace to ensure that the query was, indeed, initially run
		// as distributed but hit a retryable error and was rerun as local.
		var foundDistributed, foundLocal bool
		for _, message := range trace {
			if strings.Contains(message[0], "creating DistSQL plan with isLocal=false") {
				foundDistributed = true
			} else if strings.Contains(message[0], "encountered an error when running the distributed plan, re-running it as local") {
				foundLocal = true
			}
		}
		if injectError {
			if !foundDistributed || !foundLocal {
				t.Fatalf("with remote error injection, foundDistributed=%t, foundLocal=%t\ntrace:%s", foundDistributed, foundLocal, trace)
			}
		} else {
			// When no error is injected, the query should succeed right away
			// when run in distributed fashion.
			if !foundDistributed || foundLocal {
				t.Fatalf("without remote error injection, foundDistributed=%t, foundLocal=%t\ntrace:%s", foundDistributed, foundLocal, trace)
			}
		}
	}

	// Now disable the retry mechanism and ensure that when remote error is
	// injected, it is returned as the query result.
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.distsql.distributed_query_rerun_locally.enabled = false;")
	for _, query := range queries[:2] {
		nodeID := stmtToNodeIDForError[query]
		t.Logf("running %q with error being injected on n%d but local retry disabled", query, nodeID)
		_, err := db.Exec(query)
		require.NotNil(t, err)
		// lib/pq wraps the error, so we cannot use errors.Is() check.
		require.True(t, strings.Contains(err.Error(), getError(nodeID).Error()))
	}

	// Now sanity check the contents of the stmt bundle that was collected when
	// retry-as-local mechanism kicked in.
	baseFiles := `env.sql opt-v.txt opt-vv.txt opt.txt plan.txt schema.sql statement.sql stats-test.public.foo.sql trace-jaeger.json trace.json trace.txt`
	distributedRunFiles := `distsql-1-main-query.html vec-1-main-query-v.txt vec-1-main-query.txt`
	localRunFiles := `distsql-2-main-query.html vec-2-main-query-v.txt vec-2-main-query.txt`
	checkBundle(
		t, fmt.Sprint(bundleRows), "public.foo" /* tableName */, nil, false, /* expectErrors */
		baseFiles, distributedRunFiles, localRunFiles,
	)
}

// TestLogicalPlanCorruptionBeforeRetryingLocally verifies that if a distributed
// query (that has a TableReader in the local flow) fails with such an error
// that gets retried via the "retry-as-local" mechanism, the query still
// produces the correct result. This is a regression test for #110712 which
// occurred because of the logical plan corruption during the distributed plan
// run (namely, scanNode.spans slice was being corrupted).
func TestLogicalPlanCorruptionBeforeRetryingLocally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Use a query such that
	// - it is distributed (because it has a join)
	// - it has a TableReader on the local node
	// - that TableReader takes over the whole spans slice assigned to it
	//   (because it has a LIMIT 1, so the spans aren't partitioned).
	targetQuery := `WITH cte1 AS (SELECT job_id, details FROM job_info WHERE (job_id = 1) ORDER BY info_key DESC LIMIT 1)
SELECT id, details FROM jobs AS j INNER JOIN cte1 ON id = job_id WHERE id = 1;
`
	errToInject := errors.Newf("connection refused: n2")
	// Sanity check that this error will be treated as retryable for
	// rerun-as-local mechanism.
	require.True(t, pgerror.IsSQLRetryableError(errToInject))

	// Create a three node cluster that has a special "push callback" which
	// injects the error on the first call to DistSQLReceiver.PushBatch. This
	// particular point during the execution is crucial for reproducing #110721:
	// - the distributed physical planning has been performed,
	// - all TableReaders have fetched some data,
	// - any resume spans in the local TableReader would previously corrupt the
	//   spans stored in the logical plan.
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &ExecutorTestingKnobs{
					DistSQLReceiverPushCallbackFactory: func(_ context.Context, query string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
						if !strings.HasPrefix(query, targetQuery[:20]) {
							return nil
						}
						var injected bool
						return func(row rowenc.EncDatumRow, batch coldata.Batch, meta *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
							if !injected {
								// Inject the error only once so that when we
								// retry the query as local, it succeeds.
								row, batch = nil, nil
								meta = &execinfrapb.ProducerMetadata{Err: errToInject}
								injected = true
							}
							return row, batch, meta
						}
					},
				},
			},
		}})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE TABLE job_info(job_id INT, info_key INT, details INT, PRIMARY KEY (job_id, info_key));")
	sqlDB.Exec(t, "CREATE TABLE jobs(id INT PRIMARY KEY);")
	sqlDB.Exec(t, "INSERT INTO job_info VALUES (1, 1, 1);")
	sqlDB.Exec(t, "INSERT INTO jobs VALUES (1);")
	sqlDB.Exec(t, "ALTER TABLE job_info SPLIT AT VALUES (1, 1);")
	sqlDB.Exec(t, "ALTER TABLE jobs SPLIT AT VALUES (1);")
	sqlDB.Exec(t, "ALTER TABLE job_info EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 1, 1);")
	sqlDB.Exec(t, "ALTER TABLE jobs EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 1);")

	// Sanity check that the query gets the distributed plan as we expect.
	//
	// (Note that ideally we would have used EXPLAIN (DISTSQL) and compared the
	// diagrams, but for some reason the encoded urls differ slightly between
	// runs although visible diagrams remain the same, so we use EXPLAIN (VEC)
	// below.)
	expectedPlan := `
│
├ Node 1
│ └ *colexec.ParallelUnorderedSynchronizer
│   ├ *colrpc.Inbox
│   └ *colexecjoin.mergeJoinInnerOp
│     ├ *colrpc.Inbox
│     └ *colflow.routerOutputOp
│       └ *colflow.HashRouter
│         └ *colexecsel.selEQInt64Int64ConstOp
│           └ *colexec.limitOp
│             └ *colfetcher.ColBatchScan
└ Node 2
  └ *colrpc.Outbox
    └ *colexecjoin.mergeJoinInnerOp
      ├ *colflow.routerOutputOp
      │ └ *colflow.HashRouter
      │   └ *colfetcher.ColBatchScan
      └ *colrpc.Inbox
`
	expectedPlanRows := strings.Split(expectedPlan, "\n")
	// Skip the first empty line.
	expectedPlanRows = expectedPlanRows[1:]
	// Make sure that "regular" ColBatchScans are used to make the output of
	// EXPLAIN (VEC) constant.
	sqlDB.Exec(t, "SET direct_columnar_scans_enabled = false")
	actualPlanRows := sqlDB.QueryStr(t, "EXPLAIN (VEC) "+targetQuery)
	for i, r := range actualPlanRows {
		if expectedPlanRows[i] != r[0] {
			var actualPlan string
			for _, r := range actualPlanRows {
				actualPlan += "\n" + r[0]
			}
			t.Fatalf("expected plan: %s\nactual plan: %s", expectedPlan, actualPlan)
		}
	}

	// Now the meat of the test - run the query that gets an error injected
	// during the distributed execution, then automatically is retried as local,
	// and produces the correct output.
	r := sqlDB.QueryRow(t, targetQuery)
	var id, details int
	r.Scan(&id, &details)
	require.Equal(t, 1, id)
	require.Equal(t, 1, details)
}
