// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package sql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// Test portal implicit destruction. Unless destroying a portal is explicitly
// requested, portals live until the end of the transaction in which they're
// created. If they're created outside of a transaction, they live until
// the implicit transaction completes. As per the PostgreSQL docs, the implicit
// transaction completes when the next Sync protocol command is handled.
func TestPortalsDestroyedOnTxnFinish(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	buf, syncResults, finished, stopper, resultChannel, err := startConnExecutor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stopper.Stop(ctx)
	defer func() {
		buf.Close()
	}()

	// First we test the non-transactional case. We'll send a
	// Parse/Bind/Describe/Execute/Describe. We expect the first Describe to
	// succeed and the 2nd one to fail (since the portal is destroyed after the
	// Execute).
	cmdPos := 0
	if err = buf.Push(ctx, PrepareStmt{Name: "ps_nontxn", Statement: mustParseOne("SELECT 1")}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, BindStmt{
		PreparedStatementName: "ps_nontxn",
		PortalName:            "portal1",
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	successfulDescribePos := cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	successfulDescribePos = cmdPos
	if err = buf.Push(ctx, ExecPortal{
		Name: "portal1",
	}); err != nil {
		t.Fatal(err)
	}

	_, _, err = resultChannel.nextResult(ctx)
	require.NoError(t, err)

	cmdPos++
	secondSuccessfulDescribePos := cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, Sync{ExplicitFromClient: true}); err != nil {
		t.Fatal(err)
	}

	results := <-syncResults
	numResults := len(results)
	if numResults != cmdPos+1 {
		t.Fatalf("expected %d results, got: %d", cmdPos+1, len(results))
	}
	if err = results[successfulDescribePos].err; err != nil {
		t.Fatalf("expected first Describe to succeed, got err: %s", err)
	}
	if err = results[secondSuccessfulDescribePos].err; err != nil {
		t.Fatalf("expected second Describe to succeed, got err: %s", err)
	}

	// cmdPos gets reset after the Sync.
	cmdPos = 0
	failedDescribePos := cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}
	cmdPos++
	if err = buf.Push(ctx, Sync{ExplicitFromClient: true}); err != nil {
		t.Fatal(err)
	}

	results = <-syncResults
	numResults = len(results)
	if numResults != cmdPos+1 {
		t.Fatalf("expected %d results, got: %d", cmdPos+1, len(results))
	}
	if !testutils.IsError(results[failedDescribePos].err, "unknown portal") {
		t.Fatalf("expected error \"unknown portal\", got: %v", results[failedDescribePos].err)
	}

	// Now we test the transactional case. We'll send a
	// BEGIN/Parse/Bind/SELECT/Describe/COMMIT/Describe. We expect the first
	// Describe to succeed and the 2nd one to fail (since the portal is destroyed
	// after the COMMIT). The point of the SELECT is to show that the portal
	// survives execution of a statement.
	cmdPos++
	if err = buf.Push(ctx, ExecStmt{Statement: mustParseOne("BEGIN")}); err != nil {
		t.Fatal(err)
	}

	_, _, err = resultChannel.nextResult(ctx)
	require.NoError(t, err)

	cmdPos++
	if err = buf.Push(ctx, PrepareStmt{Name: "ps1", Statement: mustParseOne("SELECT 1")}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, BindStmt{
		PreparedStatementName: "ps1",
		PortalName:            "portal1",
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, ExecStmt{Statement: mustParseOne("SELECT 2")}); err != nil {
		t.Fatal(err)
	}

	_, _, err = resultChannel.nextResult(ctx)
	require.NoError(t, err)

	cmdPos++
	successfulDescribePos = cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, ExecStmt{Statement: mustParseOne("COMMIT")}); err != nil {
		t.Fatal(err)
	}

	_, _, err = resultChannel.nextResult(ctx)
	require.NoError(t, err)

	cmdPos++
	secondSuccessfulDescribePos = cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, Sync{ExplicitFromClient: true}); err != nil {
		t.Fatal(err)
	}

	results = <-syncResults

	exp := cmdPos + 1 - numResults
	if len(results) != exp {
		t.Fatalf("expected %d results, got: %d", exp, len(results))
	}
	succDescIdx := successfulDescribePos - numResults
	if err = results[succDescIdx].err; err != nil {
		t.Fatalf("expected first Describe to succeed, got err: %s", err)
	}
	failDescIdx := secondSuccessfulDescribePos - numResults
	if !testutils.IsError(results[failDescIdx].err, "unknown portal") {
		t.Fatalf("expected error \"unknown portal\", got: %v", results[failDescIdx].err)
	}

	buf.Close()
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
}

func mustParseOne(s string) statements.Statement[tree.Statement] {
	stmts, err := parser.Parse(s)
	if err != nil {
		log.Fatalf(context.Background(), "%v", err)
	}
	return stmts[0]
}

// startConnExecutor start a goroutine running a connExecutor. This connExecutor
// is using a mocked KV that can't really do anything, so it can't run
// statements that need to "access the database". It can only execute things
// like `SELECT 1`. It's intended for testing interactions with the network
// protocol.
//
// It returns a StmtBuf which is to be used to providing input to the executor,
// a channel for getting results after sending Sync commands, a channel that
// gets the error from closing down the executor once the StmtBuf is closed, a
// stopper that must be stopped when the test completes (this does not stop the
// executor but stops other background work).
//
// It also returns an asyncIEResultChannel which can buffer up to
// asyncIEResultChannelBufferSize items written by AddRow, so the caller might
// need to read from it.
func startConnExecutor(
	ctx context.Context,
) (
	*StmtBuf,
	<-chan []*streamingCommandResult,
	<-chan error,
	*stop.Stopper,
	ieResultReader,
	error,
) {
	// A lot of boilerplate for creating a connExecutor.
	stopper := stop.NewStopper()
	clock := hlc.NewClockForTesting(nil)
	factory := kv.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
		) (*kvpb.BatchResponse, *kvpb.Error) {
			return nil, nil
		})
	db := kv.NewDB(log.MakeTestingAmbientCtxWithNewTracer(), factory, clock, stopper)
	st := cluster.MakeTestingClusterSettings()
	nodeID := base.TestingIDContainer
	distSQLMetrics := execinfra.MakeDistSQLMetrics(time.Hour /* histogramWindow */)
	gw := gossip.MakeOptionalGossip(nil)
	tempEngine, tempFS, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec, nil /* statsCollector */)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	defer tempEngine.Close()
	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	pool := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     mon.MakeMonitorName("test"),
		Settings: st,
	})
	// This pool should never be Stop()ed because, if the test is failing, memory
	// is not properly released.
	collectionFactory := descs.NewBareBonesCollectionFactory(st, keys.SystemSQLCodec)
	cfg := &ExecutorConfig{
		AmbientCtx: ambientCtx,
		Settings:   st,
		Clock:      clock,
		DB:         db,
		SystemConfig: config.NewConstantSystemConfigProvider(
			config.NewSystemConfig(zonepb.DefaultZoneConfigRef()),
		),
		SessionRegistry:    NewSessionRegistry(),
		ClosedSessionCache: NewClosedSessionCache(st, pool, time.Now),
		NodeInfo: NodeInfo{
			NodeID:           nodeID,
			LogicalClusterID: func() uuid.UUID { return uuid.UUID{} },
		},
		Codec: keys.SystemSQLCodec,
		DistSQLPlanner: NewDistSQLPlanner(
			ctx, st, 1, /* sqlInstanceID */
			nil, /* rpcCtx */
			distsql.NewServer(
				ctx,
				execinfra.ServerConfig{
					AmbientContext:    ambientCtx,
					Settings:          st,
					Stopper:           stopper,
					Metrics:           &distSQLMetrics,
					NodeID:            nodeID,
					TempFS:            tempFS,
					ParentDiskMonitor: execinfra.NewTestDiskMonitor(ctx, st),
					CollectionFactory: collectionFactory,
				},
				flowinfra.NewRemoteFlowRunner(ambientCtx, stopper, nil /* acc */),
			),
			nil, /* distSender */
			nil, /* nodeDescs */
			gw,
			stopper,
			func(base.SQLInstanceID) bool { return true }, // everybody is available
			nil, /* connHealthCheckerSystem */
			nil, /* instanceConnHealthChecker */
			nil, /* sqlInstanceDialer */
			keys.SystemSQLCodec,
			nil, /* sqlAddressResolver */
			clock,
		),
		QueryCache:              querycache.New(0),
		TestingKnobs:            ExecutorTestingKnobs{},
		StmtDiagnosticsRecorder: stmtdiagnostics.NewRegistry(nil, st),
		HistogramWindowInterval: base.DefaultHistogramWindowInterval(),
		CollectionFactory:       collectionFactory,
		LicenseEnforcer:         license.NewEnforcer(nil),
	}

	s := NewServer(cfg, pool)
	buf := NewStmtBuf(0 /* toReserve */)
	syncResults := make(chan []*streamingCommandResult, 1)
	resultChannel := newAsyncIEResultChannel()
	var cc ClientComm = &internalClientComm{
		sync: func(res []*streamingCommandResult) {
			syncResults <- res
		},
		w: resultChannel,
	}
	sqlMetrics := MakeMemMetrics("test" /* endpoint */, time.Second /* histogramWindow */)

	onDefaultIntSizeChange := func(int32) {}
	conn, err := s.SetupConn(
		ctx,
		SessionArgs{},
		buf,
		cc,
		sqlMetrics,
		onDefaultIntSizeChange,
		clusterunique.ID{},
	)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	finished := make(chan error)

	// We're going to run the connExecutor in the background. On the main test
	// routine, we're going to push commands into the StmtBuf and, from time to
	// time, collect and check their results.
	go func() {
		finished <- s.ServeConn(
			ctx,
			conn,
			&mon.BoundAccount{},
			nil, /* cancel */
		)
	}()
	return buf, syncResults, finished, stopper, resultChannel, nil
}

// Test that a client session can close without deadlocking when the closing
// needs to cleanup temp tables and the txn that has created these tables is
// still open. The act of cleaning up used to block for the open transaction,
// thus deadlocking.
func TestSessionCloseWithPendingTempTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	srv := s.SQLServer().(*Server)
	stmtBuf := NewStmtBuf(0 /* toReserve */)
	flushed := make(chan []*streamingCommandResult)
	clientComm := &internalClientComm{
		sync: func(res []*streamingCommandResult) {
			flushed <- res
		},
	}
	onDefaultIntSizeChange := func(int32) {}
	connHandler, err := srv.SetupConn(
		ctx,
		SessionArgs{User: username.RootUserName()},
		stmtBuf,
		clientComm,
		MemoryMetrics{},
		onDefaultIntSizeChange,
		clusterunique.ID{},
	)
	require.NoError(t, err)

	stmts, err := parser.Parse(`
SET experimental_enable_temp_tables = true;
CREATE DATABASE test;
USE test;
BEGIN;
CREATE TEMPORARY TABLE foo();
`)
	require.NoError(t, err)
	for _, stmt := range stmts {
		require.NoError(t, stmtBuf.Push(ctx, ExecStmt{Statement: stmt}))
	}
	require.NoError(t, stmtBuf.Push(ctx, Sync{ExplicitFromClient: false}))

	done := make(chan error)
	go func() {
		done <- srv.ServeConn(
			ctx,
			connHandler,
			&mon.BoundAccount{},
			nil, /* cancel */
		)
	}()
	results := <-flushed
	require.Len(t, results, 6) // We expect results for 5 statements + sync.
	for _, res := range results {
		require.NoError(t, res.err)
	}

	// Close the client connection and verify that ServeConn() returns.
	stmtBuf.Close()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("session close timed out; connExecutor deadlocked?")
	case err = <-done:
		require.NoError(t, err)
	}
}
