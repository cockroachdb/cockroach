// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package sql

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Test portal implicit destruction. Unless destroying a portal is explicitly
// requested, portals live until the end of the transaction in which
// they'recreated. If they're created outside of a transaction, they live until
// the next transaction completes (so until the next statement is executed,
// which statement is expected to be the execution of the portal that was just
// created).
// For the non-transactional case, our behavior is different than Postgres',
// which states that, outside of transactions, portals live until the next Sync
// protocol command.
func TestPortalsDestroyedOnTxnFinish(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	buf, syncResults, finished, stopper, err := startConnExecutor(ctx)
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
	stmt := mustParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err = buf.Push(ctx, PrepareStmt{Name: "ps_nontxn", Statement: stmt}); err != nil {
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

	cmdPos++
	failedDescribePos := cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, Sync{}); err != nil {
		t.Fatal(err)
	}

	results := <-syncResults
	numResults := len(results)
	if numResults != cmdPos+1 {
		t.Fatalf("expected %d results, got: %d", cmdPos+1, len(results))
	}
	if err := results[successfulDescribePos].err; err != nil {
		t.Fatalf("expected first Describe to succeed, got err: %s", err)
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
	stmt = mustParseOne("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if err := buf.Push(ctx, ExecStmt{Statement: stmt}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	stmt = mustParseOne("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if err = buf.Push(ctx, PrepareStmt{Name: "ps1", Statement: stmt}); err != nil {
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
	stmt = mustParseOne("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}
	if err := buf.Push(ctx, ExecStmt{Statement: stmt}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	successfulDescribePos = cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	stmt = mustParseOne("COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if err := buf.Push(ctx, ExecStmt{Statement: stmt}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	failedDescribePos = cmdPos
	if err = buf.Push(ctx, DescribeStmt{
		Name: "portal1",
		Type: pgwirebase.PreparePortal,
	}); err != nil {
		t.Fatal(err)
	}

	cmdPos++
	if err = buf.Push(ctx, Sync{}); err != nil {
		t.Fatal(err)
	}

	results = <-syncResults

	exp := cmdPos + 1 - numResults
	if len(results) != exp {
		t.Fatalf("expected %d results, got: %d", exp, len(results))
	}
	succDescIdx := successfulDescribePos - numResults
	if err := results[succDescIdx].err; err != nil {
		t.Fatalf("expected first Describe to succeed, got err: %s", err)
	}
	failDescIdx := failedDescribePos - numResults
	if !testutils.IsError(results[failDescIdx].err, "unknown portal") {
		t.Fatalf("expected error \"unknown portal\", got: %v", results[failDescIdx].err)
	}

	buf.Close()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func mustParseOne(s string) parser.Statement {
	stmts, err := parser.Parse(s)
	if err != nil {
		log.Fatal(context.TODO(), err)
	}
	return stmts[0]
}

type dummyLivenessProvider struct {
}

// IsLive implements the livenessProvider interface.
func (l dummyLivenessProvider) IsLive(roachpb.NodeID) (bool, error) {
	return true, nil
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
func startConnExecutor(
	ctx context.Context,
) (*StmtBuf, <-chan []resWithPos, <-chan error, *stop.Stopper, error) {
	// A lot of boilerplate for creating a connExecutor.
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, 0 /* maxOffset */)
	factory := client.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, nil
		})
	db := client.NewDB(testutils.MakeAmbientCtx(), factory, clock)
	st := cluster.MakeTestingClusterSettings()
	nodeID := &base.NodeIDContainer{}
	nodeID.Set(ctx, 1)
	distSQLMetrics := distsqlrun.MakeDistSQLMetrics(time.Hour /* histogramWindow */)
	cfg := &ExecutorConfig{
		AmbientCtx:      testutils.MakeAmbientCtx(),
		Settings:        st,
		Clock:           clock,
		DB:              db,
		SessionRegistry: NewSessionRegistry(),
		NodeInfo: NodeInfo{
			NodeID:    nodeID,
			ClusterID: func() uuid.UUID { return uuid.UUID{} },
		},
		DistSQLPlanner: NewDistSQLPlanner(
			ctx, distsqlrun.Version, st, roachpb.NodeDescriptor{},
			nil, /* rpcCtx */
			distsqlrun.NewServer(ctx, distsqlrun.ServerConfig{
				AmbientContext: testutils.MakeAmbientCtx(),
				Settings:       st,
				Stopper:        stopper,
				Metrics:        &distSQLMetrics,
				NodeID:         nodeID,
			}),
			nil, /* distSender */
			nil, /* gossip */
			stopper,
			dummyLivenessProvider{}, /* liveness */
			nil,                     /* nodeDialer */
		),
		QueryCache:   querycache.New(0),
		TestingKnobs: &ExecutorTestingKnobs{},
	}
	pool := mon.MakeUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource,
		nil /* curCount */, nil /* maxHist */, math.MaxInt64, st,
	)
	// This pool should never be Stop()ed because, if the test is failing, memory
	// is not properly released.

	s := NewServer(cfg, &pool)
	buf := NewStmtBuf()
	syncResults := make(chan []resWithPos, 1)
	var cc ClientComm = &internalClientComm{
		sync: func(res []resWithPos) {
			syncResults <- res
		},
	}
	sqlMetrics := MakeMemMetrics("test" /* endpoint */, time.Second /* histogramWindow */)

	conn, err := s.SetupConn(ctx, SessionArgs{}, buf, cc, sqlMetrics)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	finished := make(chan error)

	// We're going to run the connExecutor in the background. On the main test
	// routine, we're going to push commands into the StmtBuf and, from time to
	// time, collect and check their results.
	go func() {
		finished <- s.ServeConn(ctx, conn, mon.BoundAccount{}, nil /* cancel */)
	}()
	return buf, syncResults, finished, stopper, nil
}
