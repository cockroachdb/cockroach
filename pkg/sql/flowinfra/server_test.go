// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	conn, err := s.RPCContext().GRPCDialNode(s.ServingRPCAddr(), s.NodeID(),
		rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `CREATE TABLE test.t (a INT PRIMARY KEY, b INT)`)
	r.Exec(t, `INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30)`)

	td := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	ts := execinfrapb.TableReaderSpec{
		Table:    *td,
		IndexIdx: 0,
		Reverse:  false,
		Spans:    []execinfrapb.TableReaderSpan{{Span: td.PrimaryIndexSpan(keys.SystemSQLCodec)}},
	}
	post := execinfrapb.PostProcessSpec{
		Filter:        execinfrapb.Expression{Expr: "@1 != 2"}, // a != 2
		Projection:    true,
		OutputColumns: []uint32{0, 1}, // a
	}

	txn := kv.NewTxn(ctx, kvDB, s.NodeID())
	leafInputState := txn.GetLeafTxnInputState(ctx)

	req := &execinfrapb.SetupFlowRequest{
		Version:           execinfra.Version,
		LeafTxnInputState: &leafInputState,
	}
	req.Flow = execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{{
			Core: execinfrapb.ProcessorCoreUnion{TableReader: &ts},
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE}},
			}},
		}},
	}

	distSQLClient := execinfrapb.NewDistSQLClient(conn)
	stream, err := distSQLClient.RunSyncFlow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&execinfrapb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
		t.Fatal(err)
	}

	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []execinfrapb.ProducerMetadata
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(context.Background(), msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
	}
	metas = ignoreLeafTxnState(metas)
	metas = ignoreMetricsMeta(metas)
	if len(metas) != 0 {
		t.Errorf("unexpected metadata: %v", metas)
	}
	str := rows.String(sqlbase.TwoIntCols)
	expected := "[[1 10] [3 30]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// Verify version handling.
	t.Run("version", func(t *testing.T) {
		testCases := []struct {
			version     execinfrapb.DistSQLVersion
			expectedErr string
		}{
			{
				version:     execinfra.Version + 1,
				expectedErr: "version mismatch",
			},
			{
				version:     execinfra.MinAcceptedVersion - 1,
				expectedErr: "version mismatch",
			},
			{
				version:     execinfra.MinAcceptedVersion,
				expectedErr: "",
			},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%d", tc.version), func(t *testing.T) {
				distSQLClient := execinfrapb.NewDistSQLClient(conn)
				stream, err := distSQLClient.RunSyncFlow(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				req.Version = tc.version
				if err := stream.Send(&execinfrapb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
					t.Fatal(err)
				}
				_, err = stream.Recv()
				if !testutils.IsError(err, tc.expectedErr) {
					t.Errorf("expected error '%s', got %v", tc.expectedErr, err)
				}
				// In the expectedErr == nil case, we leave a flow hanging; we're not
				// consuming it. It will get canceled by the draining process.
			})
		}
	})
}

// Test that a node gossips its DistSQL version information.
func TestDistSQLServerGossipsVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	var v execinfrapb.DistSQLVersionGossipInfo
	if err := s.GossipI().(*gossip.Gossip).GetInfoProto(
		gossip.MakeDistSQLNodeVersionKey(s.NodeID()), &v,
	); err != nil {
		t.Fatal(err)
	}

	if v.Version != execinfra.Version || v.MinAcceptedVersion != execinfra.MinAcceptedVersion {
		t.Fatalf("node is gossipping the wrong version. Expected: [%d-%d], got [%d-%d",
			execinfra.Version, execinfra.MinAcceptedVersion, v.Version, v.MinAcceptedVersion)
	}
}
