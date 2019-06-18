// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
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
	conn, err := s.RPCContext().GRPCDialNode(s.ServingAddr(), s.NodeID()).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `CREATE TABLE test.t (a INT PRIMARY KEY, b INT)`)
	r.Exec(t, `INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30)`)

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	ts := distsqlpb.TableReaderSpec{
		Table:    *td,
		IndexIdx: 0,
		Reverse:  false,
		Spans:    []distsqlpb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
	}
	post := distsqlpb.PostProcessSpec{
		Filter:        distsqlpb.Expression{Expr: "@1 != 2"}, // a != 2
		Projection:    true,
		OutputColumns: []uint32{0, 1}, // a
	}

	txn := client.NewTxn(ctx, kvDB, s.NodeID(), client.RootTxn)
	txnCoordMeta := txn.GetTxnCoordMeta(ctx)
	txnCoordMeta.StripRootToLeaf()

	req := &distsqlpb.SetupFlowRequest{Version: Version, TxnCoordMeta: &txnCoordMeta}
	req.Flow = distsqlpb.FlowSpec{
		Processors: []distsqlpb.ProcessorSpec{{
			Core: distsqlpb.ProcessorCoreUnion{TableReader: &ts},
			Post: post,
			Output: []distsqlpb.OutputRouterSpec{{
				Type:    distsqlpb.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE}},
			}},
		}},
	}

	distSQLClient := distsqlpb.NewDistSQLClient(conn)
	stream, err := distSQLClient.RunSyncFlow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&distsqlpb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
		t.Fatal(err)
	}

	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []distsqlpb.ProducerMetadata
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(context.TODO(), msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
	}
	metas = ignoreTxnCoordMeta(metas)
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
			version     distsqlpb.DistSQLVersion
			expectedErr string
		}{
			{
				version:     Version + 1,
				expectedErr: "version mismatch",
			},
			{
				version:     MinAcceptedVersion - 1,
				expectedErr: "version mismatch",
			},
			{
				version:     MinAcceptedVersion,
				expectedErr: "",
			},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%d", tc.version), func(t *testing.T) {
				distSQLClient := distsqlpb.NewDistSQLClient(conn)
				stream, err := distSQLClient.RunSyncFlow(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				req.Version = tc.version
				if err := stream.Send(&distsqlpb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
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
	defer s.Stopper().Stop(context.TODO())

	var v distsqlpb.DistSQLVersionGossipInfo
	if err := s.Gossip().GetInfoProto(
		gossip.MakeDistSQLNodeVersionKey(s.NodeID()), &v,
	); err != nil {
		t.Fatal(err)
	}

	if v.Version != Version || v.MinAcceptedVersion != MinAcceptedVersion {
		t.Fatalf("node is gossipping the wrong version. Expected: [%d-%d], got [%d-%d",
			Version, MinAcceptedVersion, v.Version, v.MinAcceptedVersion)
	}
}
