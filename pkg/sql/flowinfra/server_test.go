// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func TestServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	conn := s.RPCClientConn(t, username.RootUserName())

	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `CREATE TABLE test.t (a INT PRIMARY KEY, b INT)`)
	r.Exec(t, `INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30)`)

	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "test", "t")

	ts := execinfrapb.TableReaderSpec{
		Reverse: false,
		Spans:   []roachpb.Span{td.PrimaryIndexSpan(s.Codec())},
	}
	if err := rowenc.InitIndexFetchSpec(
		&ts.FetchSpec, s.Codec(), td, td.GetPrimaryIndex(),
		[]descpb.ColumnID{1, 2}, // a b
	); err != nil {
		t.Fatal(err)
	}

	txn := kv.NewTxn(ctx, kvDB, srv.NodeID())
	leafInputState, err := txn.GetLeafTxnInputState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	req := &execinfrapb.SetupFlowRequest{
		Version:           execversion.Latest,
		LeafTxnInputState: leafInputState,
	}
	req.Flow = execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{{
			Core: execinfrapb.ProcessorCoreUnion{TableReader: &ts},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE}},
			}},
			ResultTypes: types.TwoIntCols,
		}},
	}

	rows, err := runLocalFlow(ctx, s, req)
	if err != nil {
		t.Fatal(err)
	}
	str := rows.String(types.TwoIntCols)
	expected := "[[1 10] [2 20] [3 30]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// Verify version handling.
	t.Run("version", func(t *testing.T) {
		testCases := []struct {
			version     execversion.V
			expectedErr string
		}{
			{
				version:     execversion.Latest + 1,
				expectedErr: "version mismatch",
			},
			{
				version:     execversion.MinAccepted - 1,
				expectedErr: "version mismatch",
			},
			// TODO(yuzefovich): figure out what setup to perform to simulate
			// running a flow with acceptable version on a remote node.
			// Currently, the flow is scheduled correctly, but then encounters a
			// panic in a separate goroutine because there is no RowReceiver set
			// up for the table reader.
			//{
			//	version:     execversion.MinAccepted,
			//	expectedErr: "",
			//},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%d", tc.version), func(t *testing.T) {
				req := *req
				req.Version = tc.version
				distSQLClient := execinfrapb.NewDistSQLClient(conn)
				resp, err := distSQLClient.SetupFlow(ctx, &req)
				if err == nil && resp.Error != nil {
					err = resp.Error.ErrorDetail(ctx)
				}
				if !testutils.IsError(err, tc.expectedErr) {
					t.Errorf("expected error '%s', got %v", tc.expectedErr, err)
				}
			})
		}
	})
}

// runLocalFlow takes in a SetupFlowRequest to setup a local sync flow that is
// then run to completion. The result rows are returned. All metadata except for
// errors is ignored.
func runLocalFlow(
	ctx context.Context, s serverutils.ApplicationLayerInterface, req *execinfrapb.SetupFlowRequest,
) (rowenc.EncDatumRows, error) {
	evalCtx := eval.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)
	var rowBuf distsqlutils.RowBuffer
	flowCtx, flow, _, err := s.DistSQLServer().(*distsql.ServerImpl).SetupLocalSyncFlow(ctx, evalCtx.TestingMon, req, &rowBuf, nil /* batchOutput */, distsql.LocalState{})
	if err != nil {
		return nil, err
	}
	flow.Run(flowCtx, false /* noWait */)
	flow.Cleanup(flowCtx)

	if !rowBuf.ProducerClosed() {
		return nil, errors.New("output not closed")
	}

	var rows rowenc.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if meta != nil {
			if meta.Err != nil {
				return nil, meta.Err
			}
			continue
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// runLocalFlow takes in a SetupFlowRequest to setup a local sync flow that is
// then run to completion. The result rows are returned. All metadata except for
// errors is ignored.
func runLocalFlowTenant(
	ctx context.Context, s serverutils.ApplicationLayerInterface, req *execinfrapb.SetupFlowRequest,
) (rowenc.EncDatumRows, error) {
	evalCtx := eval.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)
	var rowBuf distsqlutils.RowBuffer
	flowCtx, flow, _, err := s.DistSQLServer().(*distsql.ServerImpl).SetupLocalSyncFlow(ctx, evalCtx.TestingMon, req, &rowBuf, nil /* batchOutput */, distsql.LocalState{})
	if err != nil {
		return nil, err
	}
	flow.Run(flowCtx, false /* noWait */)
	flow.Cleanup(flowCtx)

	if !rowBuf.ProducerClosed() {
		return nil, errors.New("output not closed")
	}

	var rows rowenc.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if meta != nil {
			if meta.Err != nil {
				return nil, meta.Err
			}
			continue
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}
