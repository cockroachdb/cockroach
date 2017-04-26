// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"fmt"
	"io"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	conn, err := s.RPCContext().GRPCDial(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}

	r := sqlutils.MakeSQLRunner(t, sqlDB)

	r.Exec(`CREATE DATABASE test`)
	r.Exec(`CREATE TABLE test.t (a INT PRIMARY KEY, b INT)`)
	r.Exec(`INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30)`)

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	ts := TableReaderSpec{
		Table:    *td,
		IndexIdx: 0,
		Reverse:  false,
		Spans:    []TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
	}
	post := PostProcessSpec{
		Filter:        Expression{Expr: "@1 != 2"}, // a != 2
		Projection:    true,
		OutputColumns: []uint32{0, 1}, // a
	}

	req := &SetupFlowRequest{Version: Version}
	req.Flow = FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &ts},
			Post: post,
			Output: []OutputRouterSpec{{
				Type:    OutputRouterSpec_PASS_THROUGH,
				Streams: []StreamEndpointSpec{{Type: StreamEndpointSpec_SYNC_RESPONSE}},
			}},
		}},
	}

	distSQLClient := NewDistSQLClient(conn)
	stream, err := distSQLClient.RunSyncFlow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&ConsumerSignal{SetupFlowRequest: req}); err != nil {
		t.Fatal(err)
	}

	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []ProducerMetadata
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
	}
	if len(metas) != 0 {
		t.Errorf("unexpected metadata: %v", metas)
	}
	str := rows.String()
	expected := "[[1 10] [3 30]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// Verify version handling.
	t.Run("version", func(t *testing.T) {
		testCases := []struct {
			version     uint32
			expectedErr string
		}{
			{
				version:     Version + 1,
				expectedErr: "version mismatch",
			},
			{
				version:     Version - 1,
				expectedErr: "version mismatch",
			},
			{
				version:     MinAcceptedVersion,
				expectedErr: "",
			},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%d", tc.version), func(t *testing.T) {
				distSQLClient := NewDistSQLClient(conn)
				stream, err := distSQLClient.RunSyncFlow(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				req.Version = tc.version
				if err := stream.Send(&ConsumerSignal{SetupFlowRequest: req}); err != nil {
					t.Fatal(err)
				}
				_, err = stream.Recv()
				if !testutils.IsError(err, tc.expectedErr) {
					t.Errorf("expected error '%s', got %v", tc.expectedErr, err)
				}
			})
		}
	})
}
