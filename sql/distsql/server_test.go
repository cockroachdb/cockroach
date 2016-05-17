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

package distsql

import (
	"fmt"
	"io"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB, cleanup := sqlutils.SetupServer(t)
	defer cleanup()
	conn, err := s.RPCContext().GRPCDial(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
		CREATE DATABASE test;
		CREATE TABLE test.t (a INT PRIMARY KEY, b INT);
		INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30);
	`); err != nil {
		t.Fatal(err)
	}

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	ts := TableReaderSpec{
		Table:         *td,
		IndexIdx:      0,
		Reverse:       false,
		Spans:         nil,
		Filter:        Expression{Expr: "$0 != 2"}, // a != 2
		OutputColumns: []uint32{0, 1},              // a
	}

	txn := client.NewTxn(context.Background(), *kvDB)

	req := &SetupFlowsRequest{Txn: txn.Proto}
	req.Flows = []FlowSpec{{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &ts},
			Output: []OutputRouterSpec{{
				Type:    OutputRouterSpec_MIRROR,
				Streams: []StreamEndpointSpec{{Mailbox: &MailboxSpec{SimpleResponse: true}}},
			}},
		}},
	}}

	distSQLClient := NewDistSQLClient(conn)
	stream, err := distSQLClient.RunSimpleFlow(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		// TODO(radu): verify stream data
		fmt.Printf("%s\n", msg)
	}
}
