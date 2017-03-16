// Copyright 2017 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsqlrun

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestOutbox(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock server that the outbox will connect and push rows to.
	stopper := stop.NewStopper()
	defer stopper.Stop()
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		t.Fatal(err)
	}

	flowCtx := FlowCtx{
		evalCtx: parser.EvalContext{},
		rpcCtx:  newInsecureRPCContext(stopper),
	}
	flowID := FlowID{uuid.MakeV4()}
	streamID := StreamID(42)
	outbox := newOutbox(&flowCtx, addr.String(), flowID, streamID)
	var outboxWG sync.WaitGroup
	outboxWG.Add(1)
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.start(context.TODO(), &outboxWG)

	// Start a producer. It will send one row 0, then send rows -1 until a drain
	// request is observed, then send row 2 and some metadata.
	producerC := make(chan error)
	go func() {
		producerC <- func() error {
			row := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
					parser.NewDInt(parser.DInt(0))),
			}
			if consumerStatus := outbox.Push(row, ProducerMetadata{}); consumerStatus != NeedMoreRows {
				return errors.Errorf("expected status: %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Send rows until the drain request is observed.
			for {
				row = sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(
						sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
						parser.NewDInt(parser.DInt(-1))),
				}
				consumerStatus := outbox.Push(row, ProducerMetadata{})
				if consumerStatus == DrainRequested {
					break
				}
				if consumerStatus == ConsumerClosed {
					return errors.Errorf("consumer closed prematurely")
				}
			}

			// Now send another row that the outbox will discard.
			row = sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
					parser.NewDInt(parser.DInt(2))),
			}
			if consumerStatus := outbox.Push(row, ProducerMetadata{}); consumerStatus != DrainRequested {
				return errors.Errorf("expected status: %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Send some metadata.
			outbox.Push(nil /* row */, ProducerMetadata{Err: errors.Errorf("meta 0")})
			outbox.Push(nil /* row */, ProducerMetadata{Err: errors.Errorf("meta 1")})
			// Send the termination signal.
			outbox.ProducerDone()

			return nil
		}()
	}()

	// Wait for the outbox to connect the stream.
	streamNotification := <-mockServer.inboundStreams
	serverStream := streamNotification.stream

	// Consume everything that the outbox sends on the stream.
	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []ProducerMetadata
	drainSignalSent := false
	for {
		msg, err := serverStream.Recv()
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
		// Eliminate the "-1" rows, that were sent before the producer found out
		// about the draining.
		last := -1
		for i := 0; i < len(rows); i++ {
			if rows[i].String() != "[-1]" {
				last = i
				continue
			}
			for j := i; j < len(rows); j++ {
				if rows[j].String() == "[-1]" {
					continue
				}
				rows[i] = rows[j]
				i = j
				last = j
				break
			}
		}
		rows = rows[0 : last+1]

		// After we receive one row, we're going to ask the producer to drain.
		if !drainSignalSent && len(rows) > 0 {
			sig := ConsumerSignal{DrainRequest: &DrainRequest{}}
			if err := serverStream.Send(&sig); err != nil {
				t.Fatal(err)
			}
			drainSignalSent = true
		}
	}
	if err := <-producerC; err != nil {
		t.Fatalf("%+v", err)
	}

	if len(metas) != 2 {
		t.Fatalf("expected 2 metadata records, got: %v", metas[0].Err)
	}
	for i, m := range metas {
		if we, ok := m.Err.(*roachpb.RemoteDistSQLProducerError); !ok {
			t.Fatalf("expected RemoteDistSQLProducerError, got: %s", m.Err)
		} else {
			expectedStr := fmt.Sprintf("RemoteDistSQLProducerError: meta %d", i)
			if we.Error() != expectedStr {
				t.Fatalf("expected: %q, got: %q", expectedStr, we.Error())
			}
		}
	}
	str := rows.String()
	expected := "[[0]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// The outbox should shut down since the producer closed.
	outboxWG.Wait()
	// Signal the server to shut down the stream.
	streamNotification.donec <- nil
}

// Test that an outbox connects its stream as soon as possible (i.e. before
// receiving any rows). This is important, since there's a timeout on waiting on
// the server-side for the streams to be connected.
func TestOutboxInitializesStreamBeforeRecevingAnyRows(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		t.Fatal(err)
	}

	flowCtx := FlowCtx{
		evalCtx: parser.EvalContext{},
		rpcCtx:  newInsecureRPCContext(stopper),
	}
	flowID := FlowID{uuid.MakeV4()}
	streamID := StreamID(42)
	outbox := newOutbox(&flowCtx, addr.String(), flowID, streamID)

	var outboxWG sync.WaitGroup
	outboxWG.Add(1)
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.start(context.TODO(), &outboxWG)

	streamNotification := <-mockServer.inboundStreams
	serverStream := streamNotification.stream
	producerMsg, err := serverStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if producerMsg.Header == nil {
		t.Fatal("missing header")
	}
	if producerMsg.Header.FlowID != flowID || producerMsg.Header.StreamID != streamID {
		t.Fatalf("wrong header: %v", producerMsg)
	}

	// Signal the server to shut down the stream. This should also prompt the
	// outbox (the client) to terminate its loop.
	streamNotification.donec <- nil
	outboxWG.Wait()
}

// startMockDistSQLServer starts a MockDistSQLServer and returns the address on
// which it's listening.
func startMockDistSQLServer(stopper *stop.Stopper) (*MockDistSQLServer, net.Addr, error) {
	rpcContext := newInsecureRPCContext(stopper)
	server := rpc.NewServer(rpcContext)
	mock := newMockDistSQLServer()
	RegisterDistSQLServer(server, mock)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
	if err != nil {
		return nil, nil, err
	}
	return mock, ln.Addr(), nil
}

func newInsecureRPCContext(stopper *stop.Stopper) *rpc.Context {
	return rpc.NewContext(
		log.AmbientContext{},
		&base.Config{Insecure: true},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
	)
}

// MockDistSQLServer implements the DistSQLServer (gRPC) interface and allows
// clients to control the inbound streams.
type MockDistSQLServer struct {
	inboundStreams chan InboundStreamNotification
}

// InboundStreamNotification is the MockDistSQLServer's way to tell its clients
// that a new gRPC call has arrived and thus a stream has arrived. The rpc
// handler is blocked until donec is signaled.
type InboundStreamNotification struct {
	stream DistSQL_FlowStreamServer
	donec  chan<- error
}

// MockDistSQLServer implements the DistSQLServer interface.
var _ DistSQLServer = &MockDistSQLServer{}

func newMockDistSQLServer() *MockDistSQLServer {
	return &MockDistSQLServer{inboundStreams: make(chan InboundStreamNotification)}
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) RunSyncFlow(stream DistSQL_RunSyncFlowServer) error {
	panic("RunSyncFlow not implemented")
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) SetupFlow(
	_ context.Context, req *SetupFlowRequest,
) (*SimpleResponse, error) {
	return nil, nil
}

// FlowStream is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) FlowStream(stream DistSQL_FlowStreamServer) error {
	donec := make(chan error)
	ds.inboundStreams <- InboundStreamNotification{stream: stream, donec: donec}
	return <-donec
}
