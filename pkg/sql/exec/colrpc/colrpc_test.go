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

package colrpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockFlowStreamClient struct {
	pmChan chan *distsqlpb.ProducerMessage
	csChan chan *distsqlpb.ConsumerSignal
}

var _ flowStreamClient = mockFlowStreamClient{}

func (c mockFlowStreamClient) Send(m *distsqlpb.ProducerMessage) error {
	c.pmChan <- m
	return nil
}

func (c mockFlowStreamClient) Recv() (*distsqlpb.ConsumerSignal, error) {
	s := <-c.csChan
	if s == nil {
		return nil, io.EOF
	}
	return s, nil
}

func (c mockFlowStreamClient) CloseSend() error {
	close(c.pmChan)
	return nil
}

type mockFlowStreamServer struct {
	pmChan chan *distsqlpb.ProducerMessage
	csChan chan *distsqlpb.ConsumerSignal
}

func (s mockFlowStreamServer) Send(cs *distsqlpb.ConsumerSignal) error {
	s.csChan <- cs
	return nil
}

func (s mockFlowStreamServer) Recv() (*distsqlpb.ProducerMessage, error) {
	pm := <-s.pmChan
	if pm == nil {
		return nil, io.EOF
	}
	return pm, nil
}

var _ flowStreamServer = mockFlowStreamServer{}

// mockFlowStreamRPCLayer mocks out a bidirectional FlowStream RPC. The client
// and server simply send messages over channels and return io.EOF when these
// channels are closed. This RPC layer does not aim to implement more than that.
// Use MockDistSQLServer for more involved RPC behavior testing.
type mockFlowStreamRPCLayer struct {
	client mockFlowStreamClient
	server mockFlowStreamServer
}

func makeMockFlowStreamRPCLayer() mockFlowStreamRPCLayer {
	// Buffer channels to simulate non-blocking sends.
	pmChan := make(chan *distsqlpb.ProducerMessage, 16)
	csChan := make(chan *distsqlpb.ConsumerSignal, 16)
	return mockFlowStreamRPCLayer{
		client: mockFlowStreamClient{pmChan: pmChan, csChan: csChan},
		server: mockFlowStreamServer{pmChan: pmChan, csChan: csChan},
	}
}

// handleStream spawns a goroutine to call Inbox.RunWithStream with the
// provided stream and returns any error on the returned channel. handleStream
// will call doneFn if non-nil once the handler returns.
func handleStream(
	ctx context.Context, inbox *Inbox, stream flowStreamServer, doneFn func(),
) chan error {
	handleStreamErrCh := make(chan error, 1)
	go func() {
		handleStreamErrCh <- inbox.RunWithStream(ctx, stream)
		if doneFn != nil {
			doneFn()
		}
	}()
	return handleStreamErrCh
}

const staticNodeID roachpb.NodeID = 3

// TODO(asubiotto): Add draining and cancellation tests.
func TestOutboxInbox(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up the RPC layer.
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := distsqlrun.StartMockDistSQLServer(stopper, staticNodeID)
	require.NoError(t, err)

	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	client := distsqlpb.NewDistSQLClient(conn)
	clientStream, err := client.FlowStream(ctx)
	require.NoError(t, err)

	serverStreamNotification := <-mockServer.InboundStreams
	serverStream := serverStreamNotification.Stream

	// Do the actual testing.
	var (
		typs        = []types.T{types.Int64}
		rng, _      = randutil.NewPseudoRand()
		inputBuffer = exec.NewBatchBuffer()
	)

	input := exec.NewRandomDataOp(
		rng,
		// Test random selection as the Outbox should be deselecting before sending
		// over data. Nulls and types are not worth testing as those are tested in
		// colserde.
		exec.RandomDataOpArgs{
			DeterministicTyps: typs,
			Selection:         true,
			BatchAccumulator:  inputBuffer.Add,
		},
	)

	outbox, err := NewOutbox(input, typs)
	require.NoError(t, err)

	inbox, err := NewInbox(typs)
	require.NoError(t, err)

	streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

	var (
		canceled uint32
		wg       sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, clientStream, func() { atomic.StoreUint32(&canceled, 1) })
		wg.Done()
	}()

	// Use a deselector op to verify that the Outbox gets rid of the selection
	// vector.
	inputBatches := exec.NewDeselectorOp(inputBuffer, typs)
	inputBatches.Init()
	outputBatches := exec.NewBatchBuffer()
	for batchNum := 0; ; batchNum++ {
		outputBatch := inbox.Next(ctx)
		// Copy batch since it's not safe to reuse after calling Next.
		batchCopy := coldata.NewMemBatchWithSize(typs, int(outputBatch.Length()))
		for i := range typs {
			batchCopy.ColVec(i).Append(outputBatch.ColVec(i), typs[i], 0, outputBatch.Length())
		}
		batchCopy.SetLength(outputBatch.Length())
		outputBatches.Add(batchCopy)

		if outputBatch.Length() == 0 {
			break
		}
	}

	// Wait for the Inbox to return.
	require.NoError(t, <-streamHandlerErrCh)
	// Wait for the Outbox to return.
	wg.Wait()
	// Verify that the Outbox terminated gracefully.
	require.True(t, atomic.LoadUint32(&canceled) == 0)

	for batchNum := 0; ; batchNum++ {
		inputBatch := inputBatches.Next(ctx)
		outputBatch := outputBatches.Next(ctx)
		for i := range typs {
			require.Equal(
				t,
				inputBatch.ColVec(i).Slice(typs[i], 0, uint64(inputBatch.Length())),
				outputBatch.ColVec(i).Slice(typs[i], 0, uint64(outputBatch.Length())),
				"batchNum: %d", batchNum,
			)
		}
		if outputBatch.Length() == 0 {
			break
		}
	}
}
