// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rafttransport

import (
	context "context"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type MockTransport struct {
	UnimplementedRaftTransportServer
}

var _ RaftTransportServer = (*MockTransport)(nil)

func (t *MockTransport) RaftMessageBatch(srv RaftTransport_RaftMessageBatchServer) error {
	req, err := srv.Recv()
	if err != nil {
		return err
	}

	var rangeID int64
	{
		var obj RaftMessageRequest
		for i := 0; i < req.ReqsLength(); i++ {
			req.Reqs(&obj, i)
			rangeID += obj.RangeId()
		}
	}

	b := flatbuffers.NewBuilder(0)
	RaftMessageBatchResponseStart(b)
	RaftMessageBatchResponseAddRangeId(b, rangeID)
	b.Finish(RaftMessageBatchResponseEnd(b))
	return srv.Send(b)
}

func TestTransport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	encoding.RegisterCodec(flatbuffers.FlatbuffersCodec{})

	ctx := context.Background()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	srv := grpc.NewServer()
	defer srv.Stop()
	RegisterRaftTransportServer(srv, &MockTransport{})
	go func() {
		_ = srv.Serve(ln)
	}()

	cc, err := grpc.Dial(
		ln.Addr().String(),
		grpc.WithDefaultCallOptions(grpc.CustomCodecCallOption{
			Codec: flatbuffers.FlatbuffersCodec{},
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer cc.Close()

	c, err := NewRaftTransportClient(cc).RaftMessageBatch(ctx)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(0)

	var msgos []flatbuffers.UOffsetT // msg offsets
	for _, s := range []string{
		"foo", "bar", "baz",
	} {
		data := b.CreateByteVector([]byte(s))
		RaftMessageRequestStart(b)
		RaftMessageRequestAddData(b, data)
		RaftMessageRequestAddRangeId(b, 123)
		msgos = append(msgos, RaftMessageRequestEnd(b))
	}

	RaftMessageBatchRequestStartReqsVector(b, len(msgos))
	for i := len(msgos) - 1; i >= 0; i-- {
		b.PrependUOffsetT(msgos[i])
	}
	msgs := b.EndVector(len(msgos))

	RaftMessageBatchRequestStart(b)
	RaftMessageBatchRequestAddReqs(b, msgs)
	b.Finish(RaftMessageBatchRequestEnd(b))

	err = c.Send(b)
	require.NoError(t, err)
	resp, err := c.Recv()
	require.NoError(t, err)
	t.Log(resp.RangeId())
}
