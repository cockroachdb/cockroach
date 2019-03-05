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

package kv

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/kr/pretty"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestTransportMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	clients := []batchClient{
		{replica: rd1},
		{replica: rd2},
		{replica: rd3},
	}
	gt := grpcTransport{orderedClients: clients}

	verifyOrder := func(replicas []roachpb.ReplicaDescriptor) {
		file, line, _ := caller.Lookup(1)
		for i, bc := range gt.orderedClients {
			if bc.replica != replicas[i] {
				t.Fatalf("%s:%d: expected order %+v; got mismatch at index %d: %+v",
					file, line, replicas, i, bc.replica)
			}
		}
	}

	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Now replica 3.
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})

	// Advance the client index and move replica 3 back to front.
	gt.clientIndex++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.clientIndex != 0 {
		t.Fatalf("expected client index 0; got %d", gt.clientIndex)
	}

	// Advance the client index again and verify replica 3 can
	// be moved to front for a second retry.
	gt.clientIndex++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.clientIndex != 0 {
		t.Fatalf("expected client index 0; got %d", gt.clientIndex)
	}

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and move rd1 front; should be no change.
	gt.clientIndex++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and and move rd1 to front. Should move
	// client index back for a retry.
	gt.clientIndex++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})
	if gt.clientIndex != 1 {
		t.Fatalf("expected client index 1; got %d", gt.clientIndex)
	}

	// Advance client index once more; verify second retry.
	gt.clientIndex++
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})
	if gt.clientIndex != 1 {
		t.Fatalf("expected client index 1; got %d", gt.clientIndex)
	}
}

// TestSpanImport tests that the gRPC transport ingests trace information that
// came from gRPC responses (through the "snowball tracing" mechanism).
func TestSpanImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	metrics := makeDistSenderMetrics()
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}
	server := mockInternalClient{}
	// Let's spice things up and simulate an error from the server.
	expectedErr := "my expected error"
	server.pErr = roachpb.NewErrorf(expectedErr)

	recCtx, getRec, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
	defer cancel()

	server.tr = opentracing.SpanFromContext(recCtx).Tracer().(*tracing.Tracer)

	br, err := gt.sendBatch(recCtx, roachpb.NodeID(1), &server, roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !testutils.IsPError(br.Error, expectedErr) {
		t.Fatalf("expected err: %s, got: %q", expectedErr, br.Error)
	}
	expectedMsg := "mockInternalClient processing batch"
	if tracing.FindMsgInRecording(getRec(), expectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", expectedMsg)
	}
}

// mockInternalClient is an implementation of roachpb.InternalClient.
// It simulates aspects of how the Node normally handles tracing in gRPC calls.
type mockInternalClient struct {
	tr   *tracing.Tracer
	pErr *roachpb.Error
}

var _ roachpb.InternalClient = &mockInternalClient{}

// Batch is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) Batch(
	ctx context.Context, in *roachpb.BatchRequest, opts ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	sp := m.tr.StartRootSpan("mock", nil /* logTags */, tracing.RecordableSpan)
	defer sp.Finish()
	tracing.StartRecording(sp, tracing.SnowballRecording)
	ctx = opentracing.ContextWithSpan(ctx, sp)

	log.Eventf(ctx, "mockInternalClient processing batch")
	br := &roachpb.BatchResponse{}
	br.Error = m.pErr
	if rec := tracing.GetRecording(sp); rec != nil {
		br.CollectedSpans = append(br.CollectedSpans, rec...)
	}
	return br, nil
}

// RangeFeed is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeFeed(
	ctx context.Context, in *roachpb.RangeFeedRequest, opts ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported RangeFeed call")
}

func TestWithMarshalingDebugging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	exp := `batch size 23 -> 28 bytes
re-marshaled protobuf:
00000000  0a 0a 0a 00 12 06 08 00  10 00 18 00 12 0e 3a 0c  |..............:.|
00000010  0a 0a 1a 03 66 6f 6f 22  03 62 61 72              |....foo".bar|

original panic:  <nil>
`

	assert.PanicsWithValue(t, exp, func() {
		var ba roachpb.BatchRequest
		ba.Add(&roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: []byte("foo"),
			},
		})
		withMarshalingDebugging(ctx, ba, func() {
			ba.Requests[0].GetInner().(*roachpb.ScanRequest).EndKey = roachpb.Key("bar")
		})
	})
}

func TestMarshalDebugOutput(t *testing.T) {
	tick := "`"
	repros := []struct {
		desc, repro string
	}{
		{
			"txnKVfetcher 196->194 https://github.com/cockroachdb/cockroach/issues/34241#issuecomment-469317193",
			`
00000000  0a 9d 01 0a 00 12 06 08  04 10 04 18 03 18 47 2a  |..............G*|
00000010  87 01 0a 22 0a 10 d0 96  0f dd 5a a8 41 65 b0 b2  |..."......Z.Ae..|
00000020  01 80 af fa 3c 66 2a 0a  08 8a e7 e3 dc 8c 8e 9e  |....<f*.........|
00000030  c4 15 30 87 b0 05 12 07  73 71 6c 20 74 78 6e 2a  |..0.....sql txn*|
00000040  0a 08 8a e7 e3 dc 8c 8e  9e c4 15 32 0a 08 8a e7  |...........2....|
00000050  e3 dc 8c 8e 9e c4 15 3a  0a 08 8a b1 99 cb 8e 8e  |.......:........|
00000060  9e c4 15 42 0e 08 01 12  0a 08 8a e7 e3 dc 8c 8e  |...B............|
00000070  9e c4 15 42 0e 08 04 12  0a 08 e5 ed d3 ad 8d 8e  |...B............|
00000080  9e c4 15 42 10 08 05 12  0c 08 ab ee f5 ab 8d 8e  |...B............|
00000090  9e c4 15 10 0a 72 00 7a  00 40 8f 4e 50 01 58 01  |.....r.z.@.NP.X.|
000000a0  12 20 3a 1e 0a 1a 1a 0b  bd 89 fd 05 fb 76 e8 54  |. :..........v.T|
000000b0  ce 80 06 22 0b bd 89 fd  05 fb 76 e8 55 63 00 01  |..."......v.Uc..|
000000c0  20 01                                             | .|
`},
		{
			"tableWriterBase 239->241 https://github.com/cockroachdb/cockroach/issues/35393#issuecomment-469590471",
			`
00000000  0a bb 01 0a 00 12 06 08  01 10 01 18 02 18 f7 05  |................|
00000010  2a a7 01 0a 2e 0a 10 f4  9d 06 36 b8 dd 44 d9 a1  |*.........6..D..|
00000020  c3 8a 70 3a ce b7 0d 1a  06 c1 89 f7 04 1a 88 2a  |..p:...........*|
00000030  0c 08 e6 e9 8d 86 a9 9d  bb c4 15 10 02 30 a8 f4  |.............0..|
00000040  47 38 05 12 07 73 71 6c  20 74 78 6e 2a 0a 08 e8  |G8...sql txn*...|
00000050  f1 da e6 e0 9d bb c4 15  32 0a 08 ba b6 c3 e4 e9  |........2.......|
00000060  9c bb c4 15 3a 0a 08 ba  80 f9 d2 eb 9c bb c4 15  |....:...........|
00000070  42 10 08 01 12 0c 08 d5  f0 cc dc ed 9d bb c4 15  |B...............|
00000080  10 0d 42 0e 08 02 12 0a  08 fa ce 95 e6 e9 9c bb  |..B.............|
00000090  c4 15 42 0e 08 04 12 0a  08 f3 d7 9e c1 ed 9d bb  |..B.............|
000000a0  c4 15 42 0e 08 05 12 0a  08 9a f6 88 db ed 9d bb  |..B.............|
000000b0  c4 15 48 01 60 01 72 00  7a 00 58 01 68 01 12 31  |..H.` + tick + `.r.z.X.h..1|
000000c0  d2 01 2e 0a 21 1a 1d c3  8b f7 04 1a 8e f7 08 36  |....!..........6|
000000d0  12 7b 7e 6a 44 c1 21 48  af 9b 60 27 95 ae 52 a2  |.{~jD.!H..` + tick + `'..R.|
000000e0  4d 00 01 88 28 06 12 09  0a 05 c6 90 cb 1f 03 12  |M...(...........|
000000f0  00                                                |.|
`},
		{
			"txnKVFetcher 213->211 https://github.com/cockroachdb/cockroach/issues/35357#issue-416959519 #1",
			`
00000000  0a b7 01 0a 00 12 06 08  03 10 03 18 02 18 ba f1  |................|
00000010  01 2a a2 01 0a 2d 0a 10  54 47 61 8b 36 42 45 25  |.*...-..TGa.6BE%|
00000020  aa 1e e0 8b c5 db f2 6d  1a 07 c1 89 f7 24 4d 89  |.......m.....$M.|
00000030  88 2a 0a 08 ed a8 98 ee  82 f4 b5 c4 15 30 f2 a1  |.*...........0..|
00000040  1e 38 01 12 07 73 71 6c  20 74 78 6e 2a 0a 08 ed  |.8...sql txn*...|
00000050  a8 98 ee 82 f4 b5 c4 15  32 0a 08 ed a8 98 ee 82  |........2.......|
00000060  f4 b5 c4 15 3a 0a 08 ed  f2 cd dc 84 f4 b5 c4 15  |....:...........|
00000070  42 0e 08 01 12 0a 08 ad  c7 80 d1 8c f4 b5 c4 15  |B...............|
00000080  42 0e 08 02 12 0a 08 a1  d6 ea cf 8c f4 b5 c4 15  |B...............|
00000090  42 0e 08 03 12 0a 08 be  91 fc ea 8c f4 b5 c4 15  |B...............|
000000a0  42 0e 08 04 12 0a 08 d1  c4 93 d0 8c f4 b5 c4 15  |B...............|
000000b0  48 01 72 00 7a 00 50 01  58 03 12 17 3a 15 0a 11  |H.r.z.P.X...:...|
000000c0  1a 05 bf 89 f7 21 12 22  06 bf 89 f7 21 12 fe 28  |.....!."....!..(|
000000d0  01 20 01                                          |. .|
`},
		{
			"tableWriter 219 -> 221 https://github.com/cockroachdb/cockroach/issues/35357#issue-416959519 #2",
			`
00000000  0a a7 01 0a 00 12 06 08  05 10 05 18 02 18 d5 0b  |................|
00000010  2a 93 01 0a 2c 0a 10 0c  86 ee d3 01 aa 4a 90 b1  |*...,........J..|
00000020  8d 41 07 ac 48 82 f4 1a  06 bd 89 f7 05 69 88 2a  |.A..H........i.*|
00000030  0a 08 8f ae d0 de 8e f4  b5 c4 15 30 ad 90 5d 38  |...........0..]8|
00000040  05 12 07 73 71 6c 20 74  78 6e 2a 0a 08 8f ae d0  |...sql txn*.....|
00000050  de 8e f4 b5 c4 15 32 0a  08 8f ae d0 de 8e f4 b5  |......2.........|
00000060  c4 15 3a 0a 08 8f f8 85  cd 90 f4 b5 c4 15 42 0e  |..:...........B.|
00000070  08 01 12 0a 08 a7 af 92  81 91 f4 b5 c4 15 42 10  |..............B.|
00000080  08 05 12 0c 08 e1 c8 a5  81 91 f4 b5 c4 15 10 08  |................|
00000090  42 0e 08 06 12 0a 08 d0  c4 92 df 8e f4 b5 c4 15  |B...............|
000000a0  48 01 72 00 7a 00 58 05  68 01 12 31 d2 01 2e 0a  |H.r.z.X.h..1....|
000000b0  21 1a 1d c2 8b f7 05 69  8f f7 0a c6 12 0d 0a 3c  |!......i.......<|
000000c0  70 6e 28 46 9e 8e ae 9c  27 5f 60 cb 0e 00 01 88  |pn(F....'_` + tick + `.....|
000000d0  28 06 12 09 0a 05 88 14  83 b8 03 12 00           |(............|
`},
	}

	for _, tc := range repros {
		t.Run(tc.desc, func(t *testing.T) {
			s := bufio.NewScanner(strings.NewReader(strings.TrimSpace(tc.repro)))
			s.Split(bufio.ScanLines)

			var enc string
			for s.Scan() {
				line := s.Text()[10:]
				line = line[:len(line)-20]
				line = strings.Replace(line, " ", "", -1)
				enc += line
			}
			b, err := hex.DecodeString(enc)
			assert.NoError(t, err)
			var ba roachpb.BatchRequest
			assert.NoError(t, protoutil.Unmarshal(b, &ba))
			t.Log(pretty.Sprint(ba.Size()))
			hdr := ba.Requests[0].GetInner().Header()
			t.Logf("[%s, %s)", hdr.Key, hdr.EndKey)
			t.Log(pretty.Sprint(ba))
		})
	}
}
