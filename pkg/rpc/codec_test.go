// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"reflect"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestCodecMarshalUnmarshal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCodec := codec{}
	for _, test := range []struct {
		name             string
		filledMsgBuilder func() interface{}
		emptyMsgBuilder  func() interface{}
	}{
		{"rpc.PingRequest",
			func() interface{} { return &PingRequest{Ping: "pong"} },
			func() interface{} { return &PingRequest{} }},
		{"raftpb.Message",
			func() interface{} {
				return &raftpb.Message{
					To:   531,
					From: 550,
				}
			},
			func() interface{} { return &raftpb.Message{} }},
		{"grpc_health_v1.HealthCheckRequest",
			func() interface{} {
				return &grpc_health_v1.HealthCheckRequest{
					Service: "wombats",
				}
			},
			func() interface{} { return &grpc_health_v1.HealthCheckRequest{} }},
		{"roachpb.GetRequest",
			func() interface{} {
				return &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: roachpb.Key("turtle"),
					},
				}
			},
			func() interface{} { return &roachpb.GetRequest{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := test.filledMsgBuilder()
			t.Logf("marshaling")
			marshaled, err := testCodec.Marshal(input)
			require.NoError(t, err, "marshal failed")
			output := test.emptyMsgBuilder()
			t.Logf("unmarshaling")
			err = testCodec.Unmarshal(marshaled, output)
			require.NoError(t, err, "unmarshal failed")
			// reflect.DeepEqual/require.Equal can fail
			// because of XXX_sizecache fields.
			//
			// google's proto Equal doesn't understand all
			// gogoproto generated types and panics.
			//
			// gogoproto's proto Equal fails because of
			// https://github.com/gogo/protobuf/issues/13
			//
			// Here, we zero any top-level fields that
			// start with XXX_ and then use require.Equal
			// (which uses require.DeepEqual). I doubt
			// this would work for the general case, but
			// it works for the protobufs tested here.
			//
			// Additionally, in the newer grpc versions, Some non-exported
			// fields (e.g. .state) are being set both during marshaling and
			// unmarshaling. These are non-comparable. So we re-construct
			// reference structs by only copying public, non-xXX fields.
			t.Logf("comparing")
			input2 := test.emptyMsgBuilder()
			output2 := test.emptyMsgBuilder()
			copyPublicFields(input2, input)
			copyPublicFields(output2, output)
			require.Equal(t, input2, output2)
		})
	}
}

func copyPublicFields(dst, src interface{}) {
	srcval := reflect.Indirect(reflect.ValueOf(src))
	dstval := reflect.Indirect(reflect.ValueOf(dst))
	typ := srcval.Type()
	for i := 0; i < srcval.NumField(); i++ {
		fname := typ.Field(i).Name
		if unicode.IsUpper(rune(fname[0])) && !strings.HasPrefix(fname, "XXX_") {
			dstval.Field(i).Set(srcval.Field(i))
		}
	}
}
