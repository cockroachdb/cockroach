// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestCodec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCodec := codec{}
	for _, test := range []struct {
		name       string
		msgBuilder func() interface{}
	}{
		{"rpc.PingRequest", func() interface{} { return &PingRequest{} }},
		{"raftpb.Message", func() interface{} { return &raftpb.Message{} }},
		{"grpc_health_v1.HealthCheckRequest", func() interface{} { return &grpc_health_v1.HealthCheckRequest{} }},
		{"roachpb.GetRequest", func() interface{} { return &roachpb.GetRequest{} }},
	} {
		t.Run(fmt.Sprintf("can call marshal and unmarhsal on %s", test.name), func(t *testing.T) {
			marshaled, err := testCodec.Marshal(test.msgBuilder())
			require.NoError(t, err, "marshal failed")
			err = testCodec.Unmarshal(marshaled, test.msgBuilder())
			require.NoError(t, err, "unmarshal failed")
		})
	}
}
