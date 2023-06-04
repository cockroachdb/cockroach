// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

func TestReplicaUnavailableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var repls roachpb.ReplicaSet
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 10, ReplicaID: 100})
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 20, ReplicaID: 200})
	desc := roachpb.NewRangeDescriptor(10, roachpb.RKey("a"), roachpb.RKey("z"), repls)
	nv := livenesspb.TestCreateNodeVitality(1)
	ts, err := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	require.NoError(t, err)
	wrappedErr := errors.New("probe failed")
	rs := raft.Status{}
	ctx := context.Background()
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, replicaUnavailableError(
		wrappedErr, desc, desc.Replicas().AsProto()[0], nv, &rs, hlc.Timestamp{WallTime: ts.UnixNano()}),
	))
	require.True(t, errors.Is(err, wrappedErr), "%+v", err)
	echotest.Require(t, string(redact.Sprint(err)), datapathutils.TestDataPath(t, "replica_unavailable_error.txt"))
}
