// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestReplicaUnavailableError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var repls roachpb.ReplicaSet
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 10, ReplicaID: 100})
	repls.AddReplica(roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 20, ReplicaID: 200})
	desc := roachpb.NewRangeDescriptor(10, roachpb.RKey("a"), roachpb.RKey("z"), repls)
	lm := livenesspb.IsLiveMap{
		1: livenesspb.IsLiveMapEntry{IsLive: true},
	}
	ts := hlc.Timestamp{WallTime: time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC).UnixNano()}
	wrappedErr := errors.New("probe failed")
	rs := raft.Status{}
	ctx := context.Background()

	rue := errors.Mark(
		replicaUnavailableError(wrappedErr, desc, desc.Replicas().Descriptors()[0], lm, &rs, ts),
		circuit.ErrBreakerOpen)

	// A Protobuf roundtrip retains the error details.
	err := errors.DecodeError(ctx, errors.EncodeError(ctx, rue))
	require.True(t, errors.Is(err, wrappedErr), "%+v", err)
	require.True(t, errors.Is(err, circuit.ErrBreakerOpen))
	echotest.Require(t, string(redact.Sprint(err)), datapathutils.TestDataPath(t, "replica_unavailable_error.txt"))

	// When wrapped as a kvpb.NewError(), GetDetail() does not retain the error
	// mark, but GoError() does.
	pErr := kvpb.NewError(rue)
	err = pErr.GetDetail().(*kvpb.ReplicaUnavailableError)
	require.True(t, errors.Is(err, wrappedErr), "%+v", err)
	require.False(t, errors.Is(err, circuit.ErrBreakerOpen))

	err = pErr.GoError()
	require.True(t, errors.Is(err, wrappedErr), "%+v", err)
	require.True(t, errors.Is(err, circuit.ErrBreakerOpen))
}
