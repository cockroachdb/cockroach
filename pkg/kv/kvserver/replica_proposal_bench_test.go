// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkReplicaProposal starts a single-voter store and repeatedly
// overwrites the same key with a large payload.
//
// It is intended to be sensitive to allocations and memory copies on the write
// path. It intentionally does not cover the SQL layer or the transport between
// kvcoord and kvserver. These add significant overheads and can be explored
// using `pkg/sql/tests.BenchmarkKV`.
func BenchmarkReplicaProposal(b *testing.B) {
	const kb = 1 << 10
	const mb = kb * kb
	for _, bytes := range []int64{
		256,
		512,
		1 * kb,
		256 * kb,
		512 * kb,
		1 * mb, // pebble max batch reuse limit is 1mb
		2 * mb,
	} {
		for _, withFollower := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%s,withFollower=%t", humanizeutil.IBytes(bytes), withFollower), func(b *testing.B) {
				runBenchmarkReplicaProposal(b, bytes, withFollower)
			})
		}
	}
}

func runBenchmarkReplicaProposal(b *testing.B, bytes int64, withFollower bool) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	nodes := 1
	if withFollower {
		nodes = 2
	}

	args := base.TestClusterArgs{}
	args.ReplicationMode = base.ReplicationManual
	tc := testcluster.StartTestCluster(b, nodes, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(b)

	if withFollower {
		// Two voters implies that the second node has to ack everything before it
		// goes through, so it won't trail behind. It might trail a bit in entry
		// application but we live with that.
		tc.AddNonVotersOrFatal(b, k, tc.Target(1))
	}

	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromString(
		randutil.RandString(rng, int(bytes), randutil.PrintableKeyAlphabet),
	)
	req := kvpb.NewPut(k, value)
	var ba kvpb.BatchRequest
	ba.Add(req)

	repl, _, err := tc.Server(0).GetStores().(*kvserver.Stores).GetReplicaForRangeID(
		ctx, tc.LookupRangeOrFatal(b, k).RangeID)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ba.Timestamp = repl.Clock().Now()
		_, pErr := repl.Send(ctx, &ba)
		if err := pErr.GoError(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.SetBytes(bytes)
}
