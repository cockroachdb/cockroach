// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// XXX: Test cases.
// - One replica waiting for split trigger, so dropping msg apps.
// - One replica has full receive queue.
// - One replica has full send queue.
// - Raft messages to one replica stuck, eventually it needs a snapshot.
// - Lease and/or raft leadership changes hands.
// - Leaseholder and raft leader are not colocated.
// - Raft commands get reproposed, either due to timeouts or not having the
//   right MLAI.

func TestBasicProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	numNodes := 3
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1), tc.Target(2))

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		si, err := tc.Server(i).GetStores().(*kvserver.Stores).GetStore(tc.Server(i).GetFirstStoreID())
		require.NoError(t, err)
		tc.Servers[i].RaftTransport().Listen(si.StoreID(),
			&unreliableRaftHandler{
				rangeID:            desc.RangeID,
				RaftMessageHandler: si,
				unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
					dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
						return false
						// XXX: Just want logging. Maybe introduce a dedicated
						// interceptor and use it in datadriven tests (with some
						// degree of determinism)?
					},
				},
			})
	}

	const bytes = 1 << 20 // 1 MiB
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromString(
		randutil.RandString(rng, bytes, randutil.PrintableKeyAlphabet),
	)
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(k, value))

	t.Log("txn start")
	require.NoError(t, tc.Server(0).DB().TxnWithAdmissionControl(
		ctx, roachpb.AdmissionHeader_FROM_SQL, admissionpb.HighPri,
		func(ctx context.Context, txn *kv.Txn) error {
			txn.Send(ctx, &ba)
			return nil
		},
	))
	t.Log("txn done")

	testutils.SucceedsSoon(t, func() error {
		iog := tc.Server(0).IOGrantCoordinator().(*admission.IOGrantCoordinator)
		wbcs := iog.TestingGetWriteBurstCapacities()
		if len(wbcs) != 3 {
			return fmt.Errorf("expected 3 flow token buckets, got %d", len(wbcs))
		}
		for ts, ftb := range wbcs {
			if ftb[0] != 16<<20 {
				return fmt.Errorf("expected 16 MiB of regular flow tokens for %s, got %s", ts, humanize.IBytes(uint64(ftb[0])))
			}
			if ftb[1] != 8<<20 {
				return fmt.Errorf("expected 8 MiB of regular flow tokens for %s, got %s", ts, humanize.IBytes(uint64(ftb[1])))
			}
		}
		return nil
	})
}

func TestProposalWithSingleFollowerDroppingMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k,
		tc.Target(1),
		tc.Target(2))

	const bytes = 1 << 20 // 1 MiB
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromString(
		randutil.RandString(rng, bytes, randutil.PrintableKeyAlphabet),
	)
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(k, value))

	desc, err := tc.LookupRange(k)
	require.NoError(t, err)

	s1, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	s2, err := tc.Server(1).GetStores().(*kvserver.Stores).GetStore(tc.Server(1).GetFirstStoreID())
	require.NoError(t, err)
	tc.Servers[1].RaftTransport().Listen(s2.StoreID(),
		&unreliableRaftHandler{
			rangeID:            desc.RangeID,
			RaftMessageHandler: s2,
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
					// Ignore everything from s1.
					return req.FromReplica.StoreID == s1.StoreID()
				},
			},
		})

	t.Log("txn start")
	for i := 0; i < 10; i++ {
		require.NoError(t, tc.Server(0).DB().TxnWithAdmissionControl(
			ctx, roachpb.AdmissionHeader_FROM_SQL, admissionpb.HighPri,
			func(ctx context.Context, txn *kv.Txn) error {
				txn.Send(ctx, &ba)
				return nil
			},
		))
		t.Logf("txn=%d done", i)
	}

	testutils.SucceedsSoon(t, func() error {
		iog := tc.Server(0).IOGrantCoordinator().(*admission.IOGrantCoordinator)
		wbcs := iog.TestingGetWriteBurstCapacities()
		if len(wbcs) != 3 {
			return fmt.Errorf("expected 3 flow token buckets, got %d", len(wbcs))
		}
		for ts, ftb := range wbcs {
			if ftb[0] != 16<<20 {
				return fmt.Errorf("expected 16 MiB of regular flow tokens for %s, got %s", ts, humanize.IBytes(uint64(ftb[0])))
			}
			if ftb[1] != 8<<20 {
				return fmt.Errorf("expected 8 MiB of regular flow tokens for %s, got %s", ts, humanize.IBytes(uint64(ftb[1])))
			}
		}
		return nil
	})
}
