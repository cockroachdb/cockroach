// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestLeaseTransferWithPipelinedWrite verifies that pipelined writes
// do not cause retry errors to be leaked to clients when the error
// can be handled internally. Pipelining dissociates a write from its
// caller, so the retries of internally-generated errors (specifically
// out-of-order lease indexes) must be retried below that level.
//
// This issue was observed in practice to affect the first insert
// after table creation with high probability.
func TestLeaseTransferWithPipelinedWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)

	// More than 30 iterations is flaky under stressrace on teamcity.
	for iter := 0; iter < 30; iter++ {
		log.Infof(ctx, "iter %d", iter)
		if _, err := db.ExecContext(ctx, "drop table if exists test"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, "create table test (a int, b int, primary key (a, b))"); err != nil {
			t.Fatal(err)
		}

		workerErrCh := make(chan error, 1)
		go func() {
			workerErrCh <- func() error {
				for i := 0; i < 1; i++ {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						return err
					}
					defer func() {
						if tx != nil {
							if err := tx.Rollback(); err != nil {
								log.Warningf(ctx, "error rolling back: %+v", err)
							}
						}
					}()
					// Run two inserts in a transaction to ensure that we have
					// pipelined writes that cannot be retried at the SQL layer
					// due to the first-statement rule.
					if _, err := tx.ExecContext(ctx, "INSERT INTO test (a, b) VALUES ($1, $2)", i, 1); err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO test (a, b) VALUES ($1, $2)", i, 2); err != nil {
						return err
					}
					if err := tx.Commit(); err != nil {
						return err
					}
					tx = nil
				}
				return nil
			}()
		}()

		// TODO(bdarnell): This test reliably reproduced the issue when
		// introduced, because table creation causes splits and repeated
		// table creation leads to lease transfers due to rebalancing.
		// This is a subtle thing to rely on and the test might become
		// more reliable if we ran more iterations in the worker goroutine
		// and added a second goroutine to explicitly transfer leases back
		// and forth.

		select {
		case <-time.After(15 * time.Second):
			// TODO(bdarnell): The test seems flaky under stress with a 5s
			// timeout. Why? I'm giving it a high timeout since hanging
			// isn't a failure mode we're particularly concerned about here,
			// but it shouldn't be taking this long even with stress.
			t.Fatal("timed out")
		case err := <-workerErrCh:
			if err != nil {
				t.Fatalf("worker failed: %+v", err)
			}
		}
	}
}

func TestLeaseCommandLearnerReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const voterStoreID, learnerStoreID roachpb.StoreID = 1, 2
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: voterStoreID, Type: roachpb.ReplicaTypeVoterFull(), ReplicaID: 1},
		{NodeID: 2, StoreID: learnerStoreID, Type: roachpb.ReplicaTypeLearner(), ReplicaID: 2},
	}
	desc := roachpb.RangeDescriptor{}
	desc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
	cArgs := CommandArgs{
		EvalCtx: (&MockEvalCtx{
			StoreID: voterStoreID,
			Desc:    &desc,
		}).EvalContext(),
		Args: &roachpb.TransferLeaseRequest{
			Lease: roachpb.Lease{
				Replica: replicas[1],
			},
		},
	}

	// Learners are not allowed to become leaseholders for now, see the comments
	// in TransferLease and RequestLease.
	_, err := TransferLease(ctx, nil, cArgs, nil)
	require.EqualError(t, err, `replica (n2,s2):2LEARNER of type LEARNER cannot hold lease`)

	cArgs.Args = &roachpb.RequestLeaseRequest{}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForUnknown = `cannot replace lease <empty> with <empty>: ` +
		`replica (n0,s0):? not found in r0:{-} [(n1,s1):1, (n2,s2):2LEARNER, next=0, gen=0]`
	require.EqualError(t, err, expForUnknown)

	cArgs.Args = &roachpb.RequestLeaseRequest{
		Lease: roachpb.Lease{
			Replica: replicas[1],
		},
	}
	_, err = RequestLease(ctx, nil, cArgs, nil)

	const expForLearner = `cannot replace lease <empty> ` +
		`with repl=(n2,s2):2LEARNER seq=0 start=0,0 exp=<nil>: ` +
		`replica (n2,s2):2LEARNER of type LEARNER cannot hold lease`
	require.EqualError(t, err, expForLearner)
}
