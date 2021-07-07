// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerInconsistency(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "inconsistency",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(3),
		Run:     runInconsistency,
	})
}

func runInconsistency(ctx context.Context, t test.Test, c cluster.Cluster) {
	// With encryption on, our attempt below to manually introduce an inconsistency
	// will fail.
	c.EncryptDefault(false)

	nodes := c.Range(1, 3)
	c.Put(ctx, t.Cockroach(), "./cockroach", nodes)
	c.Start(ctx, nodes)

	{
		db := c.Conn(ctx, 1)
		// Disable consistency checks. We're going to be introducing an
		// inconsistency and wish for it to be detected when we've set up the test
		// to expect it.
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)
		if err != nil {
			t.Fatal(err)
		}
		WaitFor3XReplication(t, db)
		_, db = db.Close(), nil
	}

	// Stop the cluster "gracefully" by letting each node initiate a "hard
	// shutdown" This will prevent any potential problems in which data isn't
	// synced. It seems (remotely) possible (see #64602) that without this, we
	// sometimes let the inconsistency win by ending up replicating it to all
	// nodes. This has not been conclusively proven, though.
	//
	// First SIGINT initiates graceful shutdown, second one initiates a
	// "hard" (i.e. don't shed leases, etc) shutdown.
	c.Stop(ctx, nodes, option.StopArgs("--sig=2", "--wait=false"))
	c.Stop(ctx, nodes, option.StopArgs("--sig=2", "--wait=true"))

	// Write an extraneous transaction record to n1's engine. This means n1 should
	// ultimately be terminated by the consistency checker (as the other two nodes
	// agree).
	//
	// Raw KVs created via:
	//
	// func TestFoo(t *testing.T) { // pkg/storage/batch_test.go
	//   t.Errorf("hex:%x", EncodeKey(MVCCKey{
	//     Key: keys.TransactionKey(keys.LocalMax, uuid.Nil),
	//   }))
	//   for i := 0; i < 3; i++ {
	//     var m enginepb.MVCCMetadata
	//     var txn enginepb.TxnMeta
	//     txn.Key = []byte(fmt.Sprintf("fake transaction %d", i))
	//     var err error
	//     m.RawBytes, err = protoutil.Marshal(&txn)
	//     require.NoError(t, err)
	//     data, err := protoutil.Marshal(&m)
	//     require.NoError(t, err)
	//     t.Error(fmt.Sprintf("hex:%x", data))
	//   }
	// }
	c.Run(ctx, c.Node(1), "./cockroach debug pebble db set {store-dir} "+
		"hex:016b1202000174786e2d0000000000000000000000000000000000 "+
		"hex:120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20302a004a00")

	m := c.NewMonitor(ctx)
	// If the consistency check "fails to fail", the verbose logging will help
	// determine why.
	c.Start(ctx, nodes, option.StartArgs("--args='--vmodule=consistency_queue=5,replica_consistency=5,queue=5'"))
	m.Go(func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Minute):
		case <-ctx.Done():
		}
		return nil
	})

	time.Sleep(10 * time.Second) // wait for n1-n3 to all be known as live to each other

	// set an aggressive consistency check interval, but only now (that we're
	// reasonably sure all nodes are live, etc). This makes sure that the consistency
	// check runs against all three nodes. If it targeted only two nodes, a random
	// one would fatal - not what we want.
	{
		db := c.Conn(ctx, 2)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '10ms'`)
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}

	if err := m.WaitE(); err == nil {
		t.Fatal("expected a node to crash")
	}

	time.Sleep(20 * time.Second) // wait for liveness to time out for dead nodes

	db := c.Conn(ctx, 2)
	rows, err := db.Query(`SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live = false;`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one dead NodeID, got %v", ids)
	}
	const expr = "this.node.is.terminating.because.a.replica.inconsistency.was.detected"
	c.Run(ctx, c.Node(1), "grep "+
		expr+" "+"{log-dir}/cockroach.log")

	if err := c.StartE(ctx, c.Node(1)); err == nil {
		// NB: we can't easily verify the error because there's a lot of output
		// which isn't fully included in the error returned from StartE.
		t.Fatalf("node restart should have failed")
	}

	// roachtest checks that no nodes are down when the test finishes, but in this
	// case we have a down node that we can't restart. Remove the data dir, which
	// tells roachtest to ignore this node.
	c.Wipe(ctx, c.Node(1))
}
