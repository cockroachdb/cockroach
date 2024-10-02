// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerInconsistency(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "inconsistency",
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run:              runInconsistency,
	})
}

func runInconsistency(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodes := c.Range(1, 3)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), nodes)

	{
		db := c.Conn(ctx, t.L(), 1)
		// Disable consistency checks. We're going to be introducing an
		// inconsistency and wish for it to be detected when we've set up the test
		// to expect it.
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)
		require.NoError(t, err)
		require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), db))
		require.NoError(t, db.Close())
	}

	// Stop the cluster "gracefully" by letting each node initiate a "hard
	// shutdown". This will prevent any potential problems in which data isn't
	// synced. It seems (remotely) possible (see #64602) that without this, we
	// sometimes let the inconsistency win by ending up replicating it to all
	// nodes. This has not been conclusively proven, though.
	//
	// First SIGINT initiates graceful shutdown, second one initiates a "hard"
	// (i.e. don't shed leases, etc) shutdown.
	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Wait = false
	stopOpts.RoachprodOpts.Sig = 2
	c.Stop(ctx, t.L(), stopOpts, nodes)
	stopOpts.RoachprodOpts.Wait = true
	c.Stop(ctx, t.L(), stopOpts, nodes)

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
	c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach debug pebble db set {store-dir} "+
		"hex:016b1202000174786e2d0000000000000000000000000000000000 "+
		"hex:120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20302a004a00")

	m := c.NewMonitor(ctx)
	// If the consistency check "fails to fail", the verbose logging will help
	// determine why.
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
		"--vmodule=consistency_queue=5,replica_consistency=5,queue=5")
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), nodes)
	m.Go(func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Minute):
		case <-ctx.Done():
		}
		return nil
	})

	time.Sleep(10 * time.Second) // wait for n1-n3 to all be known as live to each other

	// Set an aggressive consistency check interval, but only now (that we're
	// reasonably sure all nodes are live, etc). This makes sure that the consistency
	// check runs against all three nodes. If it targeted only two nodes, a random
	// one would fatal - not what we want.
	{
		db := c.Conn(ctx, t.L(), 2)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '10ms'`)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}

	require.Error(t, m.WaitE(), "expected a node to crash")
	time.Sleep(20 * time.Second) // wait for liveness to time out for dead nodes

	db := c.Conn(ctx, t.L(), 2)
	rows, err := db.Query(`SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live = false;`)
	require.NoError(t, err)
	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, ids, 1, "expected one dead NodeID")

	const expr = "This.node.is.terminating.because.a.replica.inconsistency.was.detected"
	c.Run(ctx, option.WithNodes(c.Node(1)), "grep "+expr+" {log-dir}/cockroach.log")

	// Make sure that every node creates a checkpoint.
	for n := 1; n <= 3; n++ {
		// Notes it in the log.
		const expr = "creating.checkpoint.*with.spans"
		c.Run(ctx, option.WithNodes(c.Node(n)), "grep "+expr+" {log-dir}/cockroach.log")
		// Creates at least one checkpoint directory (in rare cases it can be
		// multiple if multiple consistency checks fail in close succession), and
		// puts spans information into the checkpoint.txt file in it.
		c.Run(ctx, option.WithNodes(c.Node(n)), "find {store-dir}/auxiliary/checkpoints -name checkpoint.txt")
		// The checkpoint can be inspected by the tooling.
		c.Run(ctx, option.WithNodes(c.Node(n)), "./cockroach debug range-descriptors "+
			"$(find {store-dir}/auxiliary/checkpoints/* -maxdepth 0 -type d | head -n1)")
		c.Run(ctx, option.WithNodes(c.Node(n)), "./cockroach debug range-data --limit 10 "+
			"$(find {store-dir}/auxiliary/checkpoints/* -maxdepth 0 -type d | head -n1) 1")
	}

	// NB: we can't easily verify the error because there's a lot of output which
	// isn't fully included in the error returned from StartE.
	require.Error(t, c.StartE(
		ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1),
	), "node restart should have failed")

	// roachtest checks that no nodes are down when the test finishes, but in this
	// case we have a down node that we can't restart. Remove the data dir, which
	// tells roachtest to ignore this node.
	c.Wipe(ctx, c.Node(1))
}
