// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func registerInconsistency(r *testRegistry) {
	r.Add(testSpec{
		Name:       fmt.Sprintf("inconsistency"),
		Owner:      OwnerKV,
		MinVersion: "v19.2.2", // https://github.com/cockroachdb/cockroach/pull/42149 is new in 19.2.2
		Cluster:    makeClusterSpec(3),
		Run:        runInconsistency,
	})
}

func runInconsistency(ctx context.Context, t *test, c *cluster) {
	// With encryption on, our attempt below to manually introduce an inconsistency
	// will fail.
	c.encryptDefault = false

	nodes := c.Range(1, 3)
	c.Put(ctx, cockroach, "./cockroach", nodes)
	c.Start(ctx, t, nodes)

	{
		db := c.Conn(ctx, 1)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '10ms'`)
		if err != nil {
			t.Fatal(err)
		}
		waitForFullReplication(t, db)
		_, db = db.Close(), nil
	}

	c.Stop(ctx, nodes)

	// KV pair created via:
	//
	// t.Errorf("0x%x", EncodeKey(MVCCKey{
	// 	Key: keys.TransactionKey(keys.LocalMax, uuid.Nil),
	// }))
	// for i := 0; i < 3; i++ {
	// 	var m enginepb.MVCCMetadata
	// 	var txn enginepb.TxnMeta
	// 	txn.Key = []byte(fmt.Sprintf("fake transaction %d", i))
	// 	var err error
	// 	m.RawBytes, err = protoutil.Marshal(&txn)
	// 	require.NoError(t, err)
	// 	data, err := protoutil.Marshal(&m)
	// 	require.NoError(t, err)
	// 	t.Error(fmt.Sprintf("0x%x", data))
	// }
	//
	// Output:
	// 0x016b1202000174786e2d0000000000000000000000000000000000
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20302a004a00
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20312a004a00
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20322a004a00

	c.Run(ctx, c.Node(1), "./cockroach debug rocksdb put --hex --db={store-dir} "+
		"0x016b1202000174786e2d0000000000000000000000000000000000 "+
		"0x12040800100018002000280032280a10000000000000000000000000000000001a1066616b65207472616e73616374696f6e2a004a00")

	m := newMonitor(ctx, c)
	c.Start(ctx, t, nodes)
	m.Go(func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Minute):
		case <-ctx.Done():
		}
		return nil
	})
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
