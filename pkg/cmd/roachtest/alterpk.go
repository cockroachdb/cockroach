// Copyright 2020 The Cockroach Authors.
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
)

func registerAlterPK(r *testRegistry) {
	const numRows = 1000000
	const duration = 1 * time.Minute

	runAlterPK := func(ctx context.Context, t *test, c *cluster) {
		roachNodes := c.Range(1, c.spec.NodeCount-1)
		loadNode := c.Node(c.spec.NodeCount)
		t.Status("copying binaries")
		c.Put(ctx, cockroach, "./cockroach", roachNodes)
		c.Put(ctx, workload, "./workload", loadNode)

		t.Status("starting cockroach nodes")
		c.Start(ctx, t, roachNodes)

		initDone := make(chan struct{}, 1)
		pkChangeDone := make(chan struct{}, 1)

		m := newMonitor(ctx, c, roachNodes)
		m.Go(func(ctx context.Context) error {
			// Load up a relatively small dataset to perform a workload on.

			// Init the workload.
			cmd := fmt.Sprintf("./workload init bank --drop --rows %d {pgurl%s}", numRows, roachNodes)
			if err := c.RunE(ctx, loadNode, cmd); err != nil {
				t.Fatal(err)
			}
			initDone <- struct{}{}

			// Run the workload while the primary key change is happening.
			cmd = fmt.Sprintf("./workload run bank --duration=%s {pgurl%s}", duration, roachNodes)
			if err := c.RunE(ctx, loadNode, cmd); err != nil {
				t.Fatal(err)
			}
			// Wait for the primary key change to finish.
			<-pkChangeDone
			t.Status("starting second run of the workload after primary key change")
			// Run the workload after the primary key change occurs.
			if err := c.RunE(ctx, loadNode, cmd); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Wait for the initialization to finish. Once it's done,
			// sleep for some time, then alter the primary key.
			<-initDone
			time.Sleep(duration / 10)

			t.Status("beginning primary key change")
			db := c.Conn(ctx, roachNodes[0])
			defer db.Close()
			cmd := `
			SET experimental_enable_primary_key_changes=true;
			USE bank;
			ALTER TABLE bank ALTER COLUMN balance SET NOT NULL;
			ALTER TABLE bank ALTER PRIMARY KEY USING COLUMNS (id, balance)
			`
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				t.Fatal(err)
			}
			t.Status("primary key change finished")
			pkChangeDone <- struct{}{}
			return nil
		})
		m.Wait()
	}
	r.Add(testSpec{
		// TODO (rohany): update this setup if we want to add more workloads to this roachtest.
		Name: "alterpk",
		// Use a 4 node cluster -- 3 nodes will run cockroach, and the last will be the
		// workload driver node.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run:        runAlterPK,
	})
}
