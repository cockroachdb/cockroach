// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"
)

func registerSchemaChangeBulkIngest(r *registry) {
	r.Add(testSpec{
		Name:    "schemachange/bulkingest",
		Cluster: makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSchemaChangeBulkIngest(ctx, t, c)
		},
	})
}

func runSchemaChangeBulkIngest(ctx context.Context, t *test, c *cluster) {
	aNum := 100000000
	if c.isLocal() {
		aNum = 100000
	}
	bNum := 1
	cNum := 1
	payloadBytes := 4

	crdbNodes := c.Range(1, c.nodes-1)
	workloadNode := c.Node(c.nodes)

	c.Put(ctx, cockroach, "./cockroach", crdbNodes)
	c.Put(ctx, workload, "./workload", workloadNode)
	c.Start(ctx, t, crdbNodes, startArgs("--env=COCKROACH_IMPORT_WORKLOAD_FASTER=true"))

	cmdWrite := fmt.Sprintf(
		"./workload fixtures import bulkingest {pgurl:1} --a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
		aNum, bNum, cNum, payloadBytes,
	)

	c.Run(ctx, workloadNode, cmdWrite)

	m := newMonitor(ctx, c, crdbNodes)

	indexDuration := time.Minute * 20
	if c.isLocal() {
		indexDuration = time.Second * 30
	}
	cmdWriteAndRead := fmt.Sprintf(
		"./workload run bulkingest --duration %s {pgurl:1-%d} --a %d --b %d --c %d --payload-bytes %d",
		indexDuration.String(), c.nodes-1, aNum, bNum, cNum, payloadBytes,
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, workloadNode, cmdWriteAndRead)
		return nil
	})

	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		pauseDuration := time.Minute * 5
		if c.isLocal() {
			pauseDuration = time.Second * 10
		}
		time.Sleep(pauseDuration)
		c.l.Printf("Creating index")
		before := timeutil.Now()
		if _, err := db.Exec(`CREATE INDEX payload_a ON bulkingest.bulkingest (payload, a)`); err != nil {
			t.Fatal(err)
		}
		c.l.Printf("CREATE INDEX took %v\n", timeutil.Since(before))
		return nil
	})

	m.Wait()
}
