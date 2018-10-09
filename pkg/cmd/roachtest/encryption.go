// Copyright 2018 The Cockroach Authors.
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

	"github.com/pkg/errors"
)

func registerEncryption(r *registry) {
	// Note that no workload is run in this roachtest because kv roachtest
	// ideally runs with encryption turned on to see the performance impact and
	// to test the correctness of encryption at rest.
	runEncryption := func(ctx context.Context, t *test, c *cluster) {
		nodes := c.nodes
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Start(ctx, t, c.Range(1, nodes), startArgs("--encrypt"))

		// Check that /_status/stores/local endpoint has encryption status.
		for _, addr := range c.InternalAdminUIAddr(ctx, c.Range(1, nodes)) {
			if err := c.RunE(ctx, c.Node(nodes), fmt.Sprintf(`curl http://%s/_status/stores/local | (! grep '"encryptionStatus": null')`, addr)); err != nil {
				t.Fatalf("encryption status from /_status/stores/local endpoint is null")
			}
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --host=:"+port); err != nil {
				return err
			}
			c.Stop(ctx, c.Node(node))
			return nil
		}

		for i := 1; i <= nodes; i++ {
			if err := stop(i); err != nil {
				t.Fatal(err)
			}
		}

		// Restart node with encryption turned on to verify old key works.
		c.Start(ctx, t, c.Range(1, nodes), startArgs("--encrypt"))

		testCLIGenKey := func(size int) error {
			// Generate encryption store key through `./cockroach gen encryption-key -s=size aes-size.key`.
			if err := c.RunE(ctx, c.Node(nodes), fmt.Sprintf("./cockroach gen encryption-key -s=%[1]d aes-%[1]d.key", size)); err != nil {
				return errors.Errorf("failed to generate aes key with size %d through CLI, got err %s", size, err)
			}

			// Check the size of generated aes key has expected size.
			if err := c.RunE(ctx, c.Node(nodes), fmt.Sprintf(`size=$(wc -c <"aes-%d.key"); test $size -eq %d && exit 0 || exit 1`, size, 32+size/8)); err != nil {
				return errors.Errorf("expected aes-%d.key has size %d bytes, but got different size", size, 32+size/8)
			}

			return nil
		}

		// Check that CLI can generated key with specified sizes.
		for _, size := range []int{128, 192, 256} {
			if err := testCLIGenKey(size); err != nil {
				t.Fatal(err)
			}
		}

		// Check that CLI returns error if AES key size is incorrect.
		for _, size := range []int{20, 88, 90} {
			// Cannot check for specific error message from CLI because command
			// is run through roachprod and it will return exist status 1.
			if err := testCLIGenKey(size); err == nil {
				t.Fatalf("expected error from CLI gen encryption-key, but got nil")
			}
		}
	}

	for _, n := range []int{1} {
		r.Add(testSpec{
			Name:       fmt.Sprintf("encryption/nodes=%d", n),
			MinVersion: "v2.1.0",
			Nodes:      nodes(n),
			Stable:     true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				runEncryption(ctx, t, c)
			},
		})
	}
}
