// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

func registerEncryption(r registry.Registry) {
	// Note that no workload is run in this roachtest because kv roachtest
	// ideally runs with encryption turned on to see the performance impact and
	// to test the correctness of encryption at rest.
	runEncryption := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, nodes))

		// Check that /_status/stores/local endpoint has encryption status.
		adminAddrs, err := c.InternalAdminUIAddr(ctx, t.L(), c.Range(1, nodes))
		if err != nil {
			t.Fatal(err)
		}
		for _, addr := range adminAddrs {
			if err := c.RunE(ctx, c.Node(nodes), fmt.Sprintf(`curl http://%s/_status/stores/local | (! grep '"encryptionStatus": null')`, addr)); err != nil {
				t.Fatalf("encryption status from /_status/stores/local endpoint is null")
			}
		}

		for i := 1; i <= nodes; i++ {
			c.Stop(ctx, t.L(), option.NewStopOpts(option.Graceful(shutdownGracePeriod)), c.Node(i))
		}

		// Restart node with encryption turned on to verify old key works.
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, nodes))

		testCLIGenKey := func(size int) error {
			// Generate encryption store key through `./cockroach gen encryption-key -s=size aes-size.key`.
			if err := c.RunE(ctx, c.Node(nodes), fmt.Sprintf("./cockroach gen encryption-key -s=%[1]d aes-%[1]d.key", size)); err != nil {
				return errors.Wrapf(err, "failed to generate AES key with size %d through CLI", size)
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
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("encryption/nodes=%d", n),
			EncryptionSupport: registry.EncryptionAlwaysEnabled,
			Leases:            registry.MetamorphicLeases,
			Owner:             registry.OwnerStorage,
			Cluster:           r.MakeClusterSpec(n),
			CompatibleClouds:  registry.AllExceptAWS,
			Suites:            registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runEncryption(ctx, t, c)
			},
		})
	}
}
