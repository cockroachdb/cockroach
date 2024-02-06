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
	"fmt"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func registerEncryption(r registry.Registry) {
	// Note that no workload is run in this roachtest: it's testing basic operations
	// and not performance. Performance tests of encryption are performed in other
	// tests based on the TestSpec.EncryptionSupport field.
	runEncryptionRotation := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount
		// Rotate through two keys and all implementation versions. Also test changing key sizes through the rotation process
		if err := c.RunE(ctx, option.WithNodes(c.Node(nodes)), "./cockroach gen encryption-key -s 128 aes-128.key"); err != nil {
			t.Fatal(errors.Wrapf(err, "failed to generate AES key"))
		}
		if err := c.RunE(ctx, option.WithNodes(c.Node(nodes)), "./cockroach gen encryption-key -s 256 aes-256.key"); err != nil {
			t.Fatal(errors.Wrapf(err, "failed to generate AES key"))
		}

		keys := []string{"plain", "plain", "aes-128.key", "aes-256.key"}
		for keyIdx := 1; keyIdx < len(keys); keyIdx++ {
			for _, version := range []int{1, 2} {
				opts := option.DefaultStartOpts()
				opts.RoachprodOpts.ExtraArgs = []string{fmt.Sprintf("--enterprise-encryption=path=data,key=%s,old-key=%s,version=%d", keys[keyIdx], keys[keyIdx-1], version)}
				c.Start(ctx, t.L(), opts, install.MakeClusterSettings(), c.Range(1, nodes))
				WaitForReady(ctx, t, c, c.Range(1, nodes))

				// Check that /_status/stores/local endpoint has encryption status.
				adminAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Range(1, nodes))
				if err != nil {
					t.Fatal(err)
				}
				for _, addr := range adminAddrs {
					url := fmt.Sprintf("http://%s/_status/stores/local", addr)
					var storesResponse serverpb.StoresResponse
					if err := httputil.GetJSON(http.Client{}, url, &storesResponse); err != nil {
						t.Fatal(err)
					}
					var encryptionStatus enginepbccl.EncryptionStatus
					if err := protoutil.Unmarshal(storesResponse.Stores[0].EncryptionStatus, &encryptionStatus); err != nil {
						t.Fatal(err)
					}
					if !strings.HasSuffix(encryptionStatus.ActiveStoreKey.Source, keys[keyIdx]) {
						t.Fatalf("expected active key %s, but found %s", keys[keyIdx], encryptionStatus.ActiveStoreKey.Source)
					}
				}

				for i := 1; i <= nodes; i++ {
					if err := c.StopCockroachGracefullyOnNode(ctx, t.L(), i); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}

	runGenKey := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount
		testCLIGenKey := func(size int) error {
			// Generate encryption store key through `./cockroach gen encryption-key -s=size aes-size.key`.
			if err := c.RunE(ctx, option.WithNodes(c.Node(nodes)), fmt.Sprintf("./cockroach gen encryption-key -s=%[1]d aes-%[1]d.key", size)); err != nil {
				return errors.Wrapf(err, "failed to generate AES key with size %d through CLI", size)
			}

			// Check the size of generated aes key has expected size.
			if err := c.RunE(ctx, option.WithNodes(c.Node(nodes)), fmt.Sprintf(`size=$(wc -c <"aes-%d.key"); test $size -eq %d && exit 0 || exit 1`, size, 32+size/8)); err != nil {
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
			Name: fmt.Sprintf("encryption/rotation/nodes=%d", n),
			// This test controls encryption manually.
			EncryptionSupport: registry.EncryptionAlwaysDisabled,
			Leases:            registry.MetamorphicLeases,
			Owner:             registry.OwnerStorage,
			Cluster:           r.MakeClusterSpec(n),
			CompatibleClouds:  registry.AllExceptAWS,
			Suites:            registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runEncryptionRotation(ctx, t, c)
			},
		})
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf("encryption/gen-key/nodes=%d", n),
			EncryptionSupport: registry.EncryptionAlwaysEnabled,
			Leases:            registry.MetamorphicLeases,
			Owner:             registry.OwnerStorage,
			Cluster:           r.MakeClusterSpec(n),
			CompatibleClouds:  registry.AllExceptAWS,
			Suites:            registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runGenKey(ctx, t, c)
			},
		})
	}
}
