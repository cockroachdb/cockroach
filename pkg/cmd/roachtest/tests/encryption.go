// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func registerEncryption(r registry.Registry) {
	// Note that no workload is run in this roachtest: it's testing basic operations
	// and not performance. Performance tests of encryption are performed in other
	// tests based on the TestSpec.EncryptionSupport field.
	runEncryptionRotation := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		nodes := c.Spec().NodeCount
		httpClient := roachtestutil.DefaultHTTPClient(c, t.L())

		// Keys are configured in (key, old-key) pairs. We want to start out
		// unencrypted so we set both key and old-key to "plain" to avoid
		// special-casing the first iteration of the loop.
		keys := []string{"plain", "plain"}
		// Rotate through multiple keys with different key sizes and both
		// implementation versions.
		for _, size := range []int{128, 192, 256} {
			for _, version := range []int{1, 2} {
				filename := fmt.Sprintf("aes-%d-v%d.key", size, version)
				if err := c.RunE(ctx, option.WithNodes(c.Node(nodes)),
					fmt.Sprintf("./cockroach gen encryption-key -s %d --version=%d %s",
						size, version, filename)); err != nil {
					t.Fatal(errors.Wrapf(err, "failed to generate AES key"))
				}
				keys = append(keys, filename)
			}
		}

		for keyIdx := 1; keyIdx < len(keys); keyIdx++ {
			opts := option.DefaultStartOpts()
			opts.RoachprodOpts.ExtraArgs = []string{
				fmt.Sprintf("--enterprise-encryption=path=*,key=%s,old-key=%s",
					keys[keyIdx], keys[keyIdx-1]),
			}
			c.Start(ctx, t.L(), opts, install.MakeClusterSettings(), c.Range(1, nodes))
			roachtestutil.WaitForReady(ctx, t, c, c.Range(1, nodes))

			// Check that /_status/stores/local endpoint has encryption status.
			adminAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Range(1, nodes))
			if err != nil {
				t.Fatal(err)
			}
			for _, addr := range adminAddrs {
				url := fmt.Sprintf("http://%s/_status/stores/local", addr)
				var storesResponse serverpb.StoresResponse
				if err := httpClient.GetJSON(ctx, url, &storesResponse); err != nil {
					t.Fatal(err)
				}
				var encryptionStatus enginepb.EncryptionStatus
				if err := protoutil.Unmarshal(storesResponse.Stores[0].EncryptionStatus,
					&encryptionStatus); err != nil {
					t.Fatal(err)
				}
				if !strings.HasSuffix(encryptionStatus.ActiveStoreKey.Source, keys[keyIdx]) {
					t.Fatalf("expected active key %s, but found %s",
						keys[keyIdx], encryptionStatus.ActiveStoreKey.Source)
				}
			}

			for i := 1; i <= nodes; i++ {
				c.Stop(ctx, t.L(), option.NewStopOpts(option.Graceful(shutdownGracePeriod)), c.Node(i))
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
	}
}
