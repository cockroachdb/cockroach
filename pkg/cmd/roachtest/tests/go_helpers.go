// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

const goPath = `/mnt/data1/go`

// installGolang installs a specific version of Go on all nodes in
// "node".
func installGolang(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption,
) {
	// Update apt-get
	if err := c.RunE(
		ctx, node, `sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}

	// Install dependencies (go uses C bindings)
	if err := c.RunE(
		ctx,
		node,
		`sudo apt-get -qq install build-essential`,
	); err != nil {
		t.Fatal(err)
	}

	// Download go
	if err := c.RunE(
		ctx, node, `curl -fsSL https://dl.google.com/go/go1.20.8.linux-amd64.tar.gz > /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}

	// Verify tarball
	if err := c.RunE(
		ctx, node, `sha256sum -c - <<EOF
cc97c28d9c252fbf28f91950d830201aa403836cbed702a05932e63f7f0c7bc4 /tmp/go.tgz
EOF`,
	); err != nil {
		t.Fatal(err)
	}

	//Extract go
	if err := c.RunE(
		ctx, node, `sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}

	// Force symlink go
	if err := c.RunE(
		ctx, node, "sudo ln -sf /usr/local/go/bin/go /usr/bin",
	); err != nil {
		t.Fatal(err)
	}
}
