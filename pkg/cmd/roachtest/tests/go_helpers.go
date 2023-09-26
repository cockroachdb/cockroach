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
	c.Run(ctx, node, `sudo apt-get -qq update`)

	// Install dependencies (go uses C bindings)
	c.Run(ctx, node, `sudo apt-get -qq install build-essential`)

	c.Run(ctx, node, `curl -fsSL https://dl.google.com/go/go1.20.8.linux-amd64.tar.gz > /tmp/go.tgz`)

	// Verify tarball
	c.Run(
		ctx, node, `sha256sum -c - <<EOF
cc97c28d9c252fbf28f91950d830201aa403836cbed702a05932e63f7f0c7bc4 /tmp/go.tgz
EOF`,
	)

	c.Run(ctx, node, `sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz`)

	// Force symlink go
	c.Run(ctx, node, "sudo ln -sf /usr/local/go/bin/go /usr/bin")
}
