// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "context"

const goPath = `/mnt/data1/go`

// installGolang installs a specific version of Go on all nodes in
// "node".
func installGolang(ctx context.Context, t *test, c *cluster, node nodeListOption) {
	if err := repeatRunE(
		ctx, c, node, "download go", `curl -fsSL https://dl.google.com/go/go1.15.11.linux-amd64.tar.gz > /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, c, node, "verify tarball", `sha256sum -c - <<EOF
8825b72d74b14e82b54ba3697813772eb94add3abf70f021b6bdebe193ed01ec /tmp/go.tgz
EOF`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, c, node, "extract go", `sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, c, node, "force symlink go", "sudo ln -sf /usr/local/go/bin/go /usr/bin",
	); err != nil {
		t.Fatal(err)
	}
}
