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
		ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx,
		c,
		node,
		"install dependencies (go uses C bindings)",
		`sudo apt-get -qq install build-essential`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx, c, node, "download go", `curl -fsSL https://dl.google.com/go/go1.15.14.linux-amd64.tar.gz > /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, c, node, "verify tarball", `sha256sum -c - <<EOF
6f5410c113b803f437d7a1ee6f8f124100e536cc7361920f7e640fedf7add72d /tmp/go.tgz
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
