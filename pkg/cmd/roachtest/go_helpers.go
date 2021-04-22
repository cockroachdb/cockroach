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

// installLatestGolang installs the latest version of Go on all nodes in
// "node".
func installLatestGolang(ctx context.Context, t *test, c *cluster, node nodeListOption) {
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
		ctx, c, node, "download go", `curl -fsSL https://dl.google.com/go/go1.15.11.linux-amd64.tar.gz > /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx, c, node, "install go", "sudo apt-get install -y golang-go",
	); err != nil {
		t.Fatal(err)
	}
}
