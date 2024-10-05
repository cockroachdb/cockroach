// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

const goPath = `/mnt/data1/go`

// installGolang installs a specific version of Go on all nodes in
// "node".
func installGolang(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption,
) {
	if err := repeatRunE(
		ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
	); err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx,
		t,
		c,
		node,
		"install dependencies (go uses C bindings)",
		`sudo apt-get -qq install build-essential`,
	); err != nil {
		t.Fatal(err)
	}

	binary := "go1.21.12.linux-amd64.tar.gz"
	sha := "121ab58632787e18ae0caa8ae285b581f9470d0f6b3defde9e1600e211f583c5"
	if c.Architecture() == vm.ArchARM64 {
		binary = "go1.21.12.linux-arm64.tar.gz"
		sha = "94cb3ec4a1e08a00da55c33e63f725be91f10ba743907b5615ef34e54675ba2e"
	}

	if err := repeatRunE(
		ctx, t, c, node, "download go", fmt.Sprintf(`curl -fsSL https://dl.google.com/go/%s > /tmp/go.tgz`, binary),
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, t, c, node, "verify tarball", fmt.Sprintf(`sha256sum -c - <<EOF
%s /tmp/go.tgz
EOF`, sha),
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, t, c, node, "extract go", `sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz`,
	); err != nil {
		t.Fatal(err)
	}
	if err := repeatRunE(
		ctx, t, c, node, "force symlink go", "sudo ln -sf /usr/local/go/bin/go /usr/bin",
	); err != nil {
		t.Fatal(err)
	}
}
