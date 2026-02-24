// Copyright 2025 The Cockroach Authors.
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

const NODE22_VERSION = "22.14.0"
const NODE22_AMD64_SHA256SUM = "9d942932535988091034dc94cc5f42b6dc8784d6366df3a36c4c9ccb3996f0c2"
const NODE22_ARM64_SHA256SUM = "8cf30ff7250f9463b53c18f89c6c606dfda70378215b2c905d0a9a8b08bd45e0"
const NODE22_TARBALL = "https://nodejs.org/dist/v" + NODE22_VERSION + "/node-v" + NODE22_VERSION + "-linux-%s.tar.gz"

type nodeOpts struct {
	withPnpm bool
}

// installNode22 installs Node.js 22.x LTS on the given node.
func installNode22(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption, opts nodeOpts,
) error {
	tarball := ""
	checksum := ""
	switch c.Architecture() {
	case vm.ArchAMD64:
		tarball = fmt.Sprintf(NODE22_TARBALL, "x64")
		checksum = NODE22_AMD64_SHA256SUM
	case vm.ArchARM64:
		tarball = fmt.Sprintf(NODE22_TARBALL, "arm64")
		checksum = NODE22_ARM64_SHA256SUM
	default:
		return fmt.Errorf("unsupported architecture: %s", c.Architecture())
	}

	if err := repeatRunE(
		ctx, t, c, node, "install node 22",
		fmt.Sprintf(`
set -euo pipefail && \
curl -fsSL -o node-linux.tar.gz --compressed "%s" && \
echo "%s node-linux.tar.gz" | sha256sum -c - && \
sudo tar -xzf "node-linux.tar.gz" -C /usr/local --strip-components=1 --no-same-owner && \
rm "node-linux.tar.gz"`, tarball, checksum,
		)); err != nil {
		return err
	}

	if opts.withPnpm {
		if err := repeatRunE(
			ctx, t, c, node, "install pnpm",
			`sudo npm i -g pnpm@9.15.5`,
		); err != nil {
			return err
		}
	}

	return nil
}
