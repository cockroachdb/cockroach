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

const NODE_VERSION = "18.20.8"
const AMD64_SHA256SUM = "27a9f3f14d5e99ad05a07ed3524ba3ee92f8ff8b6db5ff80b00f9feb5ec8097a"
const ARM64_SHA256SUM = "2e3dfc51154e6fea9fc86a90c4ea8f3ecb8b60acaf7367c4b76691da192571c1"
const S390X_SHA256SUM = "6db3d48cabcb22f1f4af29633431b62d1040099a6e27182ad9f018c90f09d65b"
const NODE_TARBALL = "https://nodejs.org/dist/v" + NODE_VERSION + "/node-v" + NODE_VERSION + "-linux-%s.tar.gz"

type nodeOpts struct {
	withYarn  bool
	withLerna bool
	withMocha bool
}

func installNode18(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption, opts nodeOpts,
) error {

	// Check if node is already installed
	if err := c.RunE(
		ctx, option.WithNodes(node), `
sudo npm i -g npm && \
sudo find /usr/local/lib/node_modules/npm -type d -exec chmod 755 {} \; && \
sudo find /usr/local/lib/node_modules/npm -type f -exec chmod 644 {} \; && \
sudo find /usr/local/lib/node_modules/npm/bin -type f -exec chmod 755 {} \;
`,
	); err != nil {

		tarball := ""
		checksum := ""
		switch c.Architecture() {
		case vm.ArchAMD64:
			tarball = fmt.Sprintf(NODE_TARBALL, "x64")
			checksum = AMD64_SHA256SUM
		case vm.ArchARM64:
			tarball = fmt.Sprintf(NODE_TARBALL, "arm64")
			checksum = ARM64_SHA256SUM
		case vm.ArchS390x:
			tarball = fmt.Sprintf(NODE_TARBALL, "s390x")
			checksum = S390X_SHA256SUM
		default:
			return fmt.Errorf("unsupported architecture: %s", c.Architecture())
		}

		// We only support 18.x for now, so we hardcode the tarball checksums.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"add nodesource key and deb repository",
			fmt.Sprintf(`
set -euo pipefail && \
curl -fsSL -o node-linux.tar.gz --compressed "%s" && \
echo "%s node-linux.tar.gz" | sha256sum -c - && \
sudo tar -xzf "node-linux.tar.gz" -C /usr/local --strip-components=1 --no-same-owner && \
rm "node-linux.tar.gz" && \
sudo ln -s /usr/local/bin/node /usr/local/bin/nodejs`, tarball, checksum,
			)); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"update npm",
			`
sudo npm i -g npm
sudo find /usr/local/lib/node_modules/npm -type d -exec chmod 755 {} \;
sudo find /usr/local/lib/node_modules/npm -type f -exec chmod 644 {} \;
sudo find /usr/local/lib/node_modules/npm/bin -type f -exec chmod 755 {} \;
`,
		); err != nil {
			t.Fatal(err)
		}
	}

	if opts.withYarn {
		err := repeatRunE(
			ctx, t, c, node, "install yarn",
			`
sudo npm i -g yarn
sudo find /usr/local/lib/node_modules/yarn -type d -exec chmod 755 {} \;
sudo find /usr/local/lib/node_modules/yarn -type f -exec chmod 644 {} \;
sudo find /usr/local/lib/node_modules/yarn/bin -type f -exec chmod 755 {} \;`,
		)
		if err != nil {
			return err
		}
	}

	if opts.withLerna {
		err := repeatRunE(
			ctx, t, c, node, "install lerna", `
if [ "$(arch)" == "s390x" ]; then
	# s390x has issues with the postinstall script of lerna so we skip it.
	sudo npm i --ignore-scripts --g lerna
else
	sudo npm i --g lerna
fi
sudo find /usr/local/lib/node_modules/lerna -type d -exec chmod 755 {} \;
sudo find /usr/local/lib/node_modules/lerna -type f -exec chmod 644 {} \;`,
		)
		if err != nil {
			return err
		}
	}

	if opts.withMocha {
		err := repeatRunE(
			ctx, t, c, node, "install mocha", `
sudo npm i -g mocha
sudo find /usr/local/lib/node_modules/mocha -type d -exec chmod 755 {} \;
sudo find /usr/local/lib/node_modules/mocha -type f -exec chmod 644 {} \;
sudo find /usr/local/lib/node_modules/mocha/bin -type f -exec chmod 755 {} \;`,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
