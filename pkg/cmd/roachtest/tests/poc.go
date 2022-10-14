// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package tests

import (
	"context"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/stretchr/testify/require"
)

func registerPOC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "poc",
		Owner:   registry.OwnerTestEng,
		Cluster: r.MakeClusterSpec(5),
		Run:     runPOC,
	})
}

func runPOC(ctx context.Context, t test.Test, c cluster.Cluster) {
	const version = "v22.1.8"
	dbName := os.Getenv("POC_DB_NAME")
	require.NotEmpty(t, dbName)
	assets := os.Getenv("POC_ASSETS")
	require.NotEmpty(t, assets)

	crdbNodes := c.Range(1, 3)
	lbNode := c.Node(5)
	appNode := c.Node(4)
	c.Stage(ctx, t.L(), "release", version, "", crdbNodes)
	{
		settings := install.MakeClusterSettings()
		startOpts := option.DefaultStartOpts()
		c.Start(ctx, t.L(), startOpts, settings, crdbNodes)
	}

	db := sqlutils.MakeSQLRunner(c.Conn(ctx, t.L(), 1))
	db.Exec(t, `CREATE DATABASE $1`, dbName)

	for _, item := range []string{"client", "service", "Manifests", "docker-service"} {
		c.Put(ctx, filepath.Join(assets, item), item, appNode)
	}
	c.Put(ctx, filepath.Join(assets, "101222.sql"), "101222.sql", lbNode)
	c.Install(ctx, t.L(), lbNode, "haproxy")
	c.Run(ctx, lbNode, `./cockroach gen haproxy --url {pgurl:1} --out - | `+
		`sed -e 's/roundrobin/leastconn/ -e 's/4096/20000/ -e 's/bind :26257/bind :26000/' > haproxy.cfg`)
	c.Run(ctx, lbNode, `sudo systemd-run --unit poc-haproxy --same-dir haproxy -f haproxy.cfg`)
	// Sanity check haproxy while doing something useful - applying the migration.
	c.Run(ctx, lbNode, "./cockroach sql --insecure --host=localhost --port=26000 -f 101222.sql")

	c.PutString(ctx, `#!/bin/bash
set -euxo pipefail


sudo apt-get update
sudo apt-get install -qqy curl ca-certificates gnupg lsb-release

# TODO(tbg): do we need docker in this POC? Looks like we're just running executables.
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -qqy docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo chmod 666 /var/run/docker.sock

mv service/*.HttpApi service/HttpApi
chmod +x client/HttpLatencyTest service/HttpApi
`, "./setup.sh", 0755, appNode)
	c.Run(ctx, appNode, "./setup.sh")

	c.Run(ctx, appNode, "sudo", "systemd-run", "--unit", "HttpApi",
		"-E", `ASPNETCORE_URLS="http://localhost:5237"`,
		"-E", "ASPNETCORE_ENVIRONMENT=Development",
		"service/HttpApi",
	)

	c.Run(ctx, appNode, "sudo", "systemd-run", "client/HttpLatencyTest",
		"--host", "http://localhost:5237",
		"-d", "5",
		"--accounts", "10000",
		"-r", "60")

}
