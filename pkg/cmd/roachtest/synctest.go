// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
)

func registerSyncTest(r *testRegistry) {
	const nemesisScript = `#!/usr/bin/env bash

if [[ $1 == "on" ]]; then
  charybdefs-nemesis --probability
else
  charybdefs-nemesis --clear
fi
`

	r.Add(testSpec{
		Name:       "synctest",
		MinVersion: "v19.1.0",
		// This test sets up a custom file system; we don't want the cluster reused.
		Cluster: makeClusterSpec(1, reuseNone()),
		Run: func(ctx context.Context, t *test, c *cluster) {
			n := c.Node(1)
			tmpDir, err := ioutil.TempDir("", "synctest")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.RemoveAll(tmpDir)
			}()
			nemesis := filepath.Join(tmpDir, "nemesis")

			if err := ioutil.WriteFile(nemesis, []byte(nemesisScript), 0755); err != nil {
				t.Fatal(err)
			}

			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, nemesis, "./nemesis")
			c.Run(ctx, n, "chmod +x nemesis")
			c.Run(ctx, n, "sudo umount {store-dir}/faulty || true")
			c.Run(ctx, n, "mkdir -p {store-dir}/{real,faulty} || true")
			t.Status("setting up charybdefs")

			if err := execCmd(ctx, t.l, roachprod, "install", c.makeNodes(n), "charybdefs"); err != nil {
				t.Fatal(err)
			}
			c.Run(ctx, n, "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real && chmod 777 {store-dir}/{real,faulty}")

			t.Status("running synctest")
			c.Run(ctx, n, "./cockroach debug synctest {store-dir}/faulty ./nemesis")
		},
	})
}
