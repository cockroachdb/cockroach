// Copyright 2022 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

func registerExample(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "example",
		Owner:           registry.OwnerSQLQueries,
		Timeout:         time.Hour * 1,
		RequiresLicense: true,
		Tags:            nil,
		Cluster:         r.MakeClusterSpec(1),
		NativeLibs:      registry.LibGEOS,
		Run:             runExample,
	})
}

func runExample(ctx context.Context, t test.Test, c cluster.Cluster) {

	// Set up the nodes in the cluster.
	t.L().Printf("uploading cockroach binary to nodes")
	c.Put(ctx, t.Cockroach(), "./cockroach")
	defer func() {
		t.L().Printf("wiping nodes")
		c.Wipe(ctx)
	}()

	// Start CockroachDB.
	t.L().Printf("starting cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	defer func() {
		t.L().Printf("stopping cockroach")
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
	}()

	// Open a connection to CockroachDB.
	t.L().Printf("connecting to cockroach node 1")
	conn := c.Conn(ctx, t.L(), 1)
	defer func() {
		t.L().Printf("closing connection to cockroach node 1")
		conn.Close()
	}()

	// Create tables.
	t.L().Printf("creating table")
	if _, err := conn.Exec("CREATE TABLE t (a INT PRIMARY KEY)"); err != nil {
		t.L().Printf("error while creating table: %v", err)
		return
	}

	// Load tables with initial data.
	t.L().Printf("inserting rows")
	if _, err := conn.Exec("INSERT INTO t VALUES (0), (1), (2), (3)"); err != nil {
		t.L().Printf("error while inserting rows: %v", err)
		return
	}

	// Execute queries and iterate over results.
	t.L().Printf("querying")
	rows, err := conn.Query("SELECT a FROM t WHERE a > 1")
	if err != nil {
		t.L().Printf("error while querying: %v", err)
		return
	}
	for rows.Next() {
		var a int
		if err := rows.Scan(&a); err != nil {
			t.L().Printf("error while iterating over results: %v", err)
			return
		}
		t.L().Printf("a: %v", a)
	}
}
