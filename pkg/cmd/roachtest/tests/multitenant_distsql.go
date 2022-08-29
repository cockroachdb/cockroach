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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantDistSQL(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "multitenant/distsql",
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultiTenantDistSQL(ctx, t, c)
		},
	})
}

func runMultiTenantDistSQL(
	ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.Node(1))

	const (
		tenantID           = 11
		tenantBaseHTTPPort = 8081
		tenantBaseSQLPort  = 26259
	)

	tenantHTTPPort := func(offset int) int {
		if c.IsLocal() {
			return tenantBaseHTTPPort + offset
		}
		return tenantBaseHTTPPort
	}
	tenantSQLPort := func(offset int) int {
		if c.IsLocal() {
			return tenantBaseSQLPort + offset
		}
		return tenantBaseSQLPort
	}

	_, err := c.Conn(ctx, t.L(), 1).Exec(`SELECT crdb_internal.create_tenant($1)`, tenantID)
	require.NoError(t, err)
	instance1 := createTenantNode(ctx, t, c, c.Node(1), tenantID, 2 /* node */, tenantHTTPPort(0), tenantSQLPort(0), createTenantOtherTenantIDs([]int{1}))
	defer instance1.stop(ctx, t, c)
	instance1.start(ctx, t, c, "./cockroach")

	instance2, err := newTenantInstance(ctx, instance1, t, c, 3 /* node */, tenantHTTPPort(1), tenantSQLPort(1))
	require.NoError(t, err)
	defer instance2.stop(ctx, t, c)
	instance2.start(ctx, t, c, "./cockroach")

	t.L().Printf("got to end")
}
