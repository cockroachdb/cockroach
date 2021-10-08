// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

func registerSecure(r registry.Registry) {
	for _, numNodes := range []int{1, 3} {
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("smoketest/secure/nodes=%d", numNodes),
			Tags:    []string{"smoketest", "weekly"},
			Owner:   registry.OwnerKV, // TODO: OwnerTestEng once the open PR that introduces it has merged
			Cluster: r.MakeClusterSpec(numNodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				c.Put(ctx, t.Cockroach(), "./cockroach")
				c.Start(ctx, option.StartArgs("--secure"))
				db := c.Conn(ctx, 1)
				defer db.Close()
				_, err := db.QueryContext(ctx, `SELECT 1`)
				require.NoError(t, err)
			},
		})
	}
}
