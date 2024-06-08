// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commands

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/spf13/cobra"
)

// GetRootCommand returns the root command
func GetRootCommand(_ context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "drtprod [command] (flags)",
		Short: "drtprod runs roachprod commands against DRT clusters",
		Long: `drtprod is a tool for manipulating drt clusters using roachprod,
allowing easy creating, destruction, controls and configurations of clusters.

Commands include:
  push-hosts: write the ips and pgurl files for a cluster to a node/cluster
  dns: update/create DNS entries in drt.crdb.io for a cluster
  create: a wrapper for the 'roachprod' with predefined specs for named clusters
  *: any other command is passed to roachprod, potentially with flags added
`,
		Version: "details:\n" + build.GetInfo().Long(),
	}
}
