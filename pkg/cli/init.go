// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a cluster",
	Long: `
Perform one-time-only initialization of a CockroachDB cluster.

After starting one or more nodes with --join flags, run the init
command on one node (passing the same --host and certificate flags
you would use for the sql command). The target of the init command
must appear in the --join flags of other nodes.

A node started without the --join flag initializes itself as a
single-node cluster, so the init command is not used in that case.
`,
	Args: cobra.NoArgs,
	RunE: MaybeShoutError(MaybeDecorateGRPCError(runInit)),
}

func runInit(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, finish, err := getClientGRPCConn(ctx)
	if err != nil {
		return err
	}
	defer finish()

	c := serverpb.NewInitClient(conn)

	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
		return err
	}

	fmt.Fprintln(os.Stdout, "Cluster successfully initialized")
	return nil
}
