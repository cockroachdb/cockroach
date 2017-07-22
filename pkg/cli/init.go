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
//
// Author: Adam Gee (adamgee@gmail.com)

package cli

import (
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a cluster",
	Long: `
Perform one-time-only initialization of a CockroachDB cluster.

After starting one or more nodes with --join flags, run the init
command on one of them (passing the same --host and certificate flags
you would use for the sql command). A node started without the --join
flag initializes itself as a single-node cluster, so the init command
is not used in that case.
`,
	RunE: MaybeShoutError(MaybeDecorateGRPCError(runInit)),
}

func runInit(cmd *cobra.Command, args []string) error {
	c, stopper, err := getInitClient()
	if err != nil {
		return err
	}
	ctx := stopperContext(stopper)
	defer stopper.Stop(ctx)

	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
		log.Error(ctx, err)
		return err
	}

	return nil
}

func getInitClient() (serverpb.InitClient, *stop.Stopper, error) {
	// TODO(adam): This depends on servercfg which is a bit weird..
	conn, _, stopper, err := getClientGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return serverpb.NewInitClient(conn), stopper, nil
}
