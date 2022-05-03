// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/spf13/cobra"
)

var mtTestDirectorySvr = &cobra.Command{
	Use:   "test-directory",
	Short: "run a test directory service",
	Long: `
Run a test directory service that starts and manages tenant SQL instances as
processes on the local machine.

Use two dashes (--) to separate the test directory command's arguments from
the remaining arguments that specify the executable (and the arguments) that 
will be ran when starting each tenant.

For example:
cockroach mt test-directory --port 1234 -- cockroach mt start-sql --kv-addrs=:2222 --certs-dir=./certs --base-dir=./base
or 
cockroach mt test-directory --port 1234 -- bash -c ./tenant_start.sh 

test-directory command will always add the following arguments (in that order):
--sql-addr <addr/host>[:<port>] 
--http-addr <addr/host>[:<port>]
--tenant-id number
`,
	Args: nil,
	RunE: clierrorplus.MaybeDecorateError(runDirectorySvr),
}

func runDirectorySvr(cmd *cobra.Command, args []string) (returnErr error) {
	ctx := context.Background()
	serverCfg.Stores.Specs = nil

	stopper, shutdownLogging, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)
	defer shutdownLogging()

	tds, err := tenantdirsvr.New(stopper, args...)
	if err != nil {
		return err
	}

	listenPort, err := net.Listen(
		"tcp", fmt.Sprintf(":%d", testDirectorySvrContext.port),
	)
	if err != nil {
		return err
	}
	stopper.AddCloser(stop.CloserFn(func() { _ = listenPort.Close() }))
	return tds.Serve(listenPort)
}
