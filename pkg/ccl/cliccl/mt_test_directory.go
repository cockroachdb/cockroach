// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/cli"
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
	stopper, err := cli.ClearStoresAndSetupLoggingForMTCommands(cmd, ctx)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

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
