// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package cliccl

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

func init() {
	httpServerCmd := &cobra.Command{
		Use:   "file-serve <path>",
		Short: "start a simple static file HTTP server",
		Long: `
Runs a simple, static file HTTP server that supports GET, PUT and DELETE in <path>.
  - GET looks in <path> for requested path, returning its content, or a 404 if it does not exist.
  - PUT creates or replaces requested path in <path> with with the content of the request body.
  - DELETE deletes the requested path in <path> if it exists.

No HTTP request should be able to read or modify a file outside of <path>.

This implements the API required by the HTTP external storage option in BACKUP and RESTORE.`,
		RunE: cli.MaybeDecorateGRPCError(runHTTPServer),
	}
	flags := httpServerCmd.PersistentFlags()
	flags.StringVar(&httpServerPort, "listen", ":8082", "port (or host:port) to bind")

	utilCmds := &cobra.Command{
		Use:   "utils [command]",
		Short: "utility commands",
		Long:  `Commands for helpers and utilities.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cli.AddCmd(utilCmds)
	utilCmds.AddCommand(httpServerCmd)
}

var (
	httpServerPort string
)

func runHTTPServer(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if len(args) != 1 {
		if err := cmd.Usage(); err != nil {
			return err
		}
		return errors.New("must provide path")
	}
	base := filepath.Clean(args[0])

	if err := os.MkdirAll(base, 0700); err != nil {
		return errors.Wrap(err, "could not create base path")
	}

	log.Shout(ctx, log.Severity_DEFAULT,
		fmt.Sprintf("starting http static file server in %s, listening on %s", base, httpServerPort))

	return http.ListenAndServe(httpServerPort, storageccl.MakeHTTPFileServer(ctx, base))
}
