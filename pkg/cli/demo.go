// Copyright 2018 The Cockroach Authors.
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
	"net/url"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "open a demo sql shell",
	Long: `
Start an in-memory, standalone, single-node CockroachDB instance, and open an
interactive SQL prompt to it.
`,
	Example: `  cockroach demo`,
	Args:    cobra.NoArgs,
	RunE:    MaybeDecorateGRPCError(runDemo),
}

func setupTransientServer() (connURL string, adminURL string, cleanup func(), err error) {
	cleanup = func() {}
	ctx := context.Background()
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx)
	if err != nil {
		return connURL, adminURL, cleanup, err
	}
	cleanup = func() { stopper.Stop(ctx) }

	args := base.TestServerArgs{
		Insecure: true,
	}
	server := server.TestServerFactory.New(args).(*server.TestServer)
	if err := server.Start(args); err != nil {
		return connURL, adminURL, cleanup, err
	}
	prevCleanup := cleanup
	cleanup = func() { prevCleanup(); server.Stopper().Stop(ctx) }

	options := url.Values{}
	options.Add("sslmode", "disable")
	options.Add("application_name", "cockroach demo")
	url := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     server.ServingAddr(),
		RawQuery: options.Encode(),
	}

	return url.String(), server.AdminURL(), cleanup, nil
}

func runDemo(cmd *cobra.Command, _ []string) error {
	connURL, adminURL, cleanup, err := setupTransientServer()
	defer cleanup()
	if err != nil {
		return checkAndMaybeShout(err)
	}

	checkInteractive()

	if cliCtx.isInteractive {
		fmt.Printf(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB
# instance. Your changes will not be saved!
#
# Web UI: %s
#
`, adminURL)
	}

	conn := makeSQLConn(connURL)
	defer conn.Close()

	return runClient(cmd, conn)
}
