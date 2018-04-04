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
	RunE:    MaybeShoutError(MaybeDecorateGRPCError(runDemo)),
}

func runDemo(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	args := base.TestServerArgs{
		Insecure: true,
	}
	server := server.TestServerFactory.New(args).(*server.TestServer)
	if err := server.Start(args); err != nil {
		return err
	}
	defer server.Stopper().Stop(ctx)

	options := url.Values{}
	options.Add("sslmode", "disable")
	url := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     server.ServingAddr(),
		Path:     "demo",
		RawQuery: options.Encode(),
	}
	conn := makeSQLConn(url.String())
	defer conn.Close()

	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.ensureConn(); err != nil {
		return err
	}
	if err := conn.Exec("CREATE DATABASE demo", nil); err != nil {
		return err
	}
	cliCtx.isInteractive = true

	fmt.Println(`#
# Welcome to the CockroachDB demo database!
#
# You are connected to a temporary, in-memory CockroachDB instance. Your changes
# will not be saved.
#`)
	return runInteractive(conn)
}
