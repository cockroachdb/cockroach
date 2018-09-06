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
	gosql "database/sql"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "open a demo sql shell",
	Long: `
Start an in-memory, standalone, single-node CockroachDB instance, and open an
interactive SQL prompt to it. Various datasets are available to be preloaded as
subcommands: e.g. "cockroach demo startrek". See --help for a full list.
`,
	Example: `  cockroach demo`,
	Args:    cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
		return runDemo(cmd, nil /* gen */)
	}),
}

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()
		var genFlags *pflag.FlagSet
		if f, ok := gen.(workload.Flagser); ok {
			genFlags = f.Flags().FlagSet
		}

		genDemoCmd := &cobra.Command{
			Use:   meta.Name,
			Short: meta.Description,
			Args:  cobra.ArbitraryArgs,
			RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
				return runDemo(cmd, gen)
			}),
		}
		genDemoCmd.Flags().AddFlagSet(genFlags)
		demoCmd.AddCommand(genDemoCmd)
	}
}

func setupTransientServer(
	cmd *cobra.Command, gen workload.Generator,
) (connURL string, adminURL string, cleanup func(), err error) {
	cleanup = func() {}
	ctx := context.Background()

	df := cmd.Flags().Lookup(logflags.LogDirName)
	sf := cmd.Flags().Lookup(logflags.LogToStderrName)
	if !df.Changed && !sf.Changed {
		// User did not request logging flags; shut down logging under
		// errors and make logs appear on stderr.
		// Otherwise, the demo command would cause a cockroach-data
		// directory to appear in the current directory just for logs.
		_ = df.Value.Set("")
		df.Changed = true
		_ = sf.Value.Set(log.Severity_ERROR.String())
		sf.Changed = true
	}

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
	options.Add("application_name", sql.InternalAppNamePrefix+"cockroach demo")
	url := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     server.ServingAddr(),
		RawQuery: options.Encode(),
	}

	if gen != nil {
		meta := gen.Meta()
		url.Path = meta.Name
		db, err := gosql.Open("postgres", url.String())
		if err != nil {
			return ``, ``, nil, err
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE DATABASE ` + meta.Name); err != nil {
			return ``, ``, nil, err
		}

		ctx := context.TODO()
		const batchSize, concurrency = 0, 0
		if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
			return ``, ``, nil, err
		}
	}

	return url.String(), server.AdminURL(), cleanup, nil
}

func runDemo(cmd *cobra.Command, gen workload.Generator) error {
	connURL, adminURL, cleanup, err := setupTransientServer(cmd, gen)
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
