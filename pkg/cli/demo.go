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

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
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

	// Set up logging. For demo/transient server we use non-standard
	// behavior where we avoid file creation if possible.
	df := startCtx.logDirFlag
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

	// Set up the default zone configuration. We are using an in-memory store
	// so we really want to disable replication.
	cfg := config.DefaultZoneConfig()
	cfg.NumReplicas = proto.Int32(1)

	// TODO(benesch): should this use TestingSetDefaultZone config instead?
	restoreCfg := config.TestingSetDefaultSystemZoneConfig(cfg)
	prevCleanup := cleanup
	cleanup = func() { prevCleanup(); restoreCfg() }

	// Create the transient server.
	args := base.TestServerArgs{
		Insecure: true,
	}
	server := server.TestServerFactory.New(args).(*server.TestServer)
	if err := server.Start(args); err != nil {
		return connURL, adminURL, cleanup, err
	}
	prevCleanup2 := cleanup
	cleanup = func() { prevCleanup2(); server.Stopper().Stop(ctx) }

	// Prepare the URL for use by the SQL shell.
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
		url.Path = gen.Meta().Name
	}
	urlStr := url.String()

	// If there is a load generator, create its database and load its
	// fixture.
	if gen != nil {
		db, err := gosql.Open("postgres", urlStr)
		if err != nil {
			return ``, ``, cleanup, err
		}
		defer db.Close()

		if _, err := db.Exec(`CREATE DATABASE ` + gen.Meta().Name); err != nil {
			return ``, ``, cleanup, err
		}

		ctx := context.TODO()
		const batchSize, concurrency = 0, 0
		if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
			return ``, ``, cleanup, err
		}
	}

	return urlStr, server.AdminURL(), cleanup, nil
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
