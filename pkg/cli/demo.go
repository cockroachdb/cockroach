// Copyright 2018 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
		demoCmd.AddCommand(genDemoCmd)
		genDemoCmd.Flags().AddFlagSet(genFlags)
	}
}

func setupTransientServers(
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
	sysCfg := config.DefaultSystemZoneConfig()

	if demoCtx.nodes < 3 {
		cfg.NumReplicas = proto.Int32(1)
		sysCfg.NumReplicas = proto.Int32(1)
	}

	// Create the first transient server. The others will join this one.
	args := base.TestServerArgs{
		PartOfCluster: true,
		Insecure:      true,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DefaultZoneConfigOverride:       &cfg,
				DefaultSystemZoneConfigOverride: &sysCfg,
			},
		},
		Stopper: stopper,
	}
	serverFactory := server.TestServerFactory
	s := serverFactory.New(args).(*server.TestServer)
	if err := s.Start(args); err != nil {
		return connURL, adminURL, cleanup, err
	}
	args.JoinAddr = s.ServingAddr()
	for i := 0; i < demoCtx.nodes-1; i++ {
		s := serverFactory.New(args).(*server.TestServer)
		if err := s.Start(args); err != nil {
			return connURL, adminURL, cleanup, err
		}
	}

	// Prepare the URL for use by the SQL shell.
	options := url.Values{}
	options.Add("sslmode", "disable")
	options.Add("application_name", sqlbase.ReportableAppNamePrefix+"cockroach demo")
	url := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     s.ServingAddr(),
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
		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			return ``, ``, cleanup, err
		}
	}

	return urlStr, s.AdminURL(), cleanup, nil
}

func runDemo(cmd *cobra.Command, gen workload.Generator) error {
	connURL, adminURL, cleanup, err := setupTransientServers(cmd, gen)
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
# cluster of %d node%s. Your changes will not be saved!
#
# Web UI: %s
#
`, demoCtx.nodes, util.Pluralize(int64(demoCtx.nodes)), adminURL)
	}

	checkTzDatabaseAvailability(context.Background())

	conn := makeSQLConn(connURL)
	defer conn.Close()

	return runClient(cmd, conn)
}
