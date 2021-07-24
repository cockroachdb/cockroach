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
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugStatementBundleCmd = &cobra.Command{
	Use:     "statement-bundle [command]",
	Aliases: []string{"sb"},
	Short:   "run a cockroach debug statement-bundle tool command",
	Long: `
debug statement-bundle is a suite of tools for debugging and manipulating statement
bundles created with EXPLAIN ANALYZE (DEBUG).
`,
}

var statementBundleRecreateCmd = &cobra.Command{
	Use:   "recreate <stmt bundle zipdir>",
	Short: "recreate the statement bundle in a demo cluster",
	Long: `
Run the recreate tool to populate a demo cluster with the environment, schema,
and stats in an unzipped statement bundle directory.
`,
	Args: cobra.ExactArgs(1),
}

func init() {
	statementBundleRecreateCmd.RunE = MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		return runBundleRecreate(cmd, args)
	})
}

type statementBundle struct {
	env       []byte
	schema    []byte
	statement []byte
	stats     [][]byte
}

func loadStatementBundle(zipdir string) (*statementBundle, error) {
	ret := &statementBundle{}
	var err error
	ret.env, err = ioutil.ReadFile(filepath.Join(zipdir, "env.sql"))
	if err != nil {
		return ret, err
	}
	ret.schema, err = ioutil.ReadFile(filepath.Join(zipdir, "schema.sql"))
	if err != nil {
		return ret, err
	}
	ret.statement, err = ioutil.ReadFile(filepath.Join(zipdir, "statement.txt"))
	if err != nil {
		return ret, err
	}

	return ret, filepath.WalkDir(zipdir, func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "stats-") {
			return nil
		}
		f, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		ret.stats = append(ret.stats, f)
		return nil
	})
}

func runBundleRecreate(cmd *cobra.Command, args []string) error {
	zipdir := args[0]
	bundle, err := loadStatementBundle(zipdir)
	if err != nil {
		return err
	}

	closeFn, err := sqlCtx.Open(os.Stdin)
	if err != nil {
		return err
	}
	defer closeFn()
	ctx := context.Background()
	c, err := democluster.NewDemoCluster(ctx, &demoCtx,
		log.Infof,
		log.Warningf,
		log.Ops.Shoutf,
		func(ctx context.Context) (*stop.Stopper, error) {
			// Override the default server store spec.
			//
			// This is needed because the logging setup code peeks into this to
			// decide how to enable logging.
			serverCfg.Stores.Specs = nil
			return setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
		},
		getAdminClient,
		drainAndShutdown,
	)
	if err != nil {
		c.Close(ctx)
		return err
	}
	defer c.Close(ctx)

	initGEOS(ctx)

	if err := c.Start(ctx, runInitialSQL); err != nil {
		return CheckAndMaybeShout(err)
	}
	conn, err := sqlCtx.MakeConn(c.GetConnURL())
	if err != nil {
		return err
	}
	// Disable autostats collection, which will override the injected stats.
	if err := conn.Exec(`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`, nil); err != nil {
		return err
	}
	var initStmts = [][]byte{bundle.env, bundle.schema}
	initStmts = append(initStmts, bundle.stats...)
	for _, a := range initStmts {
		if err := conn.Exec(string(a), nil); err != nil {
			return errors.Wrapf(err, "failed to run %s", a)
		}
	}

	cliCtx.PrintfUnlessEmbedded(`#
# Statement bundle %s loaded.
# Autostats disabled.
#
# Statement was:
#
# %s
`, zipdir, bundle.statement)

	sqlCtx.ShellCtx.DemoCluster = c
	return sqlCtx.Run(conn)
}
