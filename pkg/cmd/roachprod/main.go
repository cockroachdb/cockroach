// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cli"
	"github.com/felixge/fgprof"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod [command] (flags)",
	Short: "roachprod tool for manipulating test clusters",
	Long: `roachprod is a tool for manipulating ephemeral test clusters, allowing easy
creating, destruction, starting, stopping and wiping of clusters along with
running load generators.

Examples:

  roachprod create local -n 3
  roachprod start local
  roachprod sql local:2 -- -e "select * from crdb_internal.node_runtime_info"
  roachprod stop local
  roachprod wipe local
  roachprod destroy local

The above commands will create a "local" 3 node cluster, start a cockroach
cluster on these nodes, run a sql command on the 2nd node, stop, wipe and
destroy the cluster.
`,
	Version:          "details:\n" + build.GetInfo().Long(),
	PersistentPreRun: cli.ValidateAndConfigure,
}

func maybeFGProf() (done func()) {
	if os.Getenv("ROACHPROD_FGPROF") != "true" {
		return func() {}
	}
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	w, err := os.Create(filepath.Join(dir, "fprof.pb.gz"))
	if err != nil {
		panic(err)
	}
	stop := fgprof.Start(w, fgprof.FormatPprof)
	return func() {
		if err := stop(); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "fgprof output written to %s\n", w.Name())
	}
}

func main() {
	defer maybeFGProf()()
	cli.Initialize(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
