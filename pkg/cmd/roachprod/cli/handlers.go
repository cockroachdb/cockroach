// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package cli

import (
	"fmt"
	"os"
	"os/user"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/update"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/cobra"
)

// Initialize sets up and initializes the command-line interface.
func Initialize(rootCmd *cobra.Command) {
	_ = roachprod.InitProviders()
	providerOptsContainer = vm.CreateProviderOptionsContainer()
	// The commands are displayed in the order they are added to rootCmd. Note
	// that gcCmd and adminurlCmd contain a trailing \n in their Short help in
	// order to separate the commands into logical groups.
	cobra.EnableCommandSorting = false
	syncCmd := getSyncCmd(rootCmd)
	rootCmd.AddCommand(syncCmd)
	rootCmd.AddCommand(register()...)

	setBashCompletionFunction(rootCmd, syncCmd)

	addHelpAboutNodes()

	initFlags(rootCmd, syncCmd)

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}

	if err := roachprod.InitDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := roachprod.LoadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s\n", err)
	}

	updateTime, sha, err := update.CheckLatest(roachprodUpdateBranch, roachprodUpdateOS, roachprodUpdateArch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: failed to check if a more recent 'roachprod' binary exists: %s\n", err)
	} else {
		age, err := update.TimeSinceUpdate(updateTime)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: unable to check mtime of 'roachprod' binary: %s\n", err)
		} else if age.Hours() >= 14*24 {
			fmt.Fprintf(os.Stderr, "WARN: roachprod binary is >= 2 weeks old (%s); latest sha: %q\nWARN: Consider updating the binary: `roachprod update`\n\n", age, sha)
		}
	}
}
