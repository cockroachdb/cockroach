// Copyright 2024 The Cockroach Authors.
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
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	roachprodcmds "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/commands"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/spf13/cobra"
)

// Initialize initializes the commands
func Initialize(ctx context.Context) {
	_ = os.Setenv("GCE_PROJECT", helpers.DefaultProject)
	_ = os.Setenv("ROACHPROD_DNS", helpers.DefaultDns)
	_ = roachprod.InitProviders()
	cobra.EnableCommandSorting = false

	rootCommand := commands.GetRootCommand(ctx)
	rootCommand.AddCommand(register(ctx)...)
	// check if the current command is found in drtprod. if not, redirect to roachprod
	_, _, err := rootCommand.Find(os.Args[1:])
	if err != nil {
		if strings.Contains(err.Error(), "unknown command") {
			// command is not found. So, check on roachprod
			if rperr := roachprodcmds.GetRoachprodCmd().Execute(); rperr != nil {
				// Cobra has already printed the error message.
				os.Exit(1)
			}
			return
		}
		// Cobra has already printed the error message.
		os.Exit(1)
	}
	if err := rootCommand.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
