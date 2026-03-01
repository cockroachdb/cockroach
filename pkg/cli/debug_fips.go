// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"crypto/fips140"
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/security/fips"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

// Defines CCL-specific debug commands, adds the encryption flag to debug commands in
// `pkg/cli/debug.go`, and registers a callback to generate encryption options.
func init() {
	checkFipsCmd := &cobra.Command{
		Use:   "enterprise-check-fips",
		Short: "print diagnostics for FIPS-ready configuration",
		Long: `
Performs various tests of this binary's ability to operate in FIPS-ready
mode in the current environment.
`,

		RunE: clierrorplus.MaybeDecorateError(runCheckFips),
	}

	DebugCmd.AddCommand(checkFipsCmd)
}

func runCheckFips(cmd *cobra.Command, args []string) error {
	if runtime.GOOS != "linux" {
		return errors.New("FIPS-ready mode is only supported on linux")
	}
	// Our FIPS-ready deployments has two major requirements:
	// 1. FIPS 140-3 mode is enabled in the Go runtime
	// 2. FIPS mode is enabled in the kernel.
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	emit := func(label string, status bool, detail string) {
		statusSymbol := "❌"
		if status {
			statusSymbol = "✅"
		}
		table.Append([]string{label, statusSymbol, detail})
	}

	fipsEnabled := fips140.Enabled()
	emit("FIPS-ready build", fipsEnabled, "")
	isKernelEnabled, err := fips.IsKernelEnabled()
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	emit("Kernel FIPS mode enabled", isKernelEnabled, detail)
	emit("FIPS ready", fipsEnabled && isKernelEnabled, "")

	table.Render()
	return nil
}
