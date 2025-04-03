// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/fipsccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
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

	cli.DebugCmd.AddCommand(checkFipsCmd)
}

func runCheckFips(cmd *cobra.Command, args []string) error {
	if runtime.GOOS != "linux" {
		return errors.New("FIPS-ready mode is only supported on linux")
	}
	// Our FIPS-ready deployments have three major requirements:
	// 1. This binary is built with the golang-fips toolchain and running on linux
	// 2. FIPS mode is enabled in the kernel.
	// 3. We can dynamically load the OpenSSL library (which must be the same major version that was present at
	//    build time). Verifying that the OpenSSL library is FIPS-compliant is outside the scope of this command.
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

	emit("FIPS-ready build", fipsccl.IsCompileTimeFIPSReady(), "")
	buildOpenSSLVersion, soname, err := fipsccl.BuildOpenSSLVersion()
	if err == nil {
		table.Append([]string{"Build-time OpenSSL Version", "", buildOpenSSLVersion})
		table.Append([]string{"OpenSSL library filename", "", soname})
	}

	isKernelEnabled, err := fipsccl.IsKernelEnabled()
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	emit("Kernel FIPS mode enabled", isKernelEnabled, detail)

	emit("OpenSSL loaded", fipsccl.IsOpenSSLLoaded(), "")
	emit("FIPS ready", fipsccl.IsFIPSReady(), "")

	table.Render()
	return nil
}
