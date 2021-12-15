package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

// makeRunCmd inherits almost identical functionality as "build" cmd
func makeRunCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	buildCmd := &cobra.Command{
		Use:   "run <binary>",
		Short: "Run the specified binaries",
		Long:  "Run the specified binaries.",
		Example: `
	dev run //pkg/cmd/cockroach -- start-single-node --insecure
	`,
		Args: cobra.MinimumNArgs(1),
		RunE: runE,
	}
	return buildCmd
}

func (d *dev) run(cmd *cobra.Command, commandLine []string) error {
	targets, executableArgs := splitArgsAtDash(cmd, commandLine)
	// validate that single target is provided. Bazel allows single target to be run
	if len(targets) != 1 {
		return fmt.Errorf("single target is required. Received: %v", targets)
	}
	ctx := cmd.Context()
	aliased, ok := buildTargetMapping[targets[0]]
	if !ok {
		// assume raw target is provided
		if strings.Index(targets[0], "//") == 0 {
			aliased = targets[0]
		} else {
			return fmt.Errorf("unrecognized target: %s", targets[0])
		}
	}
	args := []string{"run"}
	if aliased == buildTargetMapping["cockroach"] || aliased == buildTargetMapping["cockroach-oss"] {
		args = append(args, "--config=with_ui")
	}
	args = append(args, "--", aliased)
	args = append(args, executableArgs...)
	logCommand("bazel", args...)
	if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
		return err
	}
	return nil
}
