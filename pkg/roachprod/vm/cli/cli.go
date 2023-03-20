// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

type (
	CLIProvider struct {
		// Name of CLI command to execute.
		CLICommand string

		// Name of (auth.) profile to use.
		Profile string
	}
	CommandSpec struct {
		// If true, the command is _executed_, even in dry-run mode.
		// E.g., FindActiveAccount is a prerequisite for Create; thus, it must be executed; otherwise,
		// Create will never be invoked.
		AlwaysExecute bool
	}

	// Option for CommandSpec.
	Option func(opts *CommandSpec)
)

func AlwaysExecute() Option {
	return func(spec *CommandSpec) {
		spec.AlwaysExecute = true
	}
}

// Execute a command using CLIProvider's CLICommand.
// If command exits 0, return stdout, ignoring stderr.
// Otherwise, the returned error encapsulates both stdout and stderr, including the exit code.
func (p *CLIProvider) RunCommand(
	ctx context.Context, l *logger.Logger, args []string, opts ...Option,
) ([]byte, error) {
	cmdSpec := CommandSpec{}
	for _, opt := range opts {
		opt(&cmdSpec)
	}
	return p.runCommand(ctx, l, 2, args, cmdSpec)
}

// runJSONCommand executes CLIProvider's CLICommand, parsing the output as JSON.
// If the JSON output is valid, it is unmarshaled into 'parsed'; otherwise, an error is returned.
func (p *CLIProvider) RunJSONCommand(
	ctx context.Context, l *logger.Logger, args []string, parsed interface{}, opts ...Option,
) error {
	if p.CLICommand == "aws" {
		// Force json output in case the user has overridden the default behavior.
		args = append(args[:len(args):len(args)], "--output", "json")
	}
	cmdSpec := CommandSpec{}
	for _, opt := range opts {
		opt(&cmdSpec)
	}
	rawJSON, err := p.runCommand(ctx, l, 3, args, cmdSpec)
	if err != nil {
		return err
	}

	if !config.DryRun || cmdSpec.AlwaysExecute {
		if err := json.Unmarshal(rawJSON, &parsed); err != nil {
			return errors.Wrapf(err, "failed to parse json %s", rawJSON)
		}
	}

	return nil
}

func (p *CLIProvider) runCommand(
	ctx context.Context, l *logger.Logger, depth int, args []string, cmdSpec CommandSpec,
) ([]byte, error) {
	if p.CLICommand == "" {
		return nil, errors.New("CLICommand is not set")
	}
	if p.Profile != "" {
		args = append(args[:len(args):len(args)], "--profile", p.Profile)
	}
	maybeLogCmdStr(ctx, l, depth+1, p.CLICommand, args)
	// Bail out early if we're in dry-run mode and the command is not required to be executed.
	if config.DryRun && !cmdSpec.AlwaysExecute {
		return nil, nil
	}
	var stderrBuf bytes.Buffer
	var cmd *exec.Cmd

	if ctx == nil {
		cmd = exec.Command(p.CLICommand, args...)
	} else {
		cmd = exec.CommandContext(ctx, p.CLICommand, args...)
	}
	cmd.Stderr = &stderrBuf
	output, err := cmd.Output()
	if err != nil {
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			config.Logger.Printf("%s", string(exitErr.Stderr))
		}
		stderr := stderrBuf.Bytes()
		// TODO(peter,ajwerner): Remove this hack once gcloud behaves when adding new zones.
		// 'gcloud compute instances list --project cockroach-ephemeral --format json'
		// would fail with, "Invalid value for field 'zone': 'asia-northeast2-a'. Unknown zone.", around the time when
		// the region was added but not fully available. It remains unclear whether this can still happen with newly
		// added GCP regions. One potential workaround is to constrain the list of zones.
		if matched, _ := regexp.Match(`.*Unknown zone`, stderr); !matched {
			return nil, errors.Wrapf(err, "failed to run: %s %s\nstdout: %s\nstderr: %s\n",
				p.CLICommand, strings.Join(args, " "), bytes.TrimSpace(output), bytes.TrimSpace(stderr))
		}
	}
	return output, nil
}

// MaybeLogCmd logs the full command string, in dry-run or verbose mode.
func MaybeLogCmd(ctx context.Context, l *logger.Logger, cmd *exec.Cmd) {
	if config.DryRun || config.Verbose {
		l.PrintfCtxDepth(ctx, 2, "exec: %s %s", cmd.Args[0], strings.Join(cmd.Args[1:], " "))
	}
}

// Invoked internally by RunCommand. The depth is 3 because we want to ignore 2 internal calls plus PrintfCtxDepth
func maybeLogCmdStr(ctx context.Context, l *logger.Logger, depth int, cmd string, args []string) {
	if config.DryRun || config.Verbose {
		l.PrintfCtxDepth(ctx, depth+1, "exec: %s %s", cmd, strings.Join(args, " "))
	}
}
