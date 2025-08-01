// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package helpers

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/spf13/cobra"
)

// Wrap provide `cobra.Command` functions with a standard return code handler.
// Exit codes come from rperrors.Error.ExitCode().
//
// If the wrapped error tree of an error does not contain an instance of
// rperrors.Error, the error will automatically be wrapped with
// rperrors.Unclassified.
func Wrap(f func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		var err error
		err = f(cmd, args)
		if err != nil {
			drtprodError, ok := rperrors.AsError(err)
			if !ok {
				drtprodError = rperrors.Unclassified{Err: err}
				err = drtprodError
			}

			cmd.Printf("Error: %+v\n", err)

			os.Exit(drtprodError.ExitCode())
		}
	}
}

// ExecuteCmdWithPrefix runs a shell command with the given arguments and streams the output.
// it also adds the specified prefixes
func ExecuteCmdWithPrefix(ctx context.Context, logPrefix string, cmd string, args ...string) error {
	// Create a command with the given context and arguments.
	c := exec.CommandContext(ctx, cmd, args...)

	// Set up pipes to capture the command's stdout and stderr.
	stdout, err := c.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := c.StderrPipe()
	if err != nil {
		return err
	}

	// Stream stdout output
	outScanner := bufio.NewScanner(stdout)
	outScanner.Split(bufio.ScanLines)
	go func() {
		for outScanner.Scan() {
			m := outScanner.Text()
			fmt.Printf("[%s] %s\n", logPrefix, m)
		}
	}()

	// Stream stderr output
	errScanner := bufio.NewScanner(stderr)
	errScanner.Split(bufio.ScanLines)
	go func() {
		for errScanner.Scan() {
			m := errScanner.Text()
			fmt.Printf("[%s] %s\n", logPrefix, m)
		}
	}()

	// Wait for the command to complete and return any errors encountered.
	return c.Run()
}
