// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package helpers

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
)

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
