// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
)

func TestRunCommand(t *testing.T) {
	provider := CLIProvider{}
	l := logger.NewMockLogger(t)

	setVerbose := func(verbose bool) {
		config.Verbose = verbose
		config.DryRun = false
	}
	setDryRun := func(dryRun bool) {
		config.DryRun = dryRun
		config.Verbose = false
	}
	// Restore global config. flags after we muck with them below.
	verbose := config.Verbose
	dryRun := config.DryRun
	defer func() {
		config.Verbose = verbose
		config.DryRun = dryRun
	}()

	// Missing CLIProvider.CLICommand
	output, err := provider.RunCommand(context.Background(), l.Logger, nil)
	require.Error(t, err)
	require.Equal(t, "", string(output))
	logger.RequireEqual(t, l, nil)

	// Successful command, empty output.
	provider.CLICommand = "true"
	l = logger.NewMockLogger(t)
	output, err = provider.RunCommand(context.Background(), l.Logger, nil)
	require.NoError(t, err)
	require.Equal(t, "", string(output))
	logger.RequireEqual(t, l, nil)

	// Successful command, non-empty output.
	provider.CLICommand = "echo"
	l = logger.NewMockLogger(t)
	output, err = provider.RunCommand(context.Background(), l.Logger, []string{"-n", "foo"})
	require.NoError(t, err)
	require.Equal(t, "foo", string(output))
	logger.RequireEqual(t, l, nil)

	// Erroneous command, output logged.
	provider.CLICommand = "foobar"
	l = logger.NewMockLogger(t)
	setVerbose(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, nil)
	require.ErrorContains(t, err, fmt.Sprintf("failed to run: %s \nstdout: \nstderr: \n: exec: \"%s\"", provider.CLICommand, provider.CLICommand))
	require.Equal(t, "", string(output))
	logger.RequireLine(t, l, "exec: foobar")

	// Erroneous command, `--dry-run`.
	provider.CLICommand = "foobar"
	l = logger.NewMockLogger(t)
	setDryRun(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, nil)
	require.NoError(t, err)
	require.Equal(t, "", string(output))
	logger.RequireLine(t, l, "exec: foobar")

	// Successful command, `--verbose`.
	provider.CLICommand = "echo"
	l = logger.NewMockLogger(t)
	setVerbose(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, []string{"-n", "foo"})
	require.NoError(t, err)
	require.Equal(t, "foo", string(output))
	logger.RequireLine(t, l, "exec: echo -n foo")

	// Successful command, `--dry-run`.
	provider.CLICommand = "echo"
	l = logger.NewMockLogger(t)
	setDryRun(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, []string{"-n", "foo"})
	require.NoError(t, err)
	require.Equal(t, "", string(output))
	logger.RequireLine(t, l, "exec: echo -n foo")

	// Erroneous command, `--dry-run` with `AlwaysExecute` option.
	provider.CLICommand = "foobar"
	l = logger.NewMockLogger(t)
	setDryRun(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, nil, AlwaysExecute())
	require.Error(t, err)
	require.Equal(t, "", string(output))
	logger.RequireLine(t, l, "exec: foobar")

	// Successful command, `--dry-run` with `AlwaysExecute` option.
	provider.CLICommand = "echo"
	l = logger.NewMockLogger(t)
	setDryRun(true)
	output, err = provider.RunCommand(context.Background(), l.Logger, []string{"-n", "foo"}, AlwaysExecute())
	require.NoError(t, err)
	require.Equal(t, "foo", string(output))
	logger.RequireLine(t, l, "exec: echo -n foo")
}
