// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	c := NewCommand("./cockroach")
	require.Equal(t, "./cockroach", c.String())

	c = NewCommand("./cockroach workload init")
	require.Equal(t, "./cockroach workload init", c.String())

	c = NewCommand("./cockroach").Arg("workload").Arg("init")
	require.Equal(t, "./cockroach workload init", c.String())

	baseCommand := NewCommand("./cockroach workload run bank").Arg("{pgurl:%d}", 1)

	c = clone(baseCommand)
	c.Flag("max-ops", 10).Flag("path", "/some/path")
	require.Equal(t, "./cockroach workload run bank {pgurl:1} --max-ops 10 --path /some/path", c.String())

	c = clone(baseCommand).WithEqualsSyntax()
	c.Flag("max-ops", 10).Flag("path", "/some/path")
	require.Equal(t, "./cockroach workload run bank {pgurl:1} --max-ops=10 --path=/some/path", c.String())

	c = clone(baseCommand)
	c.MaybeFlag(true, "max-ops", 10)     // included
	c.MaybeFlag(false, "concurrency", 8) // not included
	require.True(t, c.HasFlag("max-ops"))
	require.False(t, c.HasFlag("concurrency"))
	require.Equal(t, "./cockroach workload run bank {pgurl:1} --max-ops 10", c.String())

	c = clone(baseCommand)
	c.ITEFlag(true, "max-ops", 10, 20)
	c.ITEFlag(false, "duration", 2*time.Hour, 10*time.Minute)
	require.Equal(t, "./cockroach workload run bank {pgurl:1} --duration 10m0s --max-ops 10", c.String())

	c = clone(baseCommand)
	c.Option("local")
	c.MaybeOption(true, "background") // included
	c.MaybeOption(false, "dry-run")   // not included
	require.True(t, c.HasFlag("local"))
	require.True(t, c.HasFlag("background"))
	require.False(t, c.HasFlag("dry-run"))
	require.Equal(t, "./cockroach workload run bank {pgurl:1} --background --local", c.String())

	c = clone(baseCommand)
	c.Flag("c", 10)
	c.MaybeFlag(true, "n", "8")    // included
	c.MaybeFlag(false, "j", "yes") // not included
	c.Option("x")
	require.True(t, c.HasFlag("c"))
	require.Equal(t, "./cockroach workload run bank {pgurl:1} -c 10 -n 8 -x", c.String())
}

func clone(cmd *Command) *Command {
	flags := make(map[string]*string)
	for k, v := range cmd.Flags {
		flags[k] = v
	}

	return &Command{
		Binary:    cmd.Binary,
		Arguments: append([]string{}, cmd.Arguments...),
		Flags:     flags,
		UseEquals: cmd.UseEquals,
	}
}
