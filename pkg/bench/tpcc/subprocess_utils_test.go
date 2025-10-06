// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/stretchr/testify/require"
)

type cmd struct {
	name string
	impl func(t *testing.T)
}

func (c *cmd) exec(env cmdEnv, args ...string) *exec.Cmd {
	cmd := exec.Command(os.Args[0],
		"--test.run=^TestInternal"+c.name+"$",
		"--test.v")
	if len(args) > 0 {
		cmd.Args = append(cmd.Args, "--")
		cmd.Args = append(cmd.Args, args...)
	}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=t", internalTestEnvVar))
	cmd.Env = append(cmd.Env, env.toStrings()...)
	return cmd
}

var isInternalTest = envutil.EnvOrDefaultBool(internalTestEnvVar, false)

func internalCommand(t *testing.T) {
	if !isInternalTest {
		skip.IgnoreLint(t)
	}
	f, ok := commands[strings.TrimPrefix(t.Name(), "TestInternal")]
	require.True(t, ok)
	f.impl(t)
}

func registerCmd(name string, impl func(t *testing.T)) *cmd {
	c := &cmd{
		name: name,
		impl: impl,
	}
	commands[name] = c
	return c
}

type envVar struct {
	k string
	v interface{}
}

type cmdEnv []envVar

func (ce cmdEnv) toStrings() (ret []string) {
	for _, v := range ce {
		ret = append(ret, fmt.Sprintf("%s=%v", v.k, v.v))
	}
	return ret
}
