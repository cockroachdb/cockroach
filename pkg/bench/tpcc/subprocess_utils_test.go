// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

type cmd struct {
	name    string
	impl    func(t *testing.T)
	envVars []string
}

func makeCmd(name string, impl func(t *testing.T)) cmd {
	return cmd{
		name:    name,
		impl:    impl,
		envVars: []string{allowInternalTestEnvVar + "=t"},
	}
}

func (c cmd) withEnv(k string, v any) cmd {
	c.envVars = append(c.envVars, fmt.Sprintf("%s=%v", k, v))
	return c
}

func (c cmd) exec(args ...string) (_ *exec.Cmd, stdout *bytes.Buffer) {
	cmd := exec.Command(os.Args[0], "--test.run=^"+c.name+"$", "--test.v")
	if len(args) > 0 {
	}
	for i, arg := range args {
		if i == 0 {
			cmd.Args = append(cmd.Args, "--")
		}
		cmd.Args = append(cmd.Args, "--"+arg)
	}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, c.envVars...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	return cmd, &buf
}
