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
	name string
	impl func(t *testing.T)
}

func newCmd(name string, impl func(t *testing.T)) *cmd {
	return &cmd{
		name: name,
		impl: impl,
	}
}

func (c *cmd) exec(env cmdEnv, args ...string) (_ *exec.Cmd, stdout *bytes.Buffer) {
	cmd := exec.Command(os.Args[0], "--test.run=^"+c.name+"$", "--test.v")
	if len(args) > 0 {
		cmd.Args = append(cmd.Args, "--")
		cmd.Args = append(cmd.Args, args...)
	}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=t", allowInternalTestEnvVar))
	cmd.Env = append(cmd.Env, env.toStrings()...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	return cmd, &buf
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
