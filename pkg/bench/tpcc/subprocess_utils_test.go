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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type cmd struct {
	name    string
	impl    func(t *testing.T)
	envVars []string
}

func makeCmd(name string, impl func(t *testing.T)) cmd {
	return cmd{
		name: name,
		impl: impl,
	}.withEnv(allowInternalTestEnvVar, true)
}

func (c cmd) withEnv(k string, v any) cmd {
	c.envVars = append(c.envVars, fmt.Sprintf("%s=%v", k, v))
	return c
}

func (c cmd) exec(args ...string) (_ *exec.Cmd, output *synchronizedBuffer) {
	cmd := exec.Command(os.Args[0], "--test.run=^"+c.name+"$", "--test.v")
	if len(args) > 0 {
		cmd.Args = append(cmd.Args, "--")
		cmd.Args = append(cmd.Args, args...)
	}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, c.envVars...)
	var buf synchronizedBuffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	return cmd, &buf
}

type synchronizedBuffer struct {
	mu  syncutil.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
