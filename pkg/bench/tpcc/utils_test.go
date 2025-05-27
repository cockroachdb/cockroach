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
	"os/signal"
	"syscall"
	"testing"
	"time"

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

// exec executes the command in a subprocess. The args should be in the form
// "--arg=value". It returns the command, which has not yet been executed, and a
// buffer where the STDOUT and STDERR of the subprocess will be written.
func (c cmd) exec(args ...string) (ec *exec.Cmd, output *synchronizedBuffer) {
	ec = exec.Command(os.Args[0], "--test.run=^"+c.name+"$", "--test.v")
	if len(args) > 0 {
		ec.Args = append(ec.Args, "--")
	}
	ec.Args = append(ec.Args, args...)
	ec.Env = append(os.Environ(), c.envVars...)
	output = new(synchronizedBuffer)
	ec.Stdout, ec.Stderr = output, output
	return ec, output
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

type synchronizer struct {
	pid    int // the PID of the process to coordinate with
	waitCh chan os.Signal
}

func (s *synchronizer) init(pid int) {
	s.pid = pid
	s.waitCh = make(chan os.Signal, 1)
	signal.Notify(s.waitCh, syscall.SIGUSR1)
}

func (s synchronizer) notify(t testing.TB) {
	if err := syscall.Kill(s.pid, syscall.SIGUSR1); err != nil {
		t.Fatalf("failed to notify process %d: %s", s.pid, err)
	}
}

func (s synchronizer) wait() {
	<-s.waitCh
}

func (c synchronizer) waitWithTimeout() (timedOut bool) {
	select {
	case <-c.waitCh:
		return false
	case <-time.After(5 * time.Second):
		return true
	}
}
