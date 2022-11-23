// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type session interface {
	CombinedOutput(ctx context.Context, cmd string) ([]byte, error)
	Run(ctx context.Context, cmd string) error
	SetStdin(r io.Reader)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
	Start(cmd string) error
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.Reader, error)
	StderrPipe() (io.Reader, error)
	RequestPty() error
	Wait() error
	Close()
}

type remoteSession struct {
	*exec.Cmd
	cancel  func()
	logfile string // captures ssh -vvv
}

// TODO: MG investigate - ssh log is not produced for the 3rd and final attempt
func newRemoteSession(l *logger.Logger, user, host string) (*remoteSession, error) {
	var logfile string
	var loggingArgs []string
	cl, err := l.ChildLogger(fmt.Sprintf("ssh_%s_%s", host, timeutil.Now().Format(time.RFC3339)))
	// running roachprod from the cli will result in a fileless logger
	if err == nil && l.File != nil {
		logfile = cl.File.Name()
		loggingArgs = []string{
			"-vvv", "-E", logfile,
		}
	} else {
		// NB: -q suppresses -E, at least on *nix.
		loggingArgs = []string{"-q"}
	}
	//const logfile = ""
	args := []string{
		user + "@" + host,

		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "StrictHostKeyChecking=no",
		// Send keep alives every minute to prevent connections without activity
		// from dropping. (It is speculative that the absence of this caused
		// problems, though there's some indication that we need them:
		//
		// https://github.com/cockroachdb/cockroach/issues/35337
		"-o", "ServerAliveInterval=60",
		// Timeout long connections so failure information is not lost by the roachtest
		// context cancellation killing hanging roachprod processes.
		"-o", "ConnectTimeout=5",
	}
	args = append(args, loggingArgs...)
	args = append(args, sshAuthArgs()...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ssh", args...)
	return &remoteSession{cmd, cancel, logfile}, nil
}

func (s *remoteSession) errWithDebug(err error) error {
	if err != nil && s.logfile != "" {
		err = errors.Wrapf(err, "ssh verbose log retained in %s", filepath.Base(s.logfile))
		s.logfile = "" // prevent removal on close
	}
	return err
}

func (s *remoteSession) CombinedOutput(ctx context.Context, cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)

	var b []byte
	var err error
	commandFinished := make(chan struct{})

	go func() {
		b, err = s.Cmd.CombinedOutput()
		err = s.errWithDebug(err)
		close(commandFinished)
	}()

	select {
	case <-ctx.Done():
		s.Close()
		return nil, ctx.Err()
	case <-commandFinished:
		return b, rperrors.ClassifyCmdError(err)
	}
}

func (s *remoteSession) Run(ctx context.Context, cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)

	var err error
	commandFinished := make(chan struct{})
	go func() {
		err = s.errWithDebug(s.Cmd.Run())
		close(commandFinished)
	}()

	select {
	case <-ctx.Done():
		s.Close()
		return ctx.Err()
	case <-commandFinished:
		return rperrors.ClassifyCmdError(err)
	}
}

func (s *remoteSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return rperrors.ClassifyCmdError(s.errWithDebug(s.Cmd.Start()))
}

func (s *remoteSession) SetStdin(r io.Reader) {
	s.Stdin = r
}

func (s *remoteSession) SetStdout(w io.Writer) {
	s.Stdout = w
}

func (s *remoteSession) SetStderr(w io.Writer) {
	s.Stderr = w
}

func (s *remoteSession) StdoutPipe() (io.Reader, error) {
	// NB: exec.Cmd.StdoutPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StdoutPipe()
	return r, err
}

func (s *remoteSession) StderrPipe() (io.Reader, error) {
	// NB: exec.Cmd.StderrPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StderrPipe()
	return r, err
}

func (s *remoteSession) RequestPty() error {
	s.Cmd.Args = append(s.Cmd.Args, "-t")
	return nil
}

func (s *remoteSession) Wait() error {
	return s.Cmd.Wait()
}

func (s *remoteSession) Close() {
	s.cancel()
	if s.logfile != "" {
		_ = os.Remove(s.logfile)
	}
}

type localSession struct {
	*exec.Cmd
	cancel func()
}

func newLocalSession() *localSession {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c")
	return &localSession{cmd, cancel}
}

func (s *localSession) CombinedOutput(ctx context.Context, cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)

	var b []byte
	var err error
	commandFinished := make(chan struct{})

	go func() {
		b, err = s.Cmd.CombinedOutput()
		close(commandFinished)
	}()

	select {
	case <-ctx.Done():
		s.Close()
		return nil, ctx.Err()
	case <-commandFinished:
		return b, rperrors.ClassifyCmdError(err)
	}
}

func (s *localSession) Run(ctx context.Context, cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)

	var err error
	commandFinished := make(chan struct{})
	go func() {
		err = s.Cmd.Run()
		close(commandFinished)
	}()

	select {
	case <-ctx.Done():
		s.Close()
		return ctx.Err()
	case <-commandFinished:
		return rperrors.ClassifyCmdError(err)
	}
}

func (s *localSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return rperrors.ClassifyCmdError(s.Cmd.Start())
}

func (s *localSession) SetStdin(r io.Reader) {
	s.Stdin = r
}

func (s *localSession) SetStdout(w io.Writer) {
	s.Stdout = w
}

func (s *localSession) SetStderr(w io.Writer) {
	s.Stderr = w
}

func (s *localSession) StdoutPipe() (io.Reader, error) {
	// NB: exec.Cmd.StdoutPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StdoutPipe()
	return r, err
}

func (s *localSession) StderrPipe() (io.Reader, error) {
	// NB: exec.Cmd.StderrPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StderrPipe()
	return r, err
}

func (s *localSession) RequestPty() error {
	return nil
}

func (s *localSession) Wait() error {
	return s.Cmd.Wait()
}

func (s *localSession) Close() {
	s.cancel()
}

var sshAuthArgsVal []string
var sshAuthArgsOnce sync.Once

func sshAuthArgs() []string {
	sshAuthArgsOnce.Do(func() {
		paths := []string{
			filepath.Join(config.OSUser.HomeDir, ".ssh", "id_ed25519"),
			filepath.Join(config.OSUser.HomeDir, ".ssh", "id_rsa"),
			filepath.Join(config.OSUser.HomeDir, ".ssh", "google_compute_engine"),
		}
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				sshAuthArgsVal = append(sshAuthArgsVal, "-i", p)
			}
		}
	})
	return sshAuthArgsVal
}
