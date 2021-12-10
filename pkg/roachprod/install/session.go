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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/errors"
)

type session interface {
	CombinedOutput(cmd string) ([]byte, error)
	Run(cmd string) error
	SetWithExitStatus(value bool)
	SetStdin(r io.Reader)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
	Start(cmd string) error
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.Reader, error)
	StderrPipe() (io.Reader, error)
	RequestPty() error
	Wait() error
}

type remoteSession struct {
	*exec.Cmd
	logfile        string // captures ssh -vvv
	withExitStatus bool
}

func newRemoteSession(
	ctx context.Context, user, host string, logdir string,
) (*remoteSession, error) {
	// TODO(tbg): this is disabled at the time of writing. It was difficult
	// to assign the logfiles to the roachtest and as a bonus our CI harness
	// never actually managed to collect the files since they had wrong
	// permissions; instead they clogged up the roachprod dir.
	// logfile := filepath.Join(
	//	logdir,
	// 	fmt.Sprintf("ssh_%s_%s", host, timeutil.Now().Format(time.RFC3339)),
	// )
	const logfile = ""
	withExitStatus := false
	args := []string{
		user + "@" + host,

		// TODO(tbg): see above.
		//"-vvv", "-E", logfile,
		// NB: -q suppresses -E, at least on OSX. Difficult decisions will have
		// to be made if omitting -q leads to annoyance on stdout/stderr.

		"-q",
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
	args = append(args, sshAuthArgs()...)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	return &remoteSession{cmd, logfile, withExitStatus}, nil
}

func (s *remoteSession) errWithDebug(err error) error {
	if err != nil && s.logfile != "" {
		err = errors.Wrapf(err, "ssh verbose log retained in %s", s.logfile)
		s.logfile = "" // prevent removal on close
	}
	return err
}

func (s *remoteSession) CombinedOutput(cmd string) ([]byte, error) {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	b, err := s.Cmd.CombinedOutput()
	return b, s.errWithDebug(err)
}

func (s *remoteSession) Run(cmd string) error {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.errWithDebug(s.Cmd.Run())
}

func (s *remoteSession) Start(cmd string) error {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
}

func (s *remoteSession) SetWithExitStatus(value bool) {
	s.withExitStatus = value
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

type localSession struct {
	*exec.Cmd
	withExitStatus bool
}

func newLocalSession(ctx context.Context) *localSession {
	withExitStatus := false
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c")
	return &localSession{cmd, withExitStatus}
}

func (s *localSession) CombinedOutput(cmd string) ([]byte, error) {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.CombinedOutput()
}

func (s *localSession) Run(cmd string) error {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Run()
}

func (s *localSession) Start(cmd string) error {
	if s.withExitStatus {
		cmd = strings.TrimSpace(cmd)
		if !strings.HasSuffix(cmd, ";") {
			cmd += ";"
		}
		cmd += "echo -n 'LAST EXIT STATUS: '$?;"
	}
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
}

func (s *localSession) SetWithExitStatus(value bool) {
	s.withExitStatus = value
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
