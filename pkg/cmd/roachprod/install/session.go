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
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type session interface {
	CombinedOutput(cmd string) ([]byte, error)
	Run(cmd string) error
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

func newRemoteSession(user, host string, logdir string) (*remoteSession, error) {
	logfile := filepath.Join(
		logdir,
		fmt.Sprintf("ssh_%s_%s", host, timeutil.Now().Format(time.RFC3339)),
	)
	// Check that the file can be written to. This also ensures
	// it has proper permissions so it can be collected / cleaned up afterwards.
	if err := ioutil.WriteFile(logfile, nil, 0644); err != nil {
		return nil, err
	}

	args := []string{
		user + "@" + host,
		"-vvv", "-E", logfile,
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "StrictHostKeyChecking=no",
		// Send keep alives every minute to prevent connections without activity
		// from dropping. (It is speculative that the absence of this caused
		// problems, though there's some indication that we need them:
		//
		// https://github.com/cockroachdb/cockroach/issues/35337
		"-o", "ServerAliveInterval=60",
	}
	args = append(args, sshAuthArgs()...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ssh", args...)
	return &remoteSession{cmd, cancel, logfile}, nil
}

func (s *remoteSession) errWithDebug(err error) error {
	if err != nil && s.logfile != "" {
		err = s.annotateSSHLastLines(err)
		err = errors.Wrapf(err, "ssh verbose log retained in %s", s.logfile)
		s.logfile = "" // prevent removal on close
	}
	return err
}

func (s *remoteSession) annotateSSHLastLines(err error) error {
	if err == nil || s.logfile == "" {
		return err
	}

	fi, statErr := os.Stat(s.logfile)
	if statErr != nil {
		// Can't stat.
		return errors.CombineErrors(err, statErr)
	}
	if fi.Size() == 0 {
		// Log file empty. Nothing to do.
		return err
	}
	f, openErr := os.Open(s.logfile)
	if openErr != nil {
		// Can't open.
		return errors.CombineErrors(err, openErr)
	}
	defer f.Close()

	const lastBufSz = 500
	const truncMsg = "<...skipped...>"
	buf := make([]byte, lastBufSz)
	dstOff := 0
	if int64(lastBufSz-len(truncMsg)) < fi.Size() {
		// If we are only looking at part of the log file, inform the
		// reader of the output that truncation occurred.
		copy(buf, []byte(truncMsg))
		dstOff = len(truncMsg)
	}
	// Read from the end, but avoid a negative offset.
	srcOff := int64(0)
	if fi.Size() > int64(lastBufSz-dstOff) {
		srcOff = fi.Size() - int64(lastBufSz-dstOff)
	}
	n, readErr := f.ReadAt(buf[dstOff:], srcOff)
	if readErr != nil {
		err = errors.CombineErrors(err, readErr)
		// Fallthrough: we still look at the data if any was read.
	}
	bufsz := dstOff + n
	if bufsz > 0 {
		err = errors.WithDetailf(err, "SSH debug log:\n%s", string(buf[:bufsz]))
	}
	return err
}

func (s *remoteSession) CombinedOutput(cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	b, err := s.Cmd.CombinedOutput()
	return b, s.errWithDebug(err)
}

func (s *remoteSession) Run(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.errWithDebug(s.Cmd.Run())
}

func (s *remoteSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
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

func (s *localSession) CombinedOutput(cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.CombinedOutput()
}

func (s *localSession) Run(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Run()
}

func (s *localSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
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
