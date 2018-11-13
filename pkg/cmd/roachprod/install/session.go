package install

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
)

type session interface {
	CombinedOutput(cmd string) ([]byte, error)
	Run(cmd string) error
	SetStdin(r io.Reader)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.Reader, error)
	StderrPipe() (io.Reader, error)
	RequestPty() error
	Close() error
}

type remoteSession struct {
	*exec.Cmd
	cancel func()
}

func newRemoteSession(user, host string) (*remoteSession, error) {
	args := []string{
		user + "@" + host,
		"-q",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "StrictHostKeyChecking=no",
	}
	args = append(args, sshAuthArgs()...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ssh", args...)
	return &remoteSession{cmd, cancel}, nil
}

func (s *remoteSession) CombinedOutput(cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.CombinedOutput()
}

func (s *remoteSession) Run(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Run()
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

func (s *remoteSession) Close() error {
	s.cancel()
	return nil
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

func (s *localSession) Close() error {
	s.cancel()
	return nil
}

var sshAuthArgsVal []string
var sshAuthArgsOnce sync.Once

func sshAuthArgs() []string {
	sshAuthArgsOnce.Do(func() {
		paths := []string{
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
