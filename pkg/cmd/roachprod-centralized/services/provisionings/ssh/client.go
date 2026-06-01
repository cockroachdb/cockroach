// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssh

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ssh"
)

// ISSHClient is the interface for running commands on remote machines via SSH.
type ISSHClient interface {
	RunCommand(
		ctx context.Context, l *logger.Logger, addr, user string,
		privateKey []byte, script string,
	) (stdout, stderr string, err error)
}

const (
	sshPort           = "22"
	connectTimeout    = 5 * time.Second
	maxConnRetries    = 3
	keepAliveInterval = 60 * time.Second
)

// backoffDurations defines the exponential backoff durations for SSH
// connection retries: 5s, 10s, 20s.
var backoffDurations = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
}

// SSHClient is the production implementation of ISSHClient using
// golang.org/x/crypto/ssh. Host keys are not verified
// (InsecureIgnoreHostKey) because provisioned machines are ephemeral and
// their host keys are not known in advance.
type SSHClient struct{}

// NewSSHClient creates a new SSH client.
func NewSSHClient() *SSHClient {
	return &SSHClient{}
}

// RunCommand connects to addr:22 via SSH, sends the script on stdin to
// "bash -s", and returns the captured stdout and stderr. Connection retries
// are performed for transient errors (connection refused, timeout, EOF).
// Auth failures are permanent and not retried. The command is
// context-interruptible: if ctx is cancelled during execution, the SSH
// session is closed to terminate the remote process.
func (c *SSHClient) RunCommand(
	ctx context.Context, l *logger.Logger, addr, user string, privateKey []byte, script string,
) (string, string, error) {
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return "", "", errors.Wrap(err, "parse SSH private key")
	}

	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         connectTimeout,
	}

	host := addr
	if !strings.Contains(addr, ":") {
		host = net.JoinHostPort(addr, sshPort)
	}

	client, err := dialWithRetry(ctx, host, config)
	if err != nil {
		return "", "", errors.Wrapf(err, "SSH dial %s", host)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			l.Warn("SSH client close error",
				slog.String("addr", addr), slog.Any("error", closeErr))
		}
	}()

	// Start a keep-alive goroutine that sends periodic requests to prevent
	// the server from closing idle connections.
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(keepAliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// Best-effort keep-alive; ignore errors.
				_, _, _ = client.SendRequest("keepalive@openssh.com", true, nil)
			}
		}
	}()

	session, err := client.NewSession()
	if err != nil {
		return "", "", errors.Wrap(err, "create SSH session")
	}
	defer func() {
		// Close is safe to call multiple times; the second call returns
		// an error that we ignore via the nil check on closeErr severity.
		if closeErr := session.Close(); closeErr != nil {
			// "use of closed network connection" is expected on the
			// context-cancelled path where we close the session explicitly.
			if !strings.Contains(closeErr.Error(), "use of closed") {
				l.Warn("SSH session close error",
					slog.String("addr", addr),
					slog.Any("error", closeErr))
			}
		}
	}()

	session.Stdin = strings.NewReader(script)

	var stdoutBuf, stderrBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stderrBuf

	// Use Start + Wait (instead of Run) so we can race command completion
	// against context cancellation. This follows the pattern from
	// pkg/roachprod/install/session.go.
	if err := session.Start("bash -s"); err != nil {
		return stdoutBuf.String(), stderrBuf.String(),
			errors.Wrapf(err, "start command on %s", addr)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- session.Wait()
	}()

	select {
	case <-ctx.Done():
		// Context cancelled — close session to kill the remote process.
		// The error from Close is intentionally ignored here: we are
		// forcibly terminating and will return ctx.Err() regardless.
		_ = session.Close()
		<-waitDone // drain the wait goroutine
		return stdoutBuf.String(), stderrBuf.String(), ctx.Err()
	case err := <-waitDone:
		if err != nil {
			return stdoutBuf.String(), stderrBuf.String(),
				errors.Wrapf(err, "run command on %s", addr)
		}
		return stdoutBuf.String(), stderrBuf.String(), nil
	}
}

// dialWithRetry attempts to dial the SSH server with exponential backoff.
// Transient errors (connection refused, timeout, EOF) trigger retries.
// Auth failures are considered permanent and return immediately.
func dialWithRetry(
	ctx context.Context, addr string, config *ssh.ClientConfig,
) (*ssh.Client, error) {
	var lastErr error
	for attempt := range maxConnRetries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		client, err := ssh.Dial("tcp", addr, config)
		if err == nil {
			return client, nil
		}
		lastErr = err

		if isPermanentSSHError(err) {
			return nil, err
		}

		if attempt < maxConnRetries-1 {
			backoff := backoffDurations[attempt]
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return nil, lastErr
}

// isPermanentSSHError returns true for errors that should not be retried
// (e.g., authentication failures). Transient errors like connection refused,
// timeout, or EOF during handshake are retried.
func isPermanentSSHError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "unable to authenticate") ||
		strings.Contains(msg, "no supported methods remain")
}
