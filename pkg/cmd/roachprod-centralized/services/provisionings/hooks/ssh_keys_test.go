// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"strings"
	"testing"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/require"
)

// fakeSSHClient records calls to RunCommand for testing.
type fakeSSHClient struct {
	calls  []fakeSSHCall
	err    error
	stdout string
	stderr string
}

type fakeSSHCall struct {
	addr   string
	user   string
	script string
}

func (f *fakeSSHClient) RunCommand(
	_ context.Context, _ *logger.Logger, addr, user string, _ []byte, script string,
) (string, string, error) {
	f.calls = append(f.calls, fakeSSHCall{
		addr: addr, user: user, script: script,
	})
	return f.stdout, f.stderr, f.err
}

// fakeKeysProvider implements AuthorizedKeysProvider for testing.
type fakeKeysProvider struct {
	keys []byte
	err  error
}

func (f *fakeKeysProvider) GetAuthorizedKeys(_ context.Context) ([]byte, error) {
	return f.keys, f.err
}

func TestSSHKeysSetupExecutor_Execute(t *testing.T) {
	l := logger.NewLogger("info")

	t.Run("writes authorized_keys to all machines", func(t *testing.T) {
		sshClient := &fakeSSHClient{}
		keysContent := []byte("ssh-ed25519 AAAA... alice@laptop\nssh-rsa AAAA... bob@desktop\n")
		provider := &fakeKeysProvider{keys: keysContent}
		executor := NewSSHKeysSetupExecutor(sshClient, provider)

		hctx := HookContext{
			Declaration: provmodels.HookDeclaration{
				Name: "setup-keys",
				Type: "ssh-keys-setup",
			},
			MachineIPs:    []string{"10.0.0.1", "10.0.0.2"},
			SSHUser:       "ubuntu",
			SSHPrivateKey: []byte("fake-key"),
		}

		err := executor.Execute(context.Background(), l, hctx)
		require.NoError(t, err)

		// Should have SSHed into both machines.
		require.Len(t, sshClient.calls, 2)

		// Each call should contain the base64-encoded authorized_keys.
		for _, call := range sshClient.calls {
			require.Equal(t, "ubuntu", call.user)
			require.Contains(t, call.script, "base64 -d")
			require.Contains(t, call.script, "chmod 600")
			require.Contains(t, call.script, "chmod 700")
			require.Contains(t, call.script, "mkdir -p ~/.ssh")
		}

		// Verify the machine addresses.
		addrs := []string{sshClient.calls[0].addr, sshClient.calls[1].addr}
		require.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, addrs)
	})

	t.Run("keys provider error propagated", func(t *testing.T) {
		sshClient := &fakeSSHClient{}
		provider := &fakeKeysProvider{
			err: context.DeadlineExceeded,
		}
		executor := NewSSHKeysSetupExecutor(sshClient, provider)

		hctx := HookContext{
			Declaration: provmodels.HookDeclaration{
				Name: "setup-keys",
				Type: "ssh-keys-setup",
			},
			MachineIPs: []string{"10.0.0.1"},
			SSHUser:    "ubuntu",
		}

		err := executor.Execute(context.Background(), l, hctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "fetch authorized keys")
		require.Empty(t, sshClient.calls)
	})

	t.Run("SSH failure on machine propagated", func(t *testing.T) {
		sshClient := &fakeSSHClient{
			err:    context.DeadlineExceeded,
			stderr: "connection timed out",
		}
		provider := &fakeKeysProvider{
			keys: []byte("ssh-ed25519 AAAA... alice@laptop\n"),
		}
		executor := NewSSHKeysSetupExecutor(sshClient, provider)

		hctx := HookContext{
			Declaration: provmodels.HookDeclaration{
				Name: "setup-keys",
				Type: "ssh-keys-setup",
			},
			MachineIPs:    []string{"10.0.0.1"},
			SSHUser:       "ubuntu",
			SSHPrivateKey: []byte("fake-key"),
		}

		err := executor.Execute(context.Background(), l, hctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "machine 10.0.0.1")
	})

	t.Run("script contains full file replacement", func(t *testing.T) {
		sshClient := &fakeSSHClient{}
		keysContent := []byte("ssh-ed25519 AAAA... alice@laptop\n")
		provider := &fakeKeysProvider{keys: keysContent}
		executor := NewSSHKeysSetupExecutor(sshClient, provider)

		hctx := HookContext{
			Declaration: provmodels.HookDeclaration{
				Name: "setup-keys",
				Type: "ssh-keys-setup",
			},
			MachineIPs:    []string{"10.0.0.1"},
			SSHUser:       "ubuntu",
			SSHPrivateKey: []byte("fake-key"),
		}

		err := executor.Execute(context.Background(), l, hctx)
		require.NoError(t, err)

		// The script writes to authorized_keys (not append).
		script := sshClient.calls[0].script
		require.Contains(t, script, "> ~/.ssh/authorized_keys")
		// Must NOT use >> (append).
		require.False(t, strings.Contains(script, ">>"),
			"script should replace, not append")
	})
}
