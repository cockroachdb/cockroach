// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSSHClient is a test SSH client that records calls and returns
// configurable results.
type mockSSHClient struct {
	mu      sync.Mutex
	calls   []mockSSHCall
	handler func(addr, user string, privateKey []byte, script string) (string, string, error)
}

type mockSSHCall struct {
	Addr       string
	User       string
	PrivateKey []byte
	Script     string
}

func (m *mockSSHClient) RunCommand(
	ctx context.Context, _ *logger.Logger, addr, user string, privateKey []byte, script string,
) (string, string, error) {
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.calls = append(m.calls, mockSSHCall{
			Addr: addr, User: user, PrivateKey: privateKey, Script: script,
		})
	}()

	if m.handler != nil {
		return m.handler(addr, user, privateKey, script)
	}
	return "", "", nil
}

func TestRunCommandExecutor_SingleMachine_Success(t *testing.T) {
	sshClient := &mockSSHClient{}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "test",
			Command: "echo hello",
		},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "ubuntu",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)
	require.Len(t, sshClient.calls, 1)
	assert.Equal(t, "1.2.3.4", sshClient.calls[0].Addr)
	assert.Equal(t, "ubuntu", sshClient.calls[0].User)
	assert.Contains(t, sshClient.calls[0].Script, "echo hello")
}

func TestRunCommandExecutor_MultipleMachines(t *testing.T) {
	sshClient := &mockSSHClient{}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "multi",
			Command: "echo hi",
		},
		MachineIPs:    []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)
	assert.Len(t, sshClient.calls, 3)
}

func TestRunCommandExecutor_MachineFailure(t *testing.T) {
	sshClient := &mockSSHClient{
		handler: func(addr, user string, _ []byte, _ string) (string, string, error) {
			if addr == "2.2.2.2" {
				return "", "error output", fmt.Errorf("connection failed")
			}
			return "", "", nil
		},
	}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "partial-fail",
			Command: "echo test",
		},
		MachineIPs:    []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2.2.2.2")
}

func TestRunCommandExecutor_WithEnv(t *testing.T) {
	sshClient := &mockSSHClient{}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "with-env",
			Command: "echo $MY_VAR",
		},
		HookEnv:       map[string]string{"MY_VAR": "secret_value"},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "ubuntu",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)
	require.Len(t, sshClient.calls, 1)
	// Script should contain the base64-encoded env var, not the raw value.
	assert.Contains(t, sshClient.calls[0].Script, "export MY_VAR=")
	assert.NotContains(t, sshClient.calls[0].Script, "secret_value")
}

func TestRunCommandExecutor_WithRetry_EventualSuccess(t *testing.T) {
	var callCount atomic.Int32
	sshClient := &mockSSHClient{
		handler: func(addr, _ string, _ []byte, _ string) (string, string, error) {
			c := callCount.Add(1)
			if c < 3 {
				return "", "", fmt.Errorf("not ready")
			}
			return "", "", nil
		},
	}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "retry-test",
			Command: "test -f /.ready",
			Retry: &provmodels.RetryConfig{
				Interval: "10ms",
				Timeout:  "5s",
			},
		},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, int(callCount.Load()), 3)
}

func TestRunCommandExecutor_WithRetry_Timeout(t *testing.T) {
	sshClient := &mockSSHClient{
		handler: func(_, _ string, _ []byte, _ string) (string, string, error) {
			return "", "", fmt.Errorf("always fails")
		},
	}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "timeout-test",
			Command: "test -f /.ready",
			Retry: &provmodels.RetryConfig{
				Interval: "10ms",
				Timeout:  "50ms",
			},
		},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestRunCommandExecutor_NoRetry_FailsImmediately(t *testing.T) {
	var callCount atomic.Int32
	sshClient := &mockSSHClient{
		handler: func(_, _ string, _ []byte, _ string) (string, string, error) {
			callCount.Add(1)
			return "", "", fmt.Errorf("command failed")
		},
	}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "no-retry",
			Command: "false",
			// No Retry block — should fail immediately.
		},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	err := exec.Execute(context.Background(), logger.DefaultLogger, hctx)
	require.Error(t, err)
	assert.Equal(t, int32(1), callCount.Load()) // only one attempt
}

func TestRunCommandExecutor_ContextCancel(t *testing.T) {
	// Context must be created first so the mock handler can capture it.
	ctx, cancel := context.WithCancel(context.Background())

	// SSH client simulates a long-running command that respects context,
	// mirroring the real SSHClient's Start+Wait+select pattern.
	sshClient := &mockSSHClient{
		handler: func(_, _ string, _ []byte, _ string) (string, string, error) {
			<-ctx.Done()
			return "", "", ctx.Err()
		},
	}
	exec := NewRunCommandExecutor(sshClient)

	hctx := HookContext{
		Declaration: provmodels.HookDeclaration{
			Name:    "cancel-test",
			Command: "sleep 999",
		},
		MachineIPs:    []string{"1.2.3.4"},
		SSHUser:       "root",
		SSHPrivateKey: []byte("key"),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- exec.Execute(ctx, logger.DefaultLogger, hctx)
	}()

	// Cancel the context — the executor should return promptly.
	cancel()

	err := <-errCh
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
