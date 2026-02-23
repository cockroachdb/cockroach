// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockExecutor is a test executor that records calls.
type mockExecutor struct {
	calls []HookContext
	err   error // if set, Execute returns this error
}

func (m *mockExecutor) Execute(ctx context.Context, l *logger.Logger, hctx HookContext) error {
	m.calls = append(m.calls, hctx)
	return m.err
}

func makeResolvedEnv(vars ...envtypes.ResolvedVariable) envtypes.ResolvedEnvironment {
	return envtypes.ResolvedEnvironment{
		Name:      "test-env",
		Variables: vars,
	}
}

func makeTestProv() provmodels.Provisioning {
	return provmodels.Provisioning{
		ID:    uuid.MakeV4(),
		State: provmodels.ProvisioningStateProvisioning,
		Outputs: map[string]interface{}{
			"instances": []interface{}{
				map[string]interface{}{"public_ip": "1.2.3.4"},
				map[string]interface{}{"public_ip": "5.6.7.8"},
			},
		},
	}
}

func writeTestMetadata(t *testing.T, dir, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "template.yaml"), []byte(content), 0o644,
	))
}

func TestOrchestrator_RunPostApply_NoHooks(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, "name: test\ndescription: no hooks\n")

	registry := NewRegistry()
	orch := NewOrchestrator(registry)
	prov := makeTestProv()

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir,
		makeResolvedEnv(),
	)
	require.NoError(t, err)
}

func TestOrchestrator_RunPostApply_NoTemplateYAML(t *testing.T) {
	dir := t.TempDir()
	// No template.yaml written.

	registry := NewRegistry()
	orch := NewOrchestrator(registry)
	prov := makeTestProv()

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir,
		makeResolvedEnv(),
	)
	require.NoError(t, err) // graceful: no hooks to run
}

func TestOrchestrator_RunPostApply_ExecutesInOrder(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: ordered
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: ubuntu
hooks:
  - name: first
    type: run-command
    ssh: true
    command: echo first
    triggers: [post_apply]
  - name: second
    type: run-command
    ssh: true
    command: echo second
    triggers: [post_apply]
  - name: manual-only
    type: run-command
    ssh: true
    command: echo skip
    triggers: [manual]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()

	env := makeResolvedEnv(envtypes.ResolvedVariable{
		Key: "ssh_key", Value: "fake-key",
		Type: envmodels.VarTypeSecret,
	})

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.NoError(t, err)

	// Should execute 2 hooks (not the manual-only one), in order.
	require.Len(t, exec.calls, 2)
	assert.Equal(t, "first", exec.calls[0].Declaration.Name)
	assert.Equal(t, "second", exec.calls[1].Declaration.Name)
	assert.Equal(t, []string{"1.2.3.4", "5.6.7.8"}, exec.calls[0].MachineIPs)
	assert.Equal(t, "ubuntu", exec.calls[0].SSHUser)
}

func TestOrchestrator_RunPostApply_OptionalHookFailsContinues(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: optional-fail
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: root
hooks:
  - name: optional-hook
    type: run-command
    ssh: true
    command: echo fail
    optional: true
    triggers: [post_apply]
  - name: required-hook
    type: run-command
    ssh: true
    command: echo ok
    triggers: [post_apply]
`)

	// Single executor that fails on first call and succeeds on second.
	callCount := 0
	registry := NewRegistry()
	registry.Register("run-command", HookExecutorFunc(func(
		ctx context.Context, l *logger.Logger, hctx HookContext,
	) error {
		callCount++
		if callCount == 1 {
			return fmt.Errorf("boom")
		}
		return nil
	}))

	orch := NewOrchestrator(registry)
	prov := makeTestProv()
	env := makeResolvedEnv(envtypes.ResolvedVariable{
		Key: "ssh_key", Value: "fake-key",
		Type: envmodels.VarTypeSecret,
	})

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.NoError(t, err) // optional failure should not fail overall
	assert.Equal(t, 2, callCount)
}

func TestOrchestrator_RunPostApply_RequiredHookFailsReturnsError(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: required-fail
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: root
hooks:
  - name: required-hook
    type: run-command
    ssh: true
    command: echo fail
    optional: false
    triggers: [post_apply]
  - name: should-not-run
    type: run-command
    ssh: true
    command: echo nope
    triggers: [post_apply]
`)

	exec := &mockExecutor{err: fmt.Errorf("required failed")}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()
	env := makeResolvedEnv(envtypes.ResolvedVariable{
		Key: "ssh_key", Value: "fake-key",
		Type: envmodels.VarTypeSecret,
	})

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required-hook")
	// Only the first hook should have been called.
	assert.Len(t, exec.calls, 1)
}

func TestOrchestrator_RunPostApply_SSHKeyNotFound(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: missing-key
ssh:
  private_key_var: nonexistent_key
  machines: .instances[].public_ip
  user: root
hooks:
  - name: test-hook
    type: run-command
    ssh: true
    command: echo hi
    triggers: [post_apply]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir,
		makeResolvedEnv(), // no variables
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_key")
	assert.Empty(t, exec.calls)
}

func TestOrchestrator_RunPostApply_HookEnvResolved(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: env-test
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: root
hooks:
  - name: with-env
    type: run-command
    ssh: true
    command: echo $CA_CERT
    env:
      CA_CERT: ca_cert_var
    triggers: [post_apply]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()
	env := makeResolvedEnv(
		envtypes.ResolvedVariable{
			Key: "ssh_key", Value: "fake-key",
			Type: envmodels.VarTypeSecret,
		},
		envtypes.ResolvedVariable{
			Key: "ca_cert_var", Value: "CERT-CONTENT",
			Type: envmodels.VarTypeSecret,
		},
	)

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.NoError(t, err)
	require.Len(t, exec.calls, 1)
	assert.Equal(t, "CERT-CONTENT", exec.calls[0].HookEnv["CA_CERT"])
}

func TestOrchestrator_RunPostApply_MachineOverride(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: override-test
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: default-user
hooks:
  - name: custom-machines
    type: run-command
    ssh: true
    machines: .instances[0].public_ip
    user: custom-user
    command: echo hi
    triggers: [post_apply]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)

	prov := provmodels.Provisioning{
		ID: uuid.MakeV4(),
		Outputs: map[string]interface{}{
			"instances": []interface{}{
				map[string]interface{}{"public_ip": "10.0.0.1"},
				map[string]interface{}{"public_ip": "10.0.0.2"},
			},
		},
	}
	env := makeResolvedEnv(envtypes.ResolvedVariable{
		Key: "ssh_key", Value: "key",
		Type: envmodels.VarTypeSecret,
	})

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.NoError(t, err)
	require.Len(t, exec.calls, 1)
	// Hook-level override: only first IP, custom user.
	assert.Equal(t, []string{"10.0.0.1"}, exec.calls[0].MachineIPs)
	assert.Equal(t, "custom-user", exec.calls[0].SSHUser)
}

// HookExecutorFunc adapts a function to the HookExecutor interface.
type HookExecutorFunc func(
	ctx context.Context, l *logger.Logger, hctx HookContext,
) error

func (f HookExecutorFunc) Execute(ctx context.Context, l *logger.Logger, hctx HookContext) error {
	return f(ctx, l, hctx)
}

func TestOrchestrator_RunPostApply_RunCommandWithoutSSHFails(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: no-ssh-flag
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: root
hooks:
  - name: bad-hook
    type: run-command
    ssh: false
    command: echo hi
    triggers: [post_apply]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()
	env := makeResolvedEnv(envtypes.ResolvedVariable{
		Key: "ssh_key", Value: "fake-key",
		Type: envmodels.VarTypeSecret,
	})

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir, env,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "run-command type requires ssh: true")
	assert.Empty(t, exec.calls) // executor should not be called
}

func TestOrchestrator_RunPostApply_SSHHookWithoutSSHConfig(t *testing.T) {
	dir := t.TempDir()
	writeTestMetadata(t, dir, `
name: no-ssh-config
hooks:
  - name: needs-ssh
    type: run-command
    ssh: true
    command: echo hi
    triggers: [post_apply]
`)

	exec := &mockExecutor{}
	registry := NewRegistry()
	registry.Register("run-command", exec)
	orch := NewOrchestrator(registry)
	prov := makeTestProv()

	err := orch.RunPostApply(
		context.Background(), logger.DefaultLogger, prov, dir,
		makeResolvedEnv(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SSH hooks but no SSH configuration")
	assert.Empty(t, exec.calls)
}
