// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"testing"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateHookEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		meta    provmodels.TemplateMetadata
		env     envtypes.ResolvedEnvironment
		wantErr string
	}{
		{
			name: "no ssh, no hooks",
			meta: provmodels.TemplateMetadata{Name: "test"},
			env:  makeResolvedEnv(),
		},
		{
			name: "ssh private key present",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				SSH: &provmodels.SSHConfig{
					PrivateKeyVar: "ssh_key",
				},
			},
			env: makeResolvedEnv(envtypes.ResolvedVariable{
				Key: "ssh_key", Value: "fake",
				Type: envmodels.VarTypeSecret,
			}),
		},
		{
			name: "ssh private key missing",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				SSH: &provmodels.SSHConfig{
					PrivateKeyVar: "ssh_key",
				},
			},
			env:     makeResolvedEnv(),
			wantErr: "ssh.private_key_var",
		},
		{
			name: "hook env mapping present",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				Hooks: []provmodels.HookDeclaration{
					{
						Name: "install-certs",
						Type: "run-command",
						Env: provmodels.HookEnvMapping{
							"CA_CERT": "ca_cert_var",
						},
					},
				},
			},
			env: makeResolvedEnv(envtypes.ResolvedVariable{
				Key: "ca_cert_var", Value: "cert-data",
				Type: envmodels.VarTypeSecret,
			}),
		},
		{
			name: "hook env mapping missing",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				Hooks: []provmodels.HookDeclaration{
					{
						Name: "install-certs",
						Type: "run-command",
						Env: provmodels.HookEnvMapping{
							"CA_CERT": "ca_cert_var",
						},
					},
				},
			},
			env:     makeResolvedEnv(),
			wantErr: "ca_cert_var",
		},
		{
			name: "multiple missing vars all reported",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				SSH: &provmodels.SSHConfig{
					PrivateKeyVar: "ssh_key",
				},
				Hooks: []provmodels.HookDeclaration{
					{
						Name: "install-certs",
						Type: "run-command",
						Env: provmodels.HookEnvMapping{
							"CA_CERT": "ca_cert_var",
						},
					},
				},
			},
			env:     makeResolvedEnv(),
			wantErr: "ssh.private_key_var",
		},
		{
			name: "all vars present with ssh and hooks",
			meta: provmodels.TemplateMetadata{
				Name: "test",
				SSH: &provmodels.SSHConfig{
					PrivateKeyVar: "ssh_key",
					Machines:      ".instances[].ip",
					User:          "ubuntu",
				},
				Hooks: []provmodels.HookDeclaration{
					{
						Name: "install-certs",
						Type: "run-command",
						Env: provmodels.HookEnvMapping{
							"CA_CERT": "ca_cert_var",
						},
					},
				},
			},
			env: makeResolvedEnv(
				envtypes.ResolvedVariable{
					Key: "ssh_key", Value: "key",
					Type: envmodels.VarTypeSecret,
				},
				envtypes.ResolvedVariable{
					Key: "ca_cert_var", Value: "cert",
					Type: envmodels.VarTypeSecret,
				},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHookEnvironment(tt.meta, tt.env)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateHookEnvironment_MultipleErrors(t *testing.T) {
	meta := provmodels.TemplateMetadata{
		Name: "test",
		SSH: &provmodels.SSHConfig{
			PrivateKeyVar: "ssh_key",
		},
		Hooks: []provmodels.HookDeclaration{
			{
				Name: "hook-a",
				Type: "run-command",
				Env: provmodels.HookEnvMapping{
					"VAR_A": "missing_a",
				},
			},
			{
				Name: "hook-b",
				Type: "run-command",
				Env: provmodels.HookEnvMapping{
					"VAR_B": "missing_b",
				},
			},
		},
	}

	err := ValidateHookEnvironment(meta, makeResolvedEnv())
	require.Error(t, err)

	msg := err.Error()
	// All three missing vars should be mentioned.
	assert.Contains(t, msg, "ssh_key")
	assert.Contains(t, msg, "missing_a")
	assert.Contains(t, msg, "missing_b")
	assert.Contains(t, msg, "test-env")
}
