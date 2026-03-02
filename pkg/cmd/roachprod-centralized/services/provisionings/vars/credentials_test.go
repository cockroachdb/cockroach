// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vars

import (
	"os"
	"path/filepath"
	"testing"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareCredentialFiles_WritesFileAndReplacesEnvVar(t *testing.T) {
	workingDir := t.TempDir()
	credContent := `{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n"}`

	envVars := map[string]string{
		"GOOGLE_APPLICATION_CREDENTIALS": credContent,
		"OTHER_VAR":                      "keep-me",
	}
	resolved := types.ResolvedEnvironment{
		Name: "test-env",
		Variables: []types.ResolvedVariable{
			{Key: "GOOGLE_APPLICATION_CREDENTIALS", Value: credContent, Type: envmodels.VarTypeSecretFile},
			{Key: "OTHER_VAR", Value: "keep-me", Type: envmodels.VarTypePlaintext},
		},
	}

	updated, cleanup, err := PrepareCredentialFiles(workingDir, envVars, resolved)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	// The original map must not be mutated.
	assert.Equal(t, credContent, envVars["GOOGLE_APPLICATION_CREDENTIALS"])

	// The returned map should have the file path.
	credPath := updated["GOOGLE_APPLICATION_CREDENTIALS"]
	assert.NotEqual(t, credContent, credPath, "env var should be replaced with file path")
	expectedPath := filepath.Join(workingDir, credentialsDir, "GOOGLE_APPLICATION_CREDENTIALS")
	assert.Equal(t, expectedPath, credPath)

	// The file should exist with the correct content and permissions.
	data, err := os.ReadFile(credPath)
	require.NoError(t, err)
	assert.Equal(t, credContent, string(data))

	info, err := os.Stat(credPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

	// Non-secret_file vars are unchanged.
	assert.Equal(t, "keep-me", updated["OTHER_VAR"])
}

func TestPrepareCredentialFiles_NoSecretFileVars(t *testing.T) {
	workingDir := t.TempDir()

	envVars := map[string]string{
		"AWS_ACCESS_KEY_ID": "AKIA...",
	}
	resolved := types.ResolvedEnvironment{
		Name: "test-env",
		Variables: []types.ResolvedVariable{
			{Key: "AWS_ACCESS_KEY_ID", Value: "AKIA...", Type: envmodels.VarTypeSecret},
		},
	}

	updated, cleanup, err := PrepareCredentialFiles(workingDir, envVars, resolved)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	// No credential files should be created.
	_, statErr := os.Stat(filepath.Join(workingDir, credentialsDir))
	assert.True(t, os.IsNotExist(statErr))

	// Env vars unchanged.
	assert.Equal(t, "AKIA...", updated["AWS_ACCESS_KEY_ID"])
}

func TestPrepareCredentialFiles_CleanupRemovesFiles(t *testing.T) {
	workingDir := t.TempDir()
	credContent := `{"type":"service_account"}`

	envVars := map[string]string{
		"GOOGLE_APPLICATION_CREDENTIALS": credContent,
	}
	resolved := types.ResolvedEnvironment{
		Name: "test-env",
		Variables: []types.ResolvedVariable{
			{Key: "GOOGLE_APPLICATION_CREDENTIALS", Value: credContent, Type: envmodels.VarTypeSecretFile},
		},
	}

	updated, cleanup, err := PrepareCredentialFiles(workingDir, envVars, resolved)
	require.NoError(t, err)

	// File exists before cleanup.
	credPath := updated["GOOGLE_APPLICATION_CREDENTIALS"]
	_, statErr := os.Stat(credPath)
	require.NoError(t, statErr)

	// After cleanup, the credentials directory is removed.
	require.NoError(t, cleanup())
	_, statErr = os.Stat(filepath.Join(workingDir, credentialsDir))
	assert.True(t, os.IsNotExist(statErr))
}

func TestPrepareCredentialFiles_MultipleSecretFiles(t *testing.T) {
	workingDir := t.TempDir()

	envVars := map[string]string{
		"CRED_A": "content-a",
		"CRED_B": "content-b",
	}
	resolved := types.ResolvedEnvironment{
		Name: "test-env",
		Variables: []types.ResolvedVariable{
			{Key: "CRED_A", Value: "content-a", Type: envmodels.VarTypeSecretFile},
			{Key: "CRED_B", Value: "content-b", Type: envmodels.VarTypeSecretFile},
		},
	}

	updated, cleanup, err := PrepareCredentialFiles(workingDir, envVars, resolved)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	dataA, err := os.ReadFile(updated["CRED_A"])
	require.NoError(t, err)
	assert.Equal(t, "content-a", string(dataA))

	dataB, err := os.ReadFile(updated["CRED_B"])
	require.NoError(t, err)
	assert.Equal(t, "content-b", string(dataB))
}
