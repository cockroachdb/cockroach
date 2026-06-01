// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildScript_NoEnv(t *testing.T) {
	script, err := buildScript(nil, "echo hello")
	require.NoError(t, err)
	assert.Contains(t, script, "set -euo pipefail")
	assert.Contains(t, script, "echo hello")
	// No export lines.
	assert.Equal(t, 0, strings.Count(script, "export "))
}

func TestBuildScript_SingleEnv(t *testing.T) {
	env := map[string]string{"MY_VAR": "my_value"}
	script, err := buildScript(env, "echo $MY_VAR")
	require.NoError(t, err)
	assert.Contains(t, script, "export MY_VAR=")
	assert.Contains(t, script, "echo $MY_VAR")

	// Verify the base64 encoding is correct.
	encoded := base64.StdEncoding.EncodeToString([]byte("my_value"))
	assert.Contains(t, script, encoded)
}

func TestBuildScript_MultilineValue(t *testing.T) {
	env := map[string]string{"CERT": "line1\nline2\nline3"}
	script, err := buildScript(env, "cat /etc/cert")
	require.NoError(t, err)

	// The multiline value should be base64-encoded.
	encoded := base64.StdEncoding.EncodeToString([]byte("line1\nline2\nline3"))
	assert.Contains(t, script, encoded)
}

func TestBuildScript_SpecialChars(t *testing.T) {
	env := map[string]string{"DATA": `single'quote "double" $dollar \backslash`}
	script, err := buildScript(env, "echo done")
	require.NoError(t, err)

	// Base64 encoding handles special chars safely.
	encoded := base64.StdEncoding.EncodeToString(
		[]byte(`single'quote "double" $dollar \backslash`),
	)
	assert.Contains(t, script, encoded)
}

func TestBuildScript_MultipleEnvVars(t *testing.T) {
	env := map[string]string{
		"VAR_A": "aaa",
		"VAR_B": "bbb",
	}
	script, err := buildScript(env, "echo done")
	require.NoError(t, err)

	// Both should be present.
	assert.Contains(t, script, "export VAR_A=")
	assert.Contains(t, script, "export VAR_B=")
	assert.Equal(t, 2, strings.Count(script, "export "))
}

func TestBuildScript_ScriptEndsWithNewline(t *testing.T) {
	script, err := buildScript(nil, "my_command")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(script, "my_command\n"))
}

func TestBuildScript_InvalidEnvVarName(t *testing.T) {
	tests := []struct {
		name    string
		envName string
	}{
		{"shell injection", "FOO;rm -rf /"},
		{"starts with digit", "1VAR"},
		{"contains space", "MY VAR"},
		{"contains dash", "MY-VAR"},
		{"empty string", ""},
		{"contains dollar", "$VAR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{tt.envName: "value"}
			_, err := buildScript(env, "echo done")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid env var name")
		})
	}
}
