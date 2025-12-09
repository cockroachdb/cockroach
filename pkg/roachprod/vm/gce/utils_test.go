// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// TestGenerateStartupScriptContent mainly tests the startup script tpl compiles.
func TestGenerateStartupScriptContent(t *testing.T) {
	// generateStartupScriptContent reads a public SSH key from the disk,
	// so we need to mock the file to avoid a panic.
	tempDir := t.TempDir()
	err := os.WriteFile(fmt.Sprintf("%s/id_rsa", tempDir), []byte("dummy private key"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/id_rsa.pub", tempDir), []byte("dummy public key"), 0644)
	require.NoError(t, err)

	config.SSHDirectory = tempDir

	// Actual test
	content, err := generateStartupScriptContent("", vm.Zfs, false, false, false, false)
	require.NoError(t, err)

	echotest.Require(t, content, datapathutils.TestDataPath(t, "startup_script"))
}

func TestIsValidSSHUser(t *testing.T) {
	testCases := []struct {
		name     string
		user     string
		expected bool
	}{
		{
			name:     "valid user",
			user:     "alice",
			expected: true,
		},
		{
			name:     "root is invalid",
			user:     "root",
			expected: false,
		},
		{
			name:     "shared user ubuntu is invalid",
			user:     config.SharedUser,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isValidSSHUser(tc.user)
			require.Equal(t, tc.expected, result)
		})
	}
}
