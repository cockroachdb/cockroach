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
	"github.com/stretchr/testify/assert"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {
	// writeStartupScript reads a public SSH key from the disk,
	// so we need to mock the file to avoid a panic.
	tempDir := t.TempDir()
	err := os.WriteFile(fmt.Sprintf("%s/id_rsa", tempDir), []byte("dummy private key"), 0644)
	assert.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/id_rsa.pub", tempDir), []byte("dummy public key"), 0644)
	assert.NoError(t, err)

	config.SSHDirectory = tempDir

	// Actual test
	_, err = writeStartupScript("", vm.Zfs, false, false, false)
	assert.NoError(t, err)
}
