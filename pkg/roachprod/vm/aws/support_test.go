// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {
	_, err := writeStartupScript("vm_name", "", vm.Zfs, false, false, "ubuntu")
	assert.NoError(t, err)
}
