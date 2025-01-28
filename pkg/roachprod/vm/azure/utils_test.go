// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {

	_, err := evalStartupTemplate(
		azureStartupArgs{
			StartupArgs: vm.StartupArgs{
				VMName:               "vm_name",
				SharedUser:           "ubuntu",
				DisksInitializedFile: vm.DisksInitializedFile,
				OSInitializedFile:    vm.OSInitializedFile,
				StartupLogs:          vm.StartupLogs,
				ChronyServers: []string{
					"time1.google.com",
				},
			},
		})
	assert.NoError(t, err)
}
