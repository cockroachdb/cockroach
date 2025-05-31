// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {

	content, err := (&Provider{}).startupScript(
		startupArgs{
			StartupArgs: vm.StartupArgs{
				VMName:               "vm_name",
				SharedUser:           "ubuntu",
				DisksInitializedFile: vm.DisksInitializedFile,
				OSInitializedFile:    vm.OSInitializedFile,
				StartupLogs:          vm.StartupLogs,
				ChronyServers: []string{
					defaultNTPServer,
				},
			},
		},
	)
	require.NoError(t, err)

	echotest.Require(t, content, datapathutils.TestDataPath(t, "startup_script"))
}
