// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {
	file, err := writeStartupScript("vm_name", "", vm.Zfs, false, false, "ubuntu")
	require.NoError(t, err)

	f, err := os.ReadFile(file)
	require.NoError(t, err)

	echotest.Require(t, string(f), datapathutils.TestDataPath(t, "startup_script"))
}
