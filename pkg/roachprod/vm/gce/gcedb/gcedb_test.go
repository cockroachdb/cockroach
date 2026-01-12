// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcedb

import (
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestGetMachineInfo(t *testing.T) {
	datadriven.RunTest(t, "testdata/machine_info", func(t *testing.T, d *datadriven.TestData) string {
		// The command is the machine type.
		machineType := strings.TrimSpace(d.Cmd)

		info, err := GetMachineInfo(machineType)
		if err != nil {
			return err.Error()
		}
		return info.String()
	})
}
