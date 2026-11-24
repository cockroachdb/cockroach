// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fips

import (
	"fmt"
	"os"

	"github.com/cockroachdb/errors"
)

const fipsSysctlFilename = "/proc/sys/crypto/fips_enabled"

// IsKernelEnabled returns true if FIPS mode is enabled in the kernel
// (by reading the crypto.fips_enabled sysctl).
func IsKernelEnabled() (bool, error) {
	data, err := os.ReadFile(fipsSysctlFilename)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, errors.New("sysctl file empty")
	}
	if data[0] == '1' {
		return true, nil
	}
	return false, fmt.Errorf("sysctl value: %q", data)
}
