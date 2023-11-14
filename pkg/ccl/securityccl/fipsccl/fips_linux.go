// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package fipsccl

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
