// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build windows
// +build windows

package sysutil

import (
	"fmt"
	"os"
)

// GetDeviceMap returns a map of device IDs to device names, for all physical
// devices.
func GetDeviceMap() (map[uint64]string, error) {
	return nil, fmt.Errorf("unsupported on windows")
}

// GetDeviceID returns the underlying device ID for the given file.
func GetDeviceID(info os.FileInfo) (uint64, bool) {
	return 0, false
}
