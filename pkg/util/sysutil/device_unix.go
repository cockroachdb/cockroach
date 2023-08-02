// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !windows
// +build !windows

//lint:file-ignore unconvert (redundant conversions are necessary for cross-platform compatibility)

package sysutil

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// GetDeviceMap returns a map of device IDs to device names, for all physical
// devices.
func GetDeviceMap() (map[uint64]string, error) {
	entries, err := os.ReadDir("/dev")
	if err != nil {
		return nil, err
	}
	devices := make([]string, 0)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		name := info.Name()
		if strings.HasPrefix(name, "loop") ||
			strings.HasPrefix(name, "tty") ||
			strings.HasPrefix(name, "pty") ||
			strings.HasPrefix(name, "vcs") {
			// Skip over pseudo devices (loop devices, {pseudo,}terminals,
			// scrollback buffers).
			continue
		}
		devices = append(devices, filepath.Join("/dev", name))
	}

	deviceIDToName := make(map[uint64]string)
	for _, device := range devices {
		di, err := os.Stat(device)
		if err != nil {
			return nil, err
		}
		if di.Sys() != nil {
			if stat, ok := di.Sys().(*syscall.Stat_t); ok {
				// st_rdev is the device ID (https://linux.die.net/man/2/stat).
				// The names are specifically what comes after /dev/.
				deviceIDToName[uint64(stat.Rdev)] = di.Name()
			}
		}
	}

	return deviceIDToName, nil
}

// GetDeviceID returns the underlying device ID for the given file.
func GetDeviceID(info os.FileInfo) (uint64, bool) {
	if info.Sys() != nil {
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			return uint64(stat.Dev), true
		}
	}
	return 0, false
}
