// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !linux && !darwin

package disk

import (
	"io/fs"

	"github.com/cockroachdb/pebble/vfs"
)

type defaultCollector struct{}

func (defaultCollector) collect([]*monitoredDisk) error {
	return nil
}

func newStatsCollector(fs vfs.FS) (*defaultCollector, error) {
	return &defaultCollector{}, nil
}

func deviceIDFromFileInfo(fs.FileInfo) DeviceID {
	return DeviceID{}
}
