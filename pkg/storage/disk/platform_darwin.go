// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build darwin

package disk

import (
	"io/fs"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

type darwinCollector struct{}

func (darwinCollector) collect([]*monitoredDisk) error {
	return nil
}

func newStatsCollector(fs vfs.FS) (*darwinCollector, error) {
	return &darwinCollector{}, nil
}

func deviceIDFromFileInfo(finfo fs.FileInfo) DeviceID {
	statInfo := finfo.Sys().(*sysutil.StatT)
	id := DeviceID{
		major: unix.Major(uint64(statInfo.Dev)),
		minor: unix.Minor(uint64(statInfo.Dev)),
	}
	return id
}
