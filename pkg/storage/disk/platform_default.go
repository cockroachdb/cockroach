// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !linux
// +build !linux

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
