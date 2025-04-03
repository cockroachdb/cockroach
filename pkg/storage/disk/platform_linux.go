// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"io"
	"io/fs"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

// A linuxStatsCollector collects disk stats from /proc/diskstats. It keeps
// /proc/diskstats open, issuing `ReadAt` calls to re-read stats.
type linuxStatsCollector struct {
	vfs.File
	buf []byte
}

// collect collects disk stats for the identified devices.
func (s *linuxStatsCollector) collect(disks []*monitoredDisk) error {
	var n int
	var err error
	for {
		n, err = s.File.ReadAt(s.buf, 0)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		// err == nil
		//
		// NB: ReadAt is required to return a non-nil error when it returns n <
		// len(s.buf). A nil error indicates a full len(s.buf) bytes were read,
		// and the diskstats file does not fit in our current buffer.
		//
		// We want to grow the buffer to be large enough to fit the entirety of
		// the file. This is required for consistency. We're only guaranteed a
		// consistent read if we read the entirety of the diskstats file in a
		// single read. Reallocate (doubling) the buffer and continue.
		s.buf = make([]byte, len(s.buf)*2)
	}
	return parseDiskStats(s.buf[:n], disks, timeutil.Now())
}

func newStatsCollector(fs vfs.FS) (*linuxStatsCollector, error) {
	file, err := fs.Open("/proc/diskstats")
	if err != nil {
		return nil, errors.Wrap(err, "opening /proc/diskstats")
	}
	return &linuxStatsCollector{
		File: file,
		buf:  make([]byte, 64),
	}, nil
}

func deviceIDFromFileInfo(finfo fs.FileInfo) DeviceID {
	statInfo := finfo.Sys().(*sysutil.StatT)
	id := DeviceID{
		major: unix.Major(statInfo.Dev),
		minor: unix.Minor(statInfo.Dev),
	}
	return id
}
