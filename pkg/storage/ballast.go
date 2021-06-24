// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

// IsDiskFull examines the store indicated by spec, determining whether the
// store's underlying disk is out of disk space. A disk is considered to be
// full if available capacity is less than half of the store's ballast size.
//
// If the current on-disk ballast is larger than the ballast size configured
// through spec, IsDiskFull will truncate the ballast to the configured size.
func IsDiskFull(fs vfs.FS, spec base.StoreSpec) (bool, error) {
	diskUsage, err := fs.GetDiskUsage(spec.Path)
	if err != nil {
		return false, errors.Wrapf(err, "retrieving disk usage: %s", spec.Path)
	}
	desiredSizeBytes := BallastSizeBytes(spec, diskUsage)

	ballastPath := base.EmergencyBallastFile(fs.PathJoin, spec.Path)
	var currentSizeBytes int64
	if fi, err := fs.Stat(ballastPath); err != nil && !oserror.IsNotExist(err) {
		return false, err
	} else if err == nil {
		currentSizeBytes = fi.Size()
	}

	// If the ballast is larger than desired, truncate it now in case the
	// freed disk space will allow us to start. Generally, re-sizing the
	// ballast is the responsibility of the Engine.
	if currentSizeBytes > desiredSizeBytes {
		if err := fs.MkdirAll(fs.PathDir(ballastPath), 0755); err != nil {
			return false, err
		}
		// TODO(jackson): Expose Truncate on vfs.FS.
		if err := os.Truncate(ballastPath, desiredSizeBytes); err != nil {
			return false, errors.Wrap(err, "truncating ballast")
		}
		diskUsage, err = fs.GetDiskUsage(spec.Path)
		if err != nil {
			return false, errors.Wrapf(err, "retrieving disk usage: %s", spec.Path)
		}
	}

	// If the filesystem reports less than half the disk space available,
	// consider the disk full. If the ballast hasn't been removed yet,
	// removing it will free enough disk space to start. We don't use exactly
	// the ballast size in case some of the headroom gets consumed elsewhere:
	// eg, the operator's shell history, system logs, copy-on-write filesystem
	// metadata, etc.
	return diskUsage.AvailBytes < uint64(desiredSizeBytes/2), nil
}

// BallastSizeBytes returns the desired size of the emergency ballast,
// calculated from the provided store spec and disk usage. If the store spec
// contains an explicit ballast size (either in bytes or as a perecentage of
// the disk's total capacity), the store spec's size is used. Otherwise,
// BallastSizeBytes returns 1GiB or 1% of total capacity, whichever is
// smaller.
func BallastSizeBytes(spec base.StoreSpec, diskUsage vfs.DiskUsage) int64 {
	if spec.BallastSize != nil {
		v := spec.BallastSize.InBytes
		if spec.BallastSize.Percent != 0 {
			v = int64(float64(diskUsage.TotalBytes) * spec.BallastSize.Percent / 100)
		}
		return v
	}

	// Default to a 1% or 1GiB ballast, whichever is smaller.
	var v int64 = 1 << 30 // 1 GiB
	if p := int64(float64(diskUsage.TotalBytes) * 0.01); p < v {
		v = p
	}
	return v
}

func maybeEstablishBallast(
	fs vfs.FS, ballastPath string, ballastSizeBytes int64, diskUsage vfs.DiskUsage,
) (resized bool, err error) {
	var currentSizeBytes int64
	fi, err := fs.Stat(ballastPath)
	if err != nil && !oserror.IsNotExist(err) {
		return false, err
	} else if err == nil {
		currentSizeBytes = fi.Size()
	}

	switch {
	case currentSizeBytes > ballastSizeBytes:
		// If the current ballast is too big, shrink it regardless of current
		// disk space availability.
		return true, sysutil.ResizeLargeFile(ballastPath, ballastSizeBytes)
	case currentSizeBytes < ballastSizeBytes:
		// We need to either create the ballast or extend the current ballast
		// to make it larger. The ballast may have been intentionally removed
		// to enable recovery. Only create/extend the ballast if there's
		// sufficient disk space.
		extendBytes := ballastSizeBytes - currentSizeBytes

		// If available disk space is >= 4x the required amount, create the
		// ballast.
		if extendBytes <= int64(diskUsage.AvailBytes)/4 {
			return true, sysutil.ResizeLargeFile(ballastPath, ballastSizeBytes)
		}

		// If the user configured a really large ballast, we might not ever
		// have >= 4x the required amount available. Also allow extending the
		// ballast if we will have 10 GiB available after the extension.
		if int64(diskUsage.AvailBytes)-extendBytes > (10 << 30 /* 10 GiB */) {
			return true, sysutil.ResizeLargeFile(ballastPath, ballastSizeBytes)
		}

		return false, nil
	default:
		return false, nil
	}
}
