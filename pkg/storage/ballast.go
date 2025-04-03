// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

// ballastsEnabled allows overriding the automatic creation of the ballast
// files through an environment variable. Developers working on CockroachDB
// may want to include `COCKROACH_AUTO_BALLAST=false` in their environment to
// prevent the automatic creation of large ballast files on their local
// filesystem.
var ballastsEnabled bool = envutil.EnvOrDefaultBool("COCKROACH_AUTO_BALLAST", true)

// IsDiskFull examines the store indicated by spec, determining whether the
// store's underlying disk is out of disk space. A disk is considered to be
// full if available capacity is less than half of the store's ballast size.
//
// If the current on-disk ballast does not match the configured ballast size
// in spec, IsDiskFull will resize the file if available capacity allows.
func IsDiskFull(fs vfs.FS, spec base.StoreSpec) (bool, error) {
	if spec.InMemory {
		return false, nil
	}

	// The store directory might not exist yet. We don't want to try to create
	// it yet, because there might not be any disk space to do so. Check the
	// disk usage on the first parent that exists.
	path := spec.Path
	diskUsage, err := fs.GetDiskUsage(path)
	for oserror.IsNotExist(err) {
		if parentPath := fs.PathDir(path); parentPath == path {
			break
		} else {
			path = parentPath
		}
		diskUsage, err = fs.GetDiskUsage(path)
	}
	if err != nil {
		return false, errors.Wrapf(err, "retrieving disk usage: %s", spec.Path)
	}

	// Try to resize the ballast now, if necessary. This is necessary to
	// truncate the ballast if a new, lower ballast size was provided,
	// and the disk space freed by truncation will allow us to start. If
	// we need to create or grow the ballast but are unable because
	// there's insufficient disk space, it'll be resized by the periodic
	// capacity calculations when the conditions are met.
	desiredSizeBytes := BallastSizeBytes(spec, diskUsage)
	ballastPath := base.EmergencyBallastFile(fs.PathJoin, spec.Path)
	if resized, err := maybeEstablishBallast(fs, ballastPath, desiredSizeBytes, diskUsage); err != nil {
		return false, err
	} else if resized {
		diskUsage, err = fs.GetDiskUsage(path)
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
// contains an explicit ballast size (either in bytes or as a percentage of
// the disk's total capacity), the store spec's size is used. Otherwise,
// BallastSizeBytes returns 1GiB or 1% of total capacity, whichever is
// smaller.
func BallastSizeBytes(spec base.StoreSpec, diskUsage vfs.DiskUsage) int64 {
	if spec.BallastSize != nil {
		v := spec.BallastSize.Capacity
		if spec.BallastSize.Percent != 0 {
			v = int64(float64(diskUsage.TotalBytes) * spec.BallastSize.Percent / 100)
		}
		return v
	}

	// Default to a 1% or 1GiB ballast, whichever is smaller.
	var v int64 = 1 << 30 // 1 GiB
	if p := int64(float64(diskUsage.TotalBytes) * 0.01); v > p {
		v = p
	}
	return v
}

// SecondaryCacheBytes returns the desired size of the secondary cache, calculated
// from the provided store spec and disk usage. If the store spec contains an
// explicit ballast size (either in bytes or as a percentage of the disk's total
// capacity), that size is used. A zero value for cacheSize results in no
// secondary cache.
func SecondaryCacheBytes(cacheSize storagepb.SizeSpec, diskUsage vfs.DiskUsage) int64 {
	v := cacheSize.Capacity
	if cacheSize.Percent != 0 {
		v = int64(float64(diskUsage.TotalBytes) * cacheSize.Percent / 100)
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
		// TODO(jackson): Expose Truncate on vfs.FS.
		return true, sysutil.ResizeLargeFile(ballastPath, ballastSizeBytes)
	case currentSizeBytes < ballastSizeBytes && ballastsEnabled:
		if err := fs.MkdirAll(fs.PathDir(ballastPath), 0755); err != nil {
			return false, errors.Wrap(err, "creating data directory")
		}
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
		// have >= 4x the required amount available. Larger ballast sizes (eg,
		// 5%, 10%) are not unreasonably large, but it's possible that after
		// recovery available capacity won't exceed 4x the ballast sizes (eg,
		// 20%, 40%). Allow extending the ballast if we will have 10 GiB
		// available after the extension to account for these large ballasts.
		if int64(diskUsage.AvailBytes)-extendBytes > (10 << 30 /* 10 GiB */) {
			return true, sysutil.ResizeLargeFile(ballastPath, ballastSizeBytes)
		}

		return false, nil
	default:
		return false, nil
	}
}
