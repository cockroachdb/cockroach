// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
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
func (s *linuxStatsCollector) collect(
	disks []*monitoredDisk, now time.Time,
) (countCollected int, err error) {
	var n int
	for {
		n, err = s.File.ReadAt(s.buf, 0)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, err
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
	return parseDiskStats(s.buf[:n], disks, now)
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

func deviceIDFromFileInfo(finfo fs.FileInfo, path string) DeviceID {
	ctx := context.TODO()
	statInfo := finfo.Sys().(*sysutil.StatT)
	major := unix.Major(statInfo.Dev)
	minor := unix.Minor(statInfo.Dev)

	// Per /usr/include/linux/major.h and Documentation/admin-guide/devices.rst:
	switch major {
	case 0: // UNNAMED_MAJOR

		// Perform additional lookups for unknown device types
		var statfs sysutil.StatfsT
		err := sysutil.Statfs(path, &statfs)
		if err != nil {
			maybeWarnf(ctx, "unable to statfs(2) path %q (%d:%d): %v", path, major, minor, err)
			return DeviceID{major, minor}
		}

		switch statfs.Type {
		case 0x2fc12fc1: // ZFS_SUPER_MAGIC from include/sys/fs/zfs.h
			major, minor, err = deviceIDForZFS(path)
			if err != nil {
				maybeWarnf(ctx, "zfs: unable to find device ID for %q: %v", path, err)
			} else {
				maybeInfof(ctx, "zfs: mapping %q to diskstats device %d:%d", path, major, minor)
			}

			id := DeviceID{
				major: major,
				minor: minor,
			}
			return id

		case 0x58465342: // XFS_SUPER_MAGIC	from linux/magic.h "XFSB"
			maybeWarnf(ctx, "xfs: unable to find device ID for %q: %v", path, err)

		default:
			maybeWarnf(ctx, "unsupported file system type %x for path (%d:%d) %q", statfs.Type, major, minor, path)
		}

	case 259: // BLOCK_EXT_MAJOR=259

		// NOTE: Major device 259 is the happy path for ext4 and xfs filesystems: no
		// additional handling is required.

		maybeInfof(ctx, "mapping %q to diskstats device %d:%d", path, major, minor)

	default:
		maybeWarnf(ctx, "unsupported device type %d:%d for store at %q", major, minor, path)
	}

	id := DeviceID{
		major: major,
		minor: minor,
	}
	return id
}

type _ZPoolName string

func deviceIDForZFS(path string) (uint32, uint32, error) {
	zpoolName, err := zfsGetPoolName(path)
	if err != nil {
		return 0, 0, errors.Newf("unable to find the zpool for %q: %v", path, err) // nolint:errwrap
	}

	// If there are multiple devices for a zpool, an error is returned along with
	// a device name.  Continue resolving the device's major:minor numbers,
	// despite the multiple drives.
	devName, err := zpoolGetDevice(zpoolName)
	if err != nil && devName == "" {
		return 0, 0, errors.Newf("unable to find the device for pool %q: %v", zpoolName, err) // nolint:errwrap
	}

	major, minor, err := getDeviceID(devName)
	if err != nil {
		return 0, 0, errors.Newf("unable to find the device numbers for device %q: %v", devName, err) // nolint:errwrap
	}

	return major, minor, nil
}

func zfsGetPoolName(path string) (_ZPoolName, error) {
	out, err := exec.Command("df", "--no-sync", "--output=source,fstype", path).Output()
	if err != nil {
		return "", errors.Newf("unable to exec df(1): %v", err) // nolint:errwrap
	}

	return zfsParseDF(out)
}

func zfsParseDF(df []byte) (_ZPoolName, error) {
	lines := strings.Split(strings.TrimSpace(string(df)), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("unexpected df(1) output: %q", df)
	}

	fields := strings.Fields(lines[1])
	if len(fields) != 2 {
		return "", fmt.Errorf("unexpected df(1) fields (expected 2, got %d): %q", len(fields), lines[1])
	}

	if fields[1] != "zfs" {
		return "", fmt.Errorf("unexpected df(1) fields (expected 2, got %d): %q", len(fields), lines[1])
	}

	// Need to accept inputs formed like "data1" and "data1/crdb-logs"
	poolName := strings.Split(fields[0], "/")[0]

	return _ZPoolName(poolName), nil
}

func zpoolGetDevice(poolName _ZPoolName) (string, error) {
	ctx := context.TODO()

	out, err := exec.Command("zpool", "status", "-pPL", string(poolName)).Output()
	if err != nil {
		return "", errors.Newf("unable to find the devices attached to pool %q: %v", poolName, err) // nolint:errwrap
	}

	return zpoolParseStatus(ctx, poolName, out)
}

func zpoolParseStatus(ctx context.Context, poolName _ZPoolName, output []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	var devName string
	var devCount int
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == "ONLINE" && strings.HasPrefix(fields[0], "/dev/") {
			devCount++
			if devName == "" {
				devName = stripDevicePartition(fields[0])
			} else {
				maybeWarnf(ctx, "unsupported configuration: multiple devices (i.e. %q, %q) detected for zpool %q", devName, fields[0], string(poolName))
			}
		}
	}

	switch {
	case devCount == 1:
		return devName, nil
	case devCount > 1:
		return devName, errors.Newf("unsupported configuration: %d devices detected for zpool %q", devCount, string(poolName))
	default:
		return "", fmt.Errorf("no device found for zpool %q", poolName)
	}
}

var (
	nvmePartitionRegex = regexp.MustCompile(`^(nvme\d+n\d+)(p\d+)?$`)
	scsiPartitionRegex = regexp.MustCompile(`^(ram|loop|fd|(h|s|v|xv)d[a-z])(\d+)?$`)
)

// stripDevicePartition removes partition suffix from a device path.
func stripDevicePartition(devicePath string) string {
	base := filepath.Base(devicePath)

	nvmeMatches := nvmePartitionRegex.FindStringSubmatch(base)
	if len(nvmeMatches) == 3 {
		return nvmeMatches[1]
	}

	scsiMatches := scsiPartitionRegex.FindStringSubmatch(base)
	if len(scsiMatches) >= 3 {
		return scsiMatches[1]
	}

	// If no match, return original device path
	return devicePath
}

// getDeviceID takes a block device name (e.g., nvme5n1) and returns its major and minor numbers.
func getDeviceID(devPath string) (uint32, uint32, error) {
	devName := filepath.Base(devPath)
	devFilePath := fmt.Sprintf("/sys/block/%s/dev", devName)
	data, err := os.ReadFile(devFilePath)
	if err != nil {
		return 0, 0, errors.Newf("unable to read %q: %v", devFilePath, err) // nolint:errwrap
	}

	return parseDeviceID(devFilePath, data)
}

func parseDeviceID(devFilePath string, data []byte) (uint32, uint32, error) {
	devStr := strings.TrimSpace(string(data))
	parts := strings.Split(devStr, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("unexpected device string format in %q: %s", devFilePath, devStr)
	}

	var maj, min uint32
	_, err := fmt.Sscanf(devStr, "%d:%d", &maj, &min)
	if err != nil {
		return 0, 0, errors.Newf("failed parsing device numbers: %v", err) // nolint:errwrap
	}

	return maj, min, nil
}

// maybeWarnf is a convenience function to prevent panicing during bootstrap
// from using logging before it is setup.
func maybeWarnf(ctx context.Context, format string, args ...interface{}) {
	if active, _ := log.IsActive(); active {
		log.Ops.WarningfDepth(ctx, 1, format, args...)
	}
}

// maybeInfof is a convenience function to prevent panicing during bootstrap
// from using logging before it is setup.
func maybeInfof(ctx context.Context, format string, args ...interface{}) {
	if active, _ := log.IsActive(); active {
		log.Ops.InfofDepth(ctx, 1, format, args...)
	}
}
