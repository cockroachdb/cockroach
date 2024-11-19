// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cgroups

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// Filenames
	cgroupV1MemStatFilename      = "memory.stat"
	cgroupV2MemStatFilename      = "memory.stat"
	cgroupV2MemLimitFilename     = "memory.max"
	cgroupV1MemUsageFilename     = "memory.usage_in_bytes"
	cgroupV2MemUsageFilename     = "memory.current"
	cgroupV1CPUQuotaFilename     = "cpu.cfs_quota_us"
	cgroupV1CPUPeriodFilename    = "cpu.cfs_period_us"
	cgroupV1CPUSysUsageFilename  = "cpuacct.usage_sys"
	cgroupV1CPUUserUsageFilename = "cpuacct.usage_user"
	cgroupV2CPUMaxFilename       = "cpu.max"
	cgroupV2CPUStatFilename      = "cpu.stat"

	// {memory|cpu}.stat file keys
	//
	// key for # of bytes of file-backed memory on inactive LRU list in cgroupv1
	cgroupV1MemInactiveFileUsageStatKey = "total_inactive_file"
	// key for # of bytes of file-backed memory on inactive LRU list in cgroupv2
	cgroupV2MemInactiveFileUsageStatKey = "inactive_file"
	cgroupV1MemLimitStatKey             = "hierarchical_memory_limit"
)

// GetMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process
func GetMemoryLimit() (limit int64, warnings string, err error) {
	return getCgroupMemLimit("/")
}

// GetMemoryUsage attempts to retrieve the cgroup memory usage value (in bytes)
// for the current process.
func GetMemoryUsage() (usage int64, warnings string, err error) {
	return getCgroupMemUsage("/")
}

// GetMemoryInactiveFileUsage attempts to retrieve the cgroup memory
// inactive_file value (in bytes) for the current process. This represents the #
// of bytes of file-backed memory on inactive LRU list.
//
// In the eyes of container providers such as Docker, this value can be
// subtracted from the value returned by GetMemoryUsage to calculate the current
// memory usage.
//
// See: https://docs.docker.com/engine/reference/commandline/stats/#extended-description
func GetMemoryInactiveFileUsage() (usage int64, warnings string, err error) {
	return getCgroupMemInactiveFileUsage("/")
}

// getCgroupMemInactiveFileUsage reads the memory cgroup's current inactive
// file-backed memory usage (in bytes) on the inactive LRU list for both cgroups
// v1 and v2. The associated files and keys are:
//
//	cgroupv1: cgroupV1MemInactiveFileUsageStatKey in cgroupV1MemStatFilename
//	cgroupv2: cgroupV2MemInactiveFileUsageStatKey in cgroupV2MemStatFilename
//
// The `root` parameter is set to "/" in production code and exists only for
// testing. The cgroup inactive file usage detection path is implemented as:
//
//	/proc/self/cgroup file
//	|->	/proc/self/mountinfo mounts
//		|->	cgroup version
//	 		|->	version specific usage check
func getCgroupMemInactiveFileUsage(root string) (usage int64, warnings string, err error) {
	path, err := detectCntrlPath(filepath.Join(root, "/proc/self/cgroup"), "memory")
	if err != nil {
		return 0, "", err
	}

	// If the path is empty, it indicates that no memory controller was detected.
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	versionedMounts, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "memory")
	if err != nil {
		return 0, "", err
	}
	if len(versionedMounts) == 2 {
		// Look up against V2 first. Fall back to V1.
		usage, warnings, err = detectMemInactiveFileUsageInV2(filepath.Join(root, versionedMounts[1].mount, path))
		if err != nil {
			usage, warnings, err = detectMemInactiveFileUsageInV1(filepath.Join(root, versionedMounts[0].mount))
		}
	} else {
		if len(versionedMounts) != 1 {
			return usage, warnings, errors.AssertionFailedf("expected len(versionedMounts)==1 instead of %d", len(versionedMounts))
		}
		switch versionedMounts[0].version {
		case 1:
			usage, warnings, err = detectMemInactiveFileUsageInV1(filepath.Join(root, versionedMounts[0].mount))
		case 2:
			usage, warnings, err = detectMemInactiveFileUsageInV2(filepath.Join(root, versionedMounts[0].mount, path))
		}
	}

	return usage, warnings, err
}

// getCgroupMemUsage reads the memory cgroup's current memory usage (in bytes)
// for both cgroups v1 and v2. The associated files are:
//
//	cgroupv1: cgroupV1MemUsageFilename
//	cgroupv2: cgroupV2MemUsageFilename
//
// The `root` parameter is set to "/" in production code and exists only for
// testing. The cgroup memory usage detection path is implemented here as:
//
//	/proc/self/cgroup file
//	|->	/proc/self/mountinfo mounts
//		|->	cgroup version
//	 		|->	version specific usage check
func getCgroupMemUsage(root string) (usage int64, warnings string, err error) {
	path, err := detectCntrlPath(filepath.Join(root, "/proc/self/cgroup"), "memory")
	if err != nil {
		return 0, "", err
	}

	// If the path is empty, it indicates that no memory controller was detected.
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	versionedMounts, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "memory")
	if err != nil {
		return 0, "", err
	}

	if len(versionedMounts) == 2 {
		// Look up against V2 first. Fall back to V1.
		usage, warnings, err = detectMemUsageInV2(filepath.Join(root, versionedMounts[1].mount, path))
		if err != nil {
			usage, warnings, err = detectMemUsageInV1(filepath.Join(root, versionedMounts[0].mount))
		}
	} else {
		if len(versionedMounts) != 1 {
			return usage, warnings, errors.AssertionFailedf("expected len(versionedMounts)==1 instead of %d", len(versionedMounts))
		}
		switch versionedMounts[0].version {
		case 1:
			usage, warnings, err = detectMemUsageInV1(filepath.Join(root, versionedMounts[0].mount))
		case 2:
			usage, warnings, err = detectMemUsageInV2(filepath.Join(root, versionedMounts[0].mount, path))
		}
	}

	return usage, warnings, err
}

// getCgroupMemLimit reads the memory cgroup's memory limit (in bytes) for both
// cgroups v1 and v2. The associated files (and for cgroupv2, keys) are:
//
//	cgroupv1: cgroupV2MemLimitFilename
//	cgroupv2: cgroupV1MemLimitStatKey in cgroupV2MemStatFilename
//
// The `root` parameter is set to "/" in production code and exists only for
// testing. The cgroup memory limit detection path is implemented here as:
//
//	/proc/self/cgroup file
//	|->	/proc/self/mountinfo mounts
//		|->	cgroup version
//	 		|->	version specific limit check
func getCgroupMemLimit(root string) (limit int64, warnings string, err error) {
	path, err := detectCntrlPath(filepath.Join(root, "/proc/self/cgroup"), "memory")
	if err != nil {
		return 0, "", err
	}

	// If the path is empty, it indicates that no memory controller was detected.
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	versionedMounts, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "memory")
	if err != nil {
		return 0, "", err
	}

	if len(versionedMounts) == 2 {
		// Look up against V2 first. Fall back to V1.
		limit, warnings, err = detectMemLimitInV2(filepath.Join(root, versionedMounts[1].mount, path))
		if err != nil {
			limit, warnings, err = detectMemLimitInV1(filepath.Join(root, versionedMounts[0].mount))
		}
	} else {
		if len(versionedMounts) != 1 {
			return limit, warnings, errors.AssertionFailedf("expected len(versionedMounts)==1 instead of %d", len(versionedMounts))
		}
		switch versionedMounts[0].version {
		case 1:
			limit, warnings, err = detectMemLimitInV1(filepath.Join(root, versionedMounts[0].mount))
		case 2:
			limit, warnings, err = detectMemLimitInV2(filepath.Join(root, versionedMounts[0].mount, path))
		}
	}

	return limit, warnings, err
}

func readFile(filepath string) (res []byte, err error) {
	var f *os.File
	f, err = os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.CombineErrors(err, f.Close())
	}()
	res, err = io.ReadAll(f)
	return res, err
}

func cgroupFileToUint64(filepath, desc string) (res uint64, err error) {
	contents, err := readFile(filepath)
	if err != nil {
		return 0, errors.Wrapf(err, "error when reading %s from cgroup v1 at %s", desc, filepath)
	}
	res, err = strconv.ParseUint(string(bytes.TrimSpace(contents)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "error when parsing %s from cgroup v1 at %s", desc, filepath)
	}
	return res, err
}

func cgroupFileToInt64(filepath, desc string) (res int64, err error) {
	contents, err := readFile(filepath)
	if err != nil {
		return 0, errors.Wrapf(err, "error when reading %s from cgroup v1 at %s", desc, filepath)
	}
	res, err = strconv.ParseInt(string(bytes.TrimSpace(contents)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "error when parsing %s from cgroup v1 at %s", desc, filepath)
	}
	return res, nil
}

func detectCPUQuotaInV1(cRoot string) (period, quota int64, err error) {
	quotaFilePath := filepath.Join(cRoot, cgroupV1CPUQuotaFilename)
	periodFilePath := filepath.Join(cRoot, cgroupV1CPUPeriodFilename)
	quota, err = cgroupFileToInt64(quotaFilePath, "cpu quota")
	if err != nil {
		return 0, 0, err
	}
	period, err = cgroupFileToInt64(periodFilePath, "cpu period")
	if err != nil {
		return 0, 0, err
	}

	return period, quota, err
}

func detectCPUUsageInV1(cRoot string) (stime, utime uint64, err error) {
	sysFilePath := filepath.Join(cRoot, cgroupV1CPUSysUsageFilename)
	userFilePath := filepath.Join(cRoot, cgroupV1CPUUserUsageFilename)
	stime, err = cgroupFileToUint64(sysFilePath, "cpu system time")
	if err != nil {
		return 0, 0, err
	}
	utime, err = cgroupFileToUint64(userFilePath, "cpu user time")
	if err != nil {
		return 0, 0, err
	}

	return stime, utime, err
}

func detectCPUQuotaInV2(cRoot string) (period, quota int64, err error) {
	maxFilePath := filepath.Join(cRoot, cgroupV2CPUMaxFilename)
	contents, err := readFile(maxFilePath)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "error when read cpu quota from cgroup v2 at %s", maxFilePath)
	}
	fields := strings.Fields(string(contents))
	if len(fields) > 2 || len(fields) == 0 {
		return 0, 0, errors.Errorf("unexpected format when reading cpu quota from cgroup v2 at %s: %s", maxFilePath, contents)
	}
	if fields[0] == "max" {
		// Negative quota denotes no limit.
		quota = -1
	} else {
		quota, err = strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "error when reading cpu quota from cgroup v2 at %s", maxFilePath)
		}
	}
	if len(fields) == 2 {
		period, err = strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "error when reading cpu period from cgroup v2 at %s", maxFilePath)
		}
	}
	return period, quota, nil
}

func detectCPUUsageInV2(cRoot string) (stime, utime uint64, err error) {
	statFilePath := filepath.Join(cRoot, cgroupV2CPUStatFilename)
	var stat *os.File
	stat, err = os.Open(statFilePath)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "can't read cpu usage from cgroup v2 at %s", statFilePath)
	}
	defer func() {
		err = errors.CombineErrors(err, stat.Close())
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || (string(fields[0]) != "user_usec" && string(fields[0]) != "system_usec") {
			continue
		}
		keyField := string(fields[0])

		trimmed := string(bytes.TrimSpace(fields[1]))
		usageVar := &stime
		if keyField == "user_usec" {
			usageVar = &utime
		}
		*usageVar, err = strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "can't read cpu usage %s from cgroup v1 at %s", keyField, statFilePath)
		}
	}

	return stime, utime, err
}

// detectMemLimitInV1 finds the memory limit value for cgroup V1 via looking in
// [contoller mount path]/memory.stat (cgroupV1MemStatFilename) for the
// cgroupV1MemLimitStatKey.
func detectMemLimitInV1(cRoot string) (limit int64, warnings string, err error) {
	return detectMemStatValue(cRoot, cgroupV1MemStatFilename, cgroupV1MemLimitStatKey, 1)
}

// detectMemLimitInV2 finds the memory limit value for cgroup V2 via looking in
// [controller mount path]/[leaf path]/memory.max (cgroupV2MemLimitFilename)
//
// TODO(vladdy): this implementation was based on podman+criu environment.
// It may cover not all the cases when v2 becomes more widely used in container
// world.
func detectMemLimitInV2(cRoot string) (limit int64, warnings string, err error) {
	return readInt64Value(cRoot, cgroupV2MemLimitFilename, 2)
}

// detectMemUsageInV1 finds the memory usage value for cgroup V1 via looking in
// [contoller mount path]/memory.usage_in_bytes (cgroupV1MemUsageFilename)
func detectMemUsageInV1(cRoot string) (memUsage int64, warnings string, err error) {
	return readInt64Value(cRoot, cgroupV1MemUsageFilename, 1)
}

// detectMemUsageInV2 finds the memory usage value for cgroup V2 via looking in
// [controller mount path]/[leaf path]/memory.max (cgroupV2MemUsageFilename)
//
// TODO(vladdy): this implementation was based on podman+criu environment. It
// may cover not all the cases when v2 becomes more widely used in container
// world.
func detectMemUsageInV2(cRoot string) (memUsage int64, warnings string, err error) {
	return readInt64Value(cRoot, cgroupV2MemUsageFilename, 2)
}

func detectMemInactiveFileUsageInV1(root string) (int64, string, error) {
	return detectMemStatValue(root, cgroupV1MemStatFilename, cgroupV1MemInactiveFileUsageStatKey, 1)
}

func detectMemInactiveFileUsageInV2(root string) (int64, string, error) {
	return detectMemStatValue(root, cgroupV2MemStatFilename, cgroupV2MemInactiveFileUsageStatKey, 2)
}

func detectMemStatValue(
	cRoot, filename, key string, cgVersion int,
) (value int64, warnings string, err error) {
	statFilePath := filepath.Join(cRoot, filename)
	stat, err := os.Open(statFilePath)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't read file %s from cgroup v%d", filename, cgVersion)
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || string(fields[0]) != key {
			continue
		}

		trimmed := string(bytes.TrimSpace(fields[1]))
		value, err = strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, "", errors.Wrapf(err, "can't read %q memory stat from cgroup v%d in %s", key, cgVersion, filename)
		}

		return value, "", nil
	}

	return 0, "", fmt.Errorf("failed to find expected memory stat %q for cgroup v%d in %s", key, cgVersion, filename)
}

func readInt64Value(
	cRoot, filename string, cgVersion int,
) (value int64, warnings string, err error) {
	filePath := filepath.Join(cRoot, filename)
	file, err := os.Open(filePath)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't read %s from cgroup v%d", filename, cgVersion)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	present := scanner.Scan()
	if !present {
		return 0, "", errors.Wrapf(err, "no value found in %s from cgroup v%d", filename, cgVersion)
	}
	data := scanner.Bytes()
	trimmed := string(bytes.TrimSpace(data))
	// cgroupv2 has certain control files that default to "max", so handle here.
	if trimmed == "max" {
		return math.MaxInt64, "", nil
	}
	value, err = strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, "", errors.Wrapf(err, "failed to parse value in %s from cgroup v%d", filename, cgVersion)
	}
	return value, "", nil
}

// The controller is defined via either type `memory` for cgroup v1 or via empty type for cgroup v2,
// where the type is the second field in /proc/[pid]/cgroup file
func detectCntrlPath(cgroupFilePath string, controller string) (string, error) {
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err,
			"failed to read %s cgroup from cgroups file: %s",
			redact.Safe(controller),
			log.SafeManaged(cgroupFilePath))
	}
	defer func() { _ = cgroup.Close() }()

	scanner := bufio.NewScanner(cgroup)
	var unifiedPathIfFound string
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{':'})
		if len(fields) != 3 {
			// The lines should always have three fields, there's something fishy here.
			continue
		}

		f0, f1 := string(fields[0]), string(fields[1])
		// First case if v2, second - v1. We give v2 the priority here.
		// There is also a `hybrid` mode when both versions are enabled, e.g. systemd may use v1 by default.
		// However, in this case, both versions are expected to have the same control path; e.g., cat /proc/2020550/cgroup
		// ...
		// 13:memory:/system.slice/cockroach.service
		// 0::/system.slice/cockroach.service
		/// ...
		if f0 == "0" && f1 == "" {
			unifiedPathIfFound = string(fields[2])
		} else if f1 == controller {
			return string(fields[2]), nil
		}
	}

	return unifiedPathIfFound, nil
}

// versionedMounts contains a cgroup mount and the corresponding cgroup version, either 1 or 2.
type versionedMounts struct {
	mount   string
	version int
}

// Reads /proc/[pid]/mountinfo for cgroup or cgroup2 mount which defines the used version.
// Returns found mountpoints, versions or error.
//
// NOTE: per https://github.com/cockroachdb/cockroach/issues/59236, _both_ versions of cgroups may be enabled.
//
//	Thus, if both are detected, we return version1 and version2 mountpoints so that downstream checks both.
//	If only one version was found, we return the corresponding mountpoint.
//	Otherwise, an error is returned.
//
// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(
	mountinfoPath string, cRoot string, controller string,
) ([]versionedMounts, error) {
	info, err := os.Open(mountinfoPath)
	if err != nil {
		return []versionedMounts{}, errors.Wrapf(err, "failed to read mounts info from file: %s", log.SafeManaged(mountinfoPath))
	}
	defer func() {
		_ = info.Close()
	}()

	var foundVer1, foundVer2 = false, false
	var mountPointVer1, mountPointVer2 string

	scanner := bufio.NewScanner(info)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 10 {
			continue
		}

		ver, ok := detectCgroupVersion(fields, controller)
		if ok {
			mountPoint := string(fields[4])
			if ver == 2 {
				foundVer2 = true
				mountPointVer2 = mountPoint
				continue
			}
			// It is possible that the controller mount and the cgroup path are not the same (both are relative to the NS root).
			// So start with the mount and construct the relative path of the cgroup.
			// To test:
			//   cgcreate -t $USER:$USER -a $USER:$USER -g memory:crdb_test
			//   echo 3999997952 > /sys/fs/cgroup/memory/crdb_test/memory.limit_in_bytes
			//   cgexec -g memory:crdb_test ./cockroach start-single-node
			// cockroach.log -> server/config.go:433 ⋮ system total memory: ‹3.7 GiB›
			//   without constructing the relative path
			// cockroach.log -> server/config.go:433 ⋮ system total memory: ‹63 GiB›
			nsRelativePath := string(fields[3])
			if !strings.Contains(nsRelativePath, "..") {
				// We don't expect to see err here ever but in case that it happens
				// the best action is to ignore the line and hope that the rest of the lines
				// will allow us to extract a valid path.
				if relPath, err := filepath.Rel(nsRelativePath, cRoot); err == nil {
					mountPointVer1 = filepath.Join(mountPoint, relPath)
					foundVer1 = true
				}
			}
		}
	}

	if foundVer1 && foundVer2 {
		return []versionedMounts{{mountPointVer1, 1}, {mountPointVer2, 2}}, nil
	}
	if foundVer1 {
		return []versionedMounts{{mountPointVer1, 1}}, nil
	}
	if foundVer2 {
		return []versionedMounts{{mountPointVer2, 2}}, nil
	}

	return []versionedMounts{}, fmt.Errorf("failed to detect cgroup root mount and version")
}

// Return version of cgroup mount for memory controller if found
func detectCgroupVersion(fields [][]byte, controller string) (_ int, found bool) {
	if len(fields) < 10 {
		return 0, false
	}

	// Due to strange format there can be optional fields in the middle of the set, starting
	// from the field #7. The end of the fields is marked with "-" field
	var pos = 6
	for pos < len(fields) {
		if bytes.Equal(fields[pos], []byte{'-'}) {
			break
		}

		pos++
	}

	// No optional fields separator found or there is less than 3 fields after it which is wrong
	if (len(fields) - pos - 1) < 3 {
		return 0, false
	}

	pos++

	// Check for controller specifically in cgroup v1 (it is listed in super
	// options field), as the value can't be found if it is not enforced.
	if bytes.Equal(fields[pos], []byte("cgroup")) && bytes.Contains(fields[pos+2], []byte(controller)) {
		return 1, true
	} else if bytes.Equal(fields[pos], []byte("cgroup2")) {
		return 2, true
	}

	return 0, false
}

// CPUUsage returns CPU usage and quotas for an entire cgroup.
type CPUUsage struct {
	// System time and user time taken by this cgroup or process. In nanoseconds.
	Stime, Utime uint64
	// CPU period and quota for this process, in microseconds. This cgroup has
	// access to up to (quota/period) proportion of CPU resources on the system.
	// For instance, if there are 4 CPUs, quota = 150000, period = 100000,
	// this cgroup can use around ~1.5 CPUs, or 37.5% of total scheduler time.
	// If quota is -1, it's unlimited.
	Period, Quota int64
	// NumCPUs is the number of CPUs in the system. Always returned even if
	// not called from a cgroup.
	NumCPU int
}

// CPUShares returns the number of CPUs this cgroup can be expected to
// max out. If there's no limit, NumCPU is returned.
func (c CPUUsage) CPUShares() float64 {
	if c.Period <= 0 || c.Quota <= 0 {
		return float64(c.NumCPU)
	}
	return float64(c.Quota) / float64(c.Period)
}

// GetCgroupCPU returns the CPU usage and quota for the current cgroup.
func GetCgroupCPU() (CPUUsage, error) {
	cpuusage, err := getCgroupCPU("/")
	cpuusage.NumCPU = system.NumCPU()
	return cpuusage, err
}

// Helper function for getCgroupCPU. Root is always "/", except in tests.
func getCgroupCPU(root string) (CPUUsage, error) {
	path, err := detectCntrlPath(filepath.Join(root, "/proc/self/cgroup"), "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	// No CPU controller detected
	if path == "" {
		return CPUUsage{}, errors.New("no cpu controller detected")
	}

	versionedMounts, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	var res CPUUsage

	if len(versionedMounts) == 2 {
		// Look up against V2 first. Fall back to V1.
		res.Period, res.Quota, err = detectCPUQuotaInV2(filepath.Join(root, versionedMounts[1].mount, path))
		if err != nil {
			res.Period, res.Quota, err = detectCPUQuotaInV1(filepath.Join(root, versionedMounts[0].mount))
		}
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV2(filepath.Join(root, versionedMounts[1].mount, path))
		if err != nil {
			res.Stime, res.Utime, err = detectCPUUsageInV1(filepath.Join(root, versionedMounts[0].mount))
		}
		if err != nil {
			return res, err
		}
	} else {
		if len(versionedMounts) != 1 {
			return res, errors.AssertionFailedf("expected len(versionedMounts)==1 instead of %d", len(versionedMounts))
		}
		switch versionedMounts[0].version {
		case 1:
			res.Period, res.Quota, err = detectCPUQuotaInV1(filepath.Join(root, versionedMounts[0].mount))
			if err != nil {
				return res, err
			}
			res.Stime, res.Utime, err = detectCPUUsageInV1(filepath.Join(root, versionedMounts[0].mount))
			if err != nil {
				return res, err
			}
		case 2:
			res.Period, res.Quota, err = detectCPUQuotaInV2(filepath.Join(root, versionedMounts[0].mount, path))
			if err != nil {
				return res, err
			}
			res.Stime, res.Utime, err = detectCPUUsageInV2(filepath.Join(root, versionedMounts[0].mount, path))
			if err != nil {
				return res, err
			}
		}
	}

	return res, nil
}

// AdjustMaxProcs sets GOMAXPROCS (if not overridden by env variables) to be
// the CPU limit of the current cgroup, if running inside a cgroup with a cpu
// limit lower than system.NumCPU(). This is preferable to letting it fall back
// to Go default, which is system.NumCPU(), as the Go scheduler would be running
// more OS-level threads than can ever be concurrently scheduled.
func AdjustMaxProcs(ctx context.Context) {
	if _, set := os.LookupEnv("GOMAXPROCS"); !set {
		if cpuInfo, err := GetCgroupCPU(); err == nil {
			numCPUToUse := int(math.Ceil(cpuInfo.CPUShares()))
			if numCPUToUse < system.NumCPU() && numCPUToUse > 0 {
				log.Infof(ctx, "running in a container; setting GOMAXPROCS to %d", numCPUToUse)
				runtime.GOMAXPROCS(numCPUToUse)
			}
		}
	}
}
