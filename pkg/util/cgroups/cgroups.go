// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cgroups

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
)

const (
	cgroupV1MemLimitFilename     = "memory.stat"
	cgroupV2MemLimitFilename     = "memory.max"
	cgroupV1CPUQuotaFilename     = "cpu.cfs_quota_us"
	cgroupV1CPUPeriodFilename    = "cpu.cfs_period_us"
	cgroupV1CPUSysUsageFilename  = "cpuacct.usage_sys"
	cgroupV1CPUUserUsageFilename = "cpuacct.usage_user"
	cgroupV2CPUMaxFilename       = "cpu.max"
	cgroupV2CPUStatFilename      = "cpu.stat"
)

// GetMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process
func GetMemoryLimit() (limit int64, warnings string, err error) {
	return getCgroupMem("/")
}

// `root` is set to "/" in production code and exists only for testing.
// cgroup memory limit detection path implemented here as
// /proc/self/cgroup file -> /proc/self/mountinfo mounts -> cgroup version -> version specific limit check
func getCgroupMem(root string) (limit int64, warnings string, err error) {
	path, err := detectCntrlPath(filepath.Join(root, "/proc/self/cgroup"), "memory")
	if err != nil {
		return 0, "", err
	}

	// no memory controller detected
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "memory")
	if err != nil {
		return 0, "", err
	}

	switch ver {
	case 1:
		limit, warnings, err = detectMemLimitInV1(filepath.Join(root, mount))
	case 2:
		limit, warnings, err = detectMemLimitInV2(filepath.Join(root, mount, path))
	default:
		limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
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
	res, err = ioutil.ReadAll(f)
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

// Finds memory limit for cgroup V1 via looking in [contoller mount path]/memory.stat
func detectMemLimitInV1(cRoot string) (limit int64, warnings string, err error) {
	statFilePath := filepath.Join(cRoot, cgroupV1MemLimitFilename)
	stat, err := os.Open(statFilePath)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v1 at %s", statFilePath)
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || string(fields[0]) != "hierarchical_memory_limit" {
			continue
		}

		trimmed := string(bytes.TrimSpace(fields[1]))
		limit, err = strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v1 at %s", statFilePath)
		}

		return limit, "", nil
	}

	return 0, "", fmt.Errorf("failed to find expected memory limit for cgroup v1 in %s", statFilePath)
}

// Finds memory limit for cgroup V2 via looking into [controller mount path]/[leaf path]/memory.max
// TODO(vladdy): this implementation was based on podman+criu environment. It may cover not
// all the cases when v2 becomes more widely used in container world.
func detectMemLimitInV2(cRoot string) (limit int64, warnings string, err error) {
	limitFilePath := filepath.Join(cRoot, cgroupV2MemLimitFilename)

	var buf []byte
	if buf, err = ioutil.ReadFile(limitFilePath); err != nil {
		return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v2 at %s", limitFilePath)
	}

	trimmed := string(bytes.TrimSpace(buf))
	if trimmed == "max" {
		return math.MaxInt64, "", nil
	}

	limit, err = strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't parse available memory from cgroup v2 in %s", limitFilePath)
	}
	return limit, "", nil
}

// The controller is defined via either type `memory` for cgroup v1 or via empty type for cgroup v2,
// where the type is the second field in /proc/[pid]/cgroup file
func detectCntrlPath(cgroupFilePath string, controller string) (string, error) {
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read %s cgroup from cgroups file: %s", controller, cgroupFilePath)
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
		// There is also a `hybrid` mode when both  versions are enabled,
		// but no known container solutions support it afaik
		if f0 == "0" && f1 == "" {
			unifiedPathIfFound = string(fields[2])
		} else if f1 == controller {
			return string(fields[2]), nil
		}
	}

	return unifiedPathIfFound, nil
}

// Reads /proc/[pid]/mountinfo for cgoup or cgroup2 mount which defines the used version.
// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountinfoPath string, cRoot string, controller string) (string, int, error) {
	info, err := os.Open(mountinfoPath)
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to read mounts info from file: %s", mountinfoPath)
	}
	defer func() {
		_ = info.Close()
	}()

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
				return mountPoint, ver, nil
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
					return filepath.Join(mountPoint, relPath), ver, nil
				}
			}
		}
	}

	return "", 0, fmt.Errorf("failed to detect cgroup root mount and version")
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

	// Check for controller specifically in cgroup v1 (it is listed in super options field),
	// as the limit can't be found if it is not enforced
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

	mount, ver, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path, "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	var res CPUUsage

	switch ver {
	case 1:
		res.Period, res.Quota, err = detectCPUQuotaInV1(filepath.Join(root, mount))
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV1(filepath.Join(root, mount))
		if err != nil {
			return res, err
		}
	case 2:
		res.Period, res.Quota, err = detectCPUQuotaInV2(filepath.Join(root, mount, path))
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV2(filepath.Join(root, mount, path))
		if err != nil {
			return res, err
		}
	default:
		return CPUUsage{}, fmt.Errorf("detected unknown cgroup version index: %d", ver)
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
