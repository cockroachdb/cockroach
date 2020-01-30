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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/errors"
)

const (
	cgroupV1MemLimitFilename = "memory.stat"
	cgroupV2MemLimitFilename = "memory.max"
)

const (
	unknown = iota
	v1
	v2
)

// GetCgroupMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process
func GetMemoryLimit() (limit int64, warnings string, err error) {
	return getCgroupMem2("/")
}

// `root` is set to "/" in production code and exists only for testing.
// cgroup memory limit detection path implemented here as
// /proc/self/cgroup file -> /proc/self/mountinfo mounts -> cgroup version -> version specific limit check
func getCgroupMem2(root string) (limit int64, warnings string, err error) {
	path, err := detectMemCntrlPath(filepath.Join(root, "/proc/self/cgroup"))
	if err != nil {
		return 0, "", err
	}

	// no memory controller detected
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path)
	if err != nil {
		return 0, "", err
	}

	switch ver {
	case unknown:
		limit, warnings, err = 0, "no known cgroup version detected", nil
	case v1:
		limit, warnings, err = detectLimitInV1(filepath.Join(root, mount))
	case v2:
		limit, warnings, err = detectLimitInV2(filepath.Join(root, mount, path))
	default:
		limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
	}

	return
}

// Finds memory limit for cgroup V1 via looking in [contoller mount path]/memory.stat
func detectLimitInV1(cRoot string) (limit int64, warnings string, err error) {
	statFilePath := filepath.Join(cRoot, cgroupV1MemLimitFilename)
	stat, err := os.Open(statFilePath)
	if err != nil {
		return
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) == 2 {
			if string(fields[0]) == "hierarchical_memory_limit" {
				trimmed := string(bytes.TrimSpace(fields[1]))
				limit, err = strconv.ParseInt(trimmed, 10, 64)
				if err != nil {
					return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v1 at %s", statFilePath)
				}

				return
			}
		}
	}

	return 0, "", fmt.Errorf("failed to find expected memory limit for cgroup v1 in %s", statFilePath)
}

// Finds memory limit for cgroup V2 via looking into [controller mount path]/[leaf path]/memory.max
func detectLimitInV2(cRoot string) (limit int64, warnings string, err error) {
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
func detectMemCntrlPath(cgroupFilePath string) (string, error) {
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read memory cgroup from cgroups file: %s", cgroupFilePath)
	}
	defer func() { _ = cgroup.Close() }()

	scanner := bufio.NewScanner(cgroup)
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{':'})
		if len(fields) == 3 {
			f1, f2 := string(fields[0]), string(fields[1])
			if f2 == "memory" || (f1 == "0" && f2 == "") {
				return string(fields[2]), nil
			}
		}
	}

	return "", nil
}

// Reads /proc/[pid]/mountinfo for cgoup or cgroup2 mount which defines the used version.
// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountinfoPath string, cRoot string) (string, int, error) {
	info, err := os.Open(mountinfoPath)
	if err != nil {
		return "", unknown, errors.Wrapf(err, "failed to read mounts info from file: %s", mountinfoPath)
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

		fsType := getCgroupFsType(fields)
		if (fsType == v1 && string(fields[3]) == cRoot) || fsType == v2 {
			return string(fields[4]), fsType, nil
		}
	}

	return "", unknown, nil
}

// Return version of cgroup mount for memory controller or `unknown`
func getCgroupFsType(fields [][]byte) int {
	if len(fields) < 10 {
		return unknown
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
		return unknown
	}

	pos++

	// Check for memory controller specifically in cgroup v1 (it is listed in super options field),
	// as the limit can't be found if it is not enforced
	if bytes.Equal(fields[pos], []byte("cgroup")) && bytes.Contains(fields[pos+2], []byte("memory")) {
		return v1
	} else if bytes.Equal(fields[pos], []byte("cgroup2")) {
		return v2
	}

	return unknown
}
