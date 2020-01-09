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
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	defaultCGroupRootPath    = "/sys/fs/cgroup/"
	cgroupV1FilesystemSpec   = "cgroup"
	cgroupV2FilesystemSpec   = "cgroup2"
	cgroupMemorySubsystem    = "memory"
	cgroupV1MemLimitFilename = "memory.limit_in_bytes"
	cgroupV2MemLimitFilename = "memory.max"
)

// GetCgroupMemoryLimit attempts to retrieve the memory limit for the current
// process.
func GetCgroupMemoryLimit() (limit int64, warnings string, err error) {
	return getCgroupMem("/")
}

// treeRoot is set to "/" in production code and exists only for testing.
func getCgroupMem(treeRoot string) (limit int64, warnings string, err error) {
	// Let's assume that the memory cgroup is rooted at defaultMemoryCgroupRoot.
	procCgroupFile := filepath.Join(treeRoot, "proc", strconv.Itoa(os.Getpid()), "cgroup")
	cgroupPath, isV2, err := parseMemoryPathFromProcCgroupFile(procCgroupFile)
	if err != nil {
		return 0, "", errors.Wrap(err, "failed to read memory cgroup from cgroups file")
	}
	var cgroupRoot, memoryLimitFile string
	if !isV2 {
		memoryLimitFile = cgroupV1MemLimitFilename
		if cgroupRoot, err = getMemoryCgroupRoot(treeRoot); err != nil {
			return 0, "", err
		}
	} else {
		memoryLimitFile = cgroupV2MemLimitFilename
		if cgroupRoot, err = getUnifiedCgroupRoot(treeRoot); err != nil {
			return 0, "", err
		}
	}
	limit = int64(math.MinInt64)
	var parseErrors []error
	for p := cgroupPath; true; p = filepath.Dir(p) {
		limitFilePath := filepath.Join(treeRoot, cgroupRoot, p, memoryLimitFile)
		if read, err := parseCgroupLimitFile(limitFilePath); err != nil {
			if pe := new(os.PathError); !errors.As(err, &pe) {
				parseErrors = append(parseErrors, err)
			}
		} else if limit == math.MinInt64 || read < limit {
			limit = read
		}
		if p == "/" {
			break
		}
	}
	if limit == math.MinInt64 {
		if len(parseErrors) == 0 {
			return 0, "", fmt.Errorf("failed to find cgroup memory limit")
		}
		return 0, "", parseErrors[0]
	}
	return limit, joinErrorsForWarning(parseErrors), nil
}

// parseMemoryPathFromProcCgroupFile determines the path for the cgroup which
// contains the memory subsystem from the provided path to a cgroup file.
//
// From man 7 cgroups:
//
//  /proc/[pid]/cgroup (since Linux 2.6.24)
//
//  This file describes control groups to which the process with the
//  corresponding PID belongs.  The displayed information differs for cgroups
//  version 1 and version 2 hierarchies.
//
//  For each cgroup hierarchy of which the process is a member, there is one
//  entry containing three colon-separated fields:
//
//    hierarchy-ID:controller-list:cgroup-path
//
//  For example:
//
//    5:cpuacct,cpu,cpuset:/daemons
//
//  The colon-separated fields are, from left to right:
//
//    1. For cgroups version 1 hierarchies, this field contains a unique
//       hierarchy ID number that can be matched to a hierarchy ID in
//       /proc/cgroups. For the cgroups version 2 hierarchy, this field contains
//       the value 0.
//
//    2. For cgroups version 1 hierarchies, this field contains a comma-
//       separated list of the controllers bound to the hierarchy. For the
//       cgroups version 2 hierarchy, this field is empty.
//
//    3. This field contains the pathname of the control group in the hierarchy
//       to which the process belongs.  This pathname is relative to the mount
//       point of the hierarchy.
//
func parseMemoryPathFromProcCgroupFile(
	procCgroupFilePath string,
) (memoryCgroupPath string, isUnified bool, err error) {
	f, err := os.Open(procCgroupFilePath)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f) // scan lines
	var unifiedPath string
	for scanner.Scan() {
		row := scanner.Bytes()
		matches := rowRegexp.FindSubmatchIndex(row)
		if matches == nil {
			// TODO(ajwerner): consider propagating a warning or something.
			continue
		}
		if string(row[matches[2]:matches[3]]) == "0" {
			unifiedPath = string(row[matches[6]:matches[7]])
			continue
		}
		if string(row[matches[4]:matches[5]]) != "memory" {
			continue
		}
		return string(row[matches[6]:matches[7]]), false, nil
	}
	if unifiedPath != "" {
		return unifiedPath, true, nil
	}
	return "", false, errors.New("failed to find memory cgroup, must not be in one")
}

// see parseMemoryPathFromProcCgroupFile.
var rowRegexp = regexp.MustCompile(`(\d+):(.*):(.*)`)

func joinErrorsForWarning(errs []error) string {
	var buf strings.Builder
	for _, err := range errs {
		if buf.Len() == 0 {
			buf.WriteString("failed to read some cgroup files: ")
		} else {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

// parseCgroupLimitFile reads and parses a decimal integer from the provided
// file path.
func parseCgroupLimitFile(limitFilePath string) (limit int64, err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(limitFilePath); err != nil {
		return 0, errors.Wrapf(err, "can't read available memory from cgroup at %s", limitFilePath)
	}
	trimmed := string(bytes.TrimSpace(buf))
	if trimmed == "max" {
		return math.MaxInt64, nil
	}
	limit, err = strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "can't read available memory from cgroup at %s", limitFilePath)
	}
	return limit, nil
}
