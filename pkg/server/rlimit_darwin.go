// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows,!freebsd,!dragonfly

package server

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// #include <sys/types.h>
// #include <sys/sysctl.h>
import "C"

func setRlimitNoFile(limits *rlimit) error {
	return unix.Setrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits))
}

func getRlimitNoFile(limits *rlimit) error {
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits)); err != nil {
		return err
	}
	// On macOS, the true hard open file limit is
	// min(sysctl("kern.maxfiles"),
	//     sysctl("kern.maxfilesperproc"),
	//     getrlimit(RLIMIT_NOFILE))
	// This does not appear to be documented and may be incomplete.
	//
	// See https://github.com/golang/go/issues/30401 for more context.
	sysctlMaxFiles, err := getSysctlMaxFiles()
	if err != nil {
		return err
	}
	if limits.Max > sysctlMaxFiles {
		limits.Max = sysctlMaxFiles
	}
	sysctlMaxFilesPerProc, err := getSysctlMaxFilesPerProc()
	if err != nil {
		return err
	}
	if limits.Max > sysctlMaxFilesPerProc {
		limits.Max = sysctlMaxFilesPerProc
	}
	return nil
}

func getSysctlMaxFiles() (uint64, error) {
	return getSysctl(C.CTL_KERN, C.KERN_MAXFILES) // identifies the "kern.maxfiles" sysctl
}

func getSysctlMaxFilesPerProc() (uint64, error) {
	return getSysctl(C.CTL_KERN, C.KERN_MAXFILESPERPROC) // identifies the "kern.maxfilesperproc" sysctl
}

func getSysctl(x, y C.int) (uint64, error) {
	var out int32
	outLen := C.size_t(unsafe.Sizeof(out))
	sysctlMib := [...]C.int{x, y}
	r, errno := C.sysctl(&sysctlMib[0], C.u_int(len(sysctlMib)), unsafe.Pointer(&out), &outLen,
		nil /* newp */, 0 /* newlen */)
	if r != 0 {
		return 0, errno
	}
	return uint64(out), nil
}
