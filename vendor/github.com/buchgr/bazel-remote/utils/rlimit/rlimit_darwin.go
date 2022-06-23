// +build darwin

package rlimit

import (
	"log"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Raise the limit on the number of open files.
func Raise() {
	// Go 1.12 onwards uses getrlimit, which on macos does not return the
	// correct hard limit for RLIM_NOFILE. We need to use the minimum of
	// the max from getrlimit and the value from sysctl.
	// Background: https://github.com/golang/go/issues/30401

	var limits syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits)
	if err != nil {
		log.Println("Failed to find rlimit from getrlimit:", err)
		return
	}

	// Hack to avoid using cgo with bazel's rules_go :P
	cmd := exec.Command("/usr/sbin/sysctl", "-n", "kern.maxfilesperproc")
	stdout, err := cmd.Output()
	if err != nil {
		log.Println("Failed to find rlimit from sysctl:", err)
		return
	}

	val := strings.Trim(string(stdout), "\n")
	sysctlMax, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		log.Println("Failed to parse rlimit from sysctl:", err)
		return
	}

	if limits.Max > sysctlMax {
		limits.Max = sysctlMax
	}

	log.Printf("Initial RLIMIT_NOFILE cur: %d max: %d",
		limits.Cur, limits.Max)

	limits.Cur = limits.Max

	log.Printf("Setting RLIMIT_NOFILE cur: %d max: %d",
		limits.Cur, limits.Max)

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limits)
	if err != nil {
		log.Println("Failed to set rlimit:", err)
		return
	}

	return
}
