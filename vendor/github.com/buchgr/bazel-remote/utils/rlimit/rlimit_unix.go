// +build !darwin
// +build !windows

package rlimit

import (
	"log"
	"syscall"
)

// Raise the limit on the number of open files.
func Raise() {
	var limits syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits)
	if err != nil {
		log.Println("Failed to find rlimit from getrlimit:", err)
		return
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
