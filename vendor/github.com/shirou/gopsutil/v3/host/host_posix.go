//go:build linux || freebsd || openbsd || darwin || solaris
// +build linux freebsd openbsd darwin solaris

package host

import (
	"bytes"

	"golang.org/x/sys/unix"
)

func KernelArch() (string, error) {
	var utsname unix.Utsname
	err := unix.Uname(&utsname)
	return string(utsname.Machine[:bytes.IndexByte(utsname.Machine[:], 0)]), err
}
