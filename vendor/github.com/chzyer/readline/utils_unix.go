// +build darwin dragonfly freebsd linux,!appengine netbsd openbsd

package readline

import (
	"syscall"
	"unsafe"
)

type winsize struct {
	Row    uint16
	Col    uint16
	Xpixel uint16
	Ypixel uint16
}

// get width of the terminal
func getWidth(stdoutFd int) int {
	ws := &winsize{}
	retCode, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(stdoutFd),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		_ = errno
		return -1
	}
	return int(ws.Col)
}
