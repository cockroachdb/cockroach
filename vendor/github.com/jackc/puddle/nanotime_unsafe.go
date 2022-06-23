// +build !purego,!appengine,!js

// This file contains the implementation of nanotime using runtime.nanotime.

package puddle

import "unsafe"

var _ = unsafe.Sizeof(0)

//go:linkname nanotime runtime.nanotime
func nanotime() int64
