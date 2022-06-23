// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// http://golang.org/src/os/stat_js.go

package atime

import (
	"os"
	"syscall"
	"time"
)

func timespecToTime(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

func atime(fi os.FileInfo) time.Time {
	stat := fi.Sys().(*syscall.Stat_t)
	return timespecToTime(stat.Atime, stat.AtimeNsec)
}
