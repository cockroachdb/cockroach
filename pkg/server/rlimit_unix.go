// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows && !freebsd && !dragonfly && !darwin

package server

import "golang.org/x/sys/unix"

func setRlimitNoFile(limits *rlimit) error {
	return unix.Setrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits))
}

func getRlimitNoFile(limits *rlimit) error {
	return unix.Getrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits))
}
