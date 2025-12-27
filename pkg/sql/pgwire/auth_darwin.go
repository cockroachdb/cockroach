// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build darwin

package pgwire

import (
	"fmt"
	"net"
	"os/user"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func getOSUserFromUnixConn(conn net.Conn) (string, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return "", errors.New("peer authentication is only available on unix socket connections")
	}

	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		return "", errors.Wrap(err, "could not get syscall conn")
	}

	var cred *unix.Xucred // Use Xucred for macOS/darwin
	var credErr error
	err = rawConn.Control(func(fd uintptr) {
		// Use GetsockoptXucred and LOCAL_PEERCRED for macOS
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})
	if err != nil {
		return "", errors.Wrap(err, "syscall control failed")
	}
	if credErr != nil {
		return "", errors.Wrap(credErr, "getsockopt(LOCAL_PEERCRED) failed")
	}

	// The UID is in the Xucred struct
	u, err := user.LookupId(fmt.Sprintf("%d", cred.Uid))
	if err != nil {
		return "", errors.Wrapf(err, "could not find user for uid %d", cred.Uid)
	}
	return u.Username, nil
}
