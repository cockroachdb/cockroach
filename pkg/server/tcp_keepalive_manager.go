// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/server/tcpkeepalive"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

type tcpKeepAliveManager struct {
	// loggedKeepAliveStatus ensures that errors about setting the TCP
	// keepalive status are only reported once.
	loggedKeepAliveStatus int32
	settings              *cluster.Settings
}

func makeTCPKeepAliveManager(settings *cluster.Settings) tcpKeepAliveManager {
	return tcpKeepAliveManager{
		settings: settings,
	}
}

// configure attempts to set TCP keep-alive on
// connection. Does not fail on errors.
func (k *tcpKeepAliveManager) configure(ctx context.Context, conn net.Conn) {
	muxConn, ok := conn.(*cmux.MuxConn)
	if !ok {
		return
	}
	tcpConn, ok := muxConn.Conn.(*net.TCPConn)
	if !ok {
		return
	}

	// Only log success/failure once.
	doLog := atomic.CompareAndSwapInt32(&k.loggedKeepAliveStatus, 0, 1)
	handleErr := func(settingName string, err error) (returnFromFunc bool) {
		if !doLog || err == nil {
			// Return if any error is detected.
			return err != nil
		}
		log.Ops.Warningf(ctx, "failed to configure %s for pgwire: %v", settingName, err)
		return true
	}

	probeCount := tcpkeepalive.ProbeCount.Get(&k.settings.SV)
	probeFrequency := tcpkeepalive.ProbeInterval.Get(&k.settings.SV)
	idleTime := tcpkeepalive.IdleTime.Get(&k.settings.SV)
	if idleTime == 0 {
		idleTime = probeFrequency
	}
	err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     idleTime,
		Interval: probeFrequency,
		Count:    int(probeCount),
	})
	if handleErr("TCP keep-alive", err) {
		return
	}

	userTimeout := tcpkeepalive.UserTimeout.Get(&k.settings.SV)
	rawConn, err := tcpConn.SyscallConn()
	if handleErr("TCP user timeout", err) {
		return
	}
	var userTimeoutErr error
	err = rawConn.Control(func(fd uintptr) {
		userTimeoutErr = sysutil.SetTcpUserTimeout(sysutil.SocketFd(fd), userTimeout)
	})
	err = errors.CombineErrors(err, userTimeoutErr)
	if handleErr("TCP user timeout", err) {
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive interval %d and probe count to %d for pgwire", probeFrequency, probeCount)
		log.VEventf(ctx, 2, "setting TCP user timeout %s for pgwire", userTimeout)
	}
}
