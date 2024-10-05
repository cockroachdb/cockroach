// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type tcpKeepAliveManager struct {
	// The keepalive duration.
	tcpKeepAlive time.Duration
	// loggedKeepAliveStatus ensures that errors about setting the TCP
	// keepalive status are only reported once.
	loggedKeepAliveStatus int32
}

func makeTCPKeepAliveManager() tcpKeepAliveManager {
	return tcpKeepAliveManager{
		tcpKeepAlive: envutil.EnvOrDefaultDuration("COCKROACH_SQL_TCP_KEEP_ALIVE", time.Minute),
	}
}

// configure attempts to set TCP keep-alive on
// connection. Does not fail on errors.
func (k *tcpKeepAliveManager) configure(ctx context.Context, conn net.Conn) {
	if k.tcpKeepAlive == 0 {
		return
	}

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
	if err := tcpConn.SetKeepAlive(true); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
		}
		return

	}
	if err := tcpConn.SetKeepAlivePeriod(k.tcpKeepAlive); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
		}
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive to %s for pgwire", k.tcpKeepAlive)
	}
}
