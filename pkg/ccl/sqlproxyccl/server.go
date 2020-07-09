// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Serve serves a listener according to the given Options. Incoming client
// connections are taken through the Postgres handshake and relayed to the
// configured backend server.
func Serve(ln net.Listener, opts Options) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			tBegin := timeutil.Now()
			log.Infof(context.Background(), "handling client %s", conn.RemoteAddr())
			err := Proxy(conn, opts)
			log.Infof(context.Background(), "client %s disconnected after %.2fs: %v",
				conn.RemoteAddr(), timeutil.Since(tBegin).Seconds(), err)
		}()
	}
}
