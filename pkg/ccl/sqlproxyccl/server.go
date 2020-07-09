// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"log"
	"net"
	"time"
)

func Serve(ln net.Listener, opts Options) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			tBegin := time.Now()
			log.Println("handling client", conn.RemoteAddr())
			err := Proxy(conn, opts)
			log.Printf("client %s disconnected after %.2fs: %v",
				conn.RemoteAddr(), time.Since(tBegin).Seconds(), err)
		}()
	}
	return nil
}
