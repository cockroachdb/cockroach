// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build gofuzz

package pgwire

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func FuzzServeConn(data []byte) int {
	s := MakeServer(
		log.AmbientContext{},
		&base.Config{},
		&cluster.Settings{},
		sql.MemoryMetrics{},
		&mon.BytesMonitor{},
		time.Minute,
		&sql.ExecutorConfig{
			Settings: &cluster.Settings{},
		},
	)

	// Fake a connection using a pipe.
	srv, client := net.Pipe()
	go func() {
		// Write the fuzz data to the connection and close.
		_, _ = client.Write(data)
		_ = client.Close()
	}()
	go func() {
		// Discard all data sent from the server.
		_, _ = io.Copy(ioutil.Discard, client)
	}()
	err := s.ServeConn(context.Background(), srv)
	if err != nil {
		return 0
	}
	return 1
}
