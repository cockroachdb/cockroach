// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

package pgwire

import (
	"context"
	"io"
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
		_, _ = io.Copy(io.Discard, client)
	}()
	err := s.ServeConn(context.Background(), srv)
	if err != nil {
		return 0
	}
	return 1
}
