// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func benchmarkSocketRead(withDeadline bool, b *testing.B) {
	server, err := net.Listen("tcp", "")
	require.NoErrorf(b, err, "unable to listen: %v", err)

	// Server is sending and reading bytes in an infinite loop
	go func() {
		cServ, err := server.Accept()
		require.NoErrorf(b, err, "unable to accept connection: %v", err)
		bServ := []byte{1}
		for {
			_, _ = cServ.Write(bServ)
			_, _ = cServ.Read(bServ)
		}
	}()

	cCli, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		b.Fatalf("Unable to create server socket: %v", err)
	}

	bCli := []byte{1}

	// Timeout in 30 sec
	deadline := timeutil.Now().Add(3e10)
	for i := 0; i < b.N; i++ {
		now := timeutil.Now()
		if withDeadline {
			// Set a new timeout if it was more than 0.1s since last call
			if now.Sub(deadline) > 1e8 {
				deadline = now.Add(3e10)
				_ = cCli.SetReadDeadline(deadline)
			}
		}
		_, _ = cCli.Read(bCli)
		_, _ = cCli.Write(bCli)
	}
}

// No statistically significant difference in a single roundtrip time between
// using and not using deadline as implemented above. Both show the same value in my tests.
// SocketReadWithDeadline-32     11.1µs ± 1%
// SocketReadWithoutDeadline-32  11.0µs ± 3%
func BenchmarkSocketReadWithoutDeadline(b *testing.B) {
	benchmarkSocketRead(false, b)
}
func BenchmarkSocketReadWithDeadline(b *testing.B) {
	benchmarkSocketRead(true, b)
}
