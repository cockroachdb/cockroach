// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func setupServerWithIdleDisconnect(t testing.TB, timeout time.Duration) net.Addr {
	server, err := net.Listen("tcp", "")
	require.NoError(t, err)

	// Server is echoing back bytes in an infinite loop.
	go func() {
		cServ, err := server.Accept()
		if err != nil {
			t.Errorf("Error during accept: %v", err)
		}
		defer cServ.Close()
		cServ = IdleDisconnectOverlay(cServ, timeout)
		_, _ = io.Copy(cServ, cServ)
	}()
	return server.Addr()
}

func ping(conn net.Conn) error {
	n, err := conn.Write([]byte{1})
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.Newf("Expected 1 written byte but got %d", n)
	}
	n, err = conn.Read([]byte{1})
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.Newf("Expected 1 read byte but got %d", n)
	}
	return nil
}

func benchmarkSocketRead(timeout time.Duration, b *testing.B) {
	addr := setupServerWithIdleDisconnect(b, timeout)

	cCli, err := net.Dial("tcp", addr.String())
	if err != nil {
		b.Errorf("Error during dial: %v", err)
	}
	defer cCli.Close()

	bCli := []byte{1}
	for i := 0; i < b.N; i++ {
		_, err = cCli.Write(bCli)
		if err != nil {
			b.Errorf("Error during write: %v", err)
		}
		_, err = cCli.Read(bCli)
		if err != nil {
			b.Errorf("Error during read: %v", err)
		}
	}
	_, err = cCli.Write([]byte{}) // This serves as EOF and shuts down the echo server
	if err != nil {
		b.Errorf("Error during read: %v", err)
	}
}

// No statistically significant difference in a single roundtrip time between
// using and not using deadline as implemented above. Both show the same value in my tests.
// SocketReadWithDeadline-32     11.1µs ± 1%
// SocketReadWithoutDeadline-32  11.0µs ± 3%
func BenchmarkSocketReadWithoutDeadline(b *testing.B) {
	benchmarkSocketRead(0, b)
}
func BenchmarkSocketReadWithDeadline(b *testing.B) {
	benchmarkSocketRead(1e8, b)
}

func TestIdleDisconnectOverlay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		timeout          time.Duration
		willCloseAtCheck int
	}{
		// The disconnect checks are done at 0.2s, 0.7s and 1.5s marks.
		{0, 0},
		{0.1e9, 1},
		{0.3e9, 2},
		{0.6e9, 3},
		{1e9, 0},
	}

	for _, tt := range tests {
		name := fmt.Sprintf(
			"timeout(%s)-willCloseAt(%d)", tt.timeout, tt.willCloseAtCheck,
		)
		t.Run(name, func(t *testing.T) {
			addr := setupServerWithIdleDisconnect(t, tt.timeout)
			conn, err := net.Dial("tcp", addr.String())
			require.NoError(t, err, "Unable to dial server")
			time.Sleep(.2e9)
			if tt.willCloseAtCheck == 1 {
				require.Error(t, ping(conn))
				return
			}
			require.NoError(t, ping(conn))
			time.Sleep(.5e9)
			if tt.willCloseAtCheck == 2 {
				require.Error(t, ping(conn))
				return
			}
			require.NoError(t, ping(conn))
			time.Sleep(.8e9)
			if tt.willCloseAtCheck == 3 {
				require.Error(t, ping(conn))
				return
			}
			require.NoError(t, ping(conn))
		})
	}
}
