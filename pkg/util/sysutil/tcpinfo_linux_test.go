// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package sysutil

import (
	"context"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

// GetRTTInfo doesn't panic when it's called with a nil connection.
func TestGetTCPInfoNilConn(t *testing.T) {
	var conn *net.TCPConn
	info, ok := GetRTTInfo(conn)
	require.Nil(t, info)
	require.False(t, ok)
}

func TestGetTCPInfoLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		skip.IgnoreLint(t, "skipping test; RTT inspection is only supported on Linux")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a TCP echo server.
	listener, _, err := createEchoServer(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Connect a client.
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Give the connection a moment to be established.
	time.Sleep(10 * time.Millisecond)

	pingPayload := []byte("ping")
	pongBuffer := make([]byte, len(pingPayload))

	// Write a "ping".
	if _, err := conn.Write(pingPayload); err != nil {
		t.Fatal(err)
	}
	// Read the "pong" echo.
	if _, err := io.ReadFull(conn, pongBuffer); err != nil {
		t.Fatal(err)
	}

	// RTT info is only available on TCP connections. GetRTTInfo takes a
	// *net.TCPConn, forcing the caller to perform a type assertion.
	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)

	info, ok := GetRTTInfo(tcpConn)

	require.True(t, ok)
	require.NotNil(t, info)
}

// createEchoServer creates a TCP echo server that echoes back all received data.
// It returns the listener and a channel that will receive any server errors.
func createEchoServer(ctx context.Context, addr string) (net.Listener, <-chan error, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	serverErrChan := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			// If the context is canceled, the listener may be closed,
			// resulting in an error. This is expected during cleanup.
			select {
			case <-ctx.Done():
				serverErrChan <- nil
			default:
				serverErrChan <- err
			}
			return
		}
		defer conn.Close()
		// Echo all data received back to the client. This will run until
		// the client closes the connection, resulting in an io.EOF.
		if _, err := io.Copy(conn, conn); err != nil && err != io.EOF {
			serverErrChan <- err
		} else {
			serverErrChan <- nil
		}
	}()

	return listener, serverErrChan, nil
}
