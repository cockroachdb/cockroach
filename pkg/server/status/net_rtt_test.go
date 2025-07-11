package status

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/stretchr/testify/require"
)

func TestRTTLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping test; RTT inspection is only supported on Linux")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start a TCP echo server.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

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

	// 2. Connect a client and start a ping-pong data transfer loop.
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	var pingPongCounter atomic.Int64
	go func() {
		pingPayload := []byte("ping")
		pongBuffer := make([]byte, len(pingPayload))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Write a "ping".
				if _, err := conn.Write(pingPayload); err != nil {
					return
				}
				// Read the "pong" echo.
				if _, err := io.ReadFull(conn, pongBuffer); err != nil {
					return
				}
				pingPongCounter.Add(1)
			}
		}
	}()

	// Give the connection a moment to be established and for activity to start.
	time.Sleep(100 * time.Millisecond)

	// 3. Find the connection using gopsutil.
	pid := int32(os.Getpid())
	proc, err := process.NewProcess(pid)
	require.NoError(t, err)

	// 4. Loop and print RTT info.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	iterations := 25
	fmt.Println("Reading RTT for 25 seconds while actively sending data...")

	for i := 0; i < iterations; i++ {
		select {
		case <-ticker.C:
			conns, err := proc.Connections()
			require.NoError(t, err)

			var foundConn bool
			for _, c := range conns {
				// Find the specific connection we just made.
				laddr := conn.LocalAddr().(*net.TCPAddr)
				raddr := conn.RemoteAddr().(*net.TCPAddr)

				if c.Laddr.Port == uint32(laddr.Port) && c.Raddr.Port == uint32(raddr.Port) {
					foundConn = true
					rttInfo, err := getRTTInfo(c) // This will call the linux-specific implementation.
					require.NoError(t, err)
					require.NotNil(t, rttInfo)

					pings := pingPongCounter.Load()
					fmt.Printf("Iteration %d: RTT=%s, RTTVar=%s, Ping-Pongs=%d\n", i+1, rttInfo.RTT, rttInfo.RTTVar, pings)
					// On a local connection, RTT should be very small but non-zero.
					require.Greater(t, rttInfo.RTT, time.Duration(0))
					break
				}
			}
			require.True(t, foundConn, "did not find established connection")

		case <-ctx.Done():
			t.Fatal("test context cancelled")
		}
	}
}
