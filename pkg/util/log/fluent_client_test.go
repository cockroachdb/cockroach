// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestFluentClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	serverAddr, cleanup, fluentData := servePseudoFluent(t)
	defer cleanup()

	t.Logf("addr: %v", serverAddr)

	// Set up a logging configuration with the server we've just set up
	// as target for the OPS channel.
	cfg := logconfig.DefaultConfig()
	cfg.Sinks.FluentServers = map[string]*logconfig.FluentSinkConfig{
		"ops": {
			Address:  serverAddr,
			Channels: logconfig.ChannelList{Channels: []Channel{channel.OPS}}},
	}
	// Derive a full config using the same directory as the
	// TestLogScope.
	require.NoError(t, cfg.Validate(&sc.logDir))

	// Apply the configuration.
	TestingResetActive()
	cleanup, err := ApplyConfig(cfg)
	require.NoError(t, err)
	defer cleanup()

	// Send a log event on the OPS channel.
	Ops.Infof(context.Background(), "hello world")

	// Check that the event was indeed sent via the Fluent sink.
	var ev []byte
	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case ev = <-fluentData:
	}

	var info map[string]interface{}
	if err := json.Unmarshal(ev, &info); err != nil {
		t.Fatalf("unable to decode json: %q: %v", ev, err)
	}
	// Erase non-deterministic fields.
	info["t"] = "XXX"
	info["g"] = 222
	msg, err := json.Marshal(info)
	require.NoError(t, err)

	const expected = `{"c":1,"f":"util/log/fluent_client_test.go","g":222,"l":58,"message":"hello world","n":1,"r":1,"s":1,"sev":"I","t":"XXX","tag":"log_test.ops"}`
	require.Equal(t, expected, string(msg))
}

// servePseudoFluent creates an in-memory TCP listener which accepts
// newline-terminated strings of data and reports them over the
// returned channel.
func servePseudoFluent(t *testing.T) (serverAddr string, cleanup func(), fluentData chan []byte) {
	l, err := net.ListenTCP("tcp", nil)
	require.NoError(t, err)

	fluentData = make(chan []byte, 1)

	serverCtx, serverCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// When canceled at the end of the test, we need to exit the loop.
			select {
			case <-serverCtx.Done():
				return
			default:
			}

			// Take one client connection.
			conn, err := l.Accept()
			if err != nil {
				t.Logf("accept error: %v", err)
				break
			}
			t.Logf("got client: %v", conn.RemoteAddr())

			// Serve the connection.
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if err := conn.Close(); err != nil {
						t.Logf("close error: %v", err)
					}
				}()

				buf := bufio.NewReader(conn)
				for {
					// When the test finishes, the other side has a connection
					// open. Use the context cancellation and a read timeout
					// (below) to detect this.
					select {
					case <-serverCtx.Done():
						return
					default:
					}

					t.Logf("client %v: waiting for data", conn.RemoteAddr())
					if err := conn.SetReadDeadline(timeutil.Now().Add(100 * time.Millisecond)); err != nil {
						t.Logf("set read deadline: %v", err)
						return
					}

					// Read one line of data and report it over the channel.
					str, err := buf.ReadBytes('\n')
					if err != nil {
						t.Logf("read error: %v", err)
					}
					if len(str) > 0 {
						t.Logf("received: %q", string(str))
						fluentData <- str
					}
				}
			}()
		}
	}()
	cleanup = func() {
		// Cancel the context. This prevents further progress in the
		// acceptor loop before.
		serverCancel()
		// Close the listen socket. This breaks any currently running call
		// to Accept().
		require.NoError(t, l.Close())
		// Wait until all servers have stopped running.
		wg.Wait()
	}
	return l.Addr().String(), cleanup, fluentData
}
