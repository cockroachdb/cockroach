// Copyright 2021 The Cockroach Authors.
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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type requestTestFunc func(body string) error

// testBase sets the provided HTTPDefaults, logs "hello World", captures the
// resulting request to the server, and validates the body with the provided
// requestTestFunc.
// Options also given to cause the server to hang (which naturally skips the body valiation)
// and to set a maximum duration for the log call.
func testBase(
	t *testing.T,
	defaults logconfig.HTTPDefaults,
	fn requestTestFunc,
	hangServer bool,
	deadline time.Duration,
) {
	var wg sync.WaitGroup
	defer wg.Wait()

	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	bodyCh := make(chan string, 1)
	cancelCh := make(chan struct{})

	handler := func(rw http.ResponseWriter, r *http.Request) {
		wg.Add(1)
		buf := make([]byte, 5000)
		if _, err := r.Body.Read(buf); err != nil && err != io.EOF {
			t.Error(err)
		}
		if hangServer {
			<-cancelCh
		} else {
			select {
			case bodyCh <- string(buf):
			case <-cancelCh:
			}
		}
		wg.Done()
	}

	serverErrCh := make(chan error)
	serverClosedCh := make(chan struct{})
	defer func() { <-serverClosedCh }()
	{
		l, err := net.Listen("tcp", "127.0.0.1:")
		if err != nil {
			t.Fatal(err)
		}
		_, port, err := addr.SplitHostPort(l.Addr().String(), "port")
		if err != nil {
			t.Fatal(err)
		}
		*defaults.Address += ":" + port
		s := http.Server{Handler: http.HandlerFunc(handler)}
		wg.Add(1)
		go func() {
			defer close(serverClosedCh)
			defer wg.Done()
			err := s.Serve(l)
			if err != http.ErrServerClosed {
				select {
				case serverErrCh <- err:
				case <-cancelCh:
				}
			}
		}()
		// At the end of this function, close the server
		// allowing the above goroutine to finish and close serverClosedCh
		// allowing the deferred read to proceed and this function to return.
		// (Basically, it's a WaitGroup of one.)
		defer func() {
			close(cancelCh)
			time.Sleep(time.Second)
			require.NoError(t, s.Close())
		}()
	}

	// Set up a logging configuration with the server we've just set up
	// as target for the OPS channel.
	cfg := logconfig.DefaultConfig()
	cfg.Sinks.HTTPServers = map[string]*logconfig.HTTPSinkConfig{
		"ops": {
			HTTPDefaults: defaults,
			Channels:     logconfig.ChannelList{Channels: []Channel{channel.OPS}}},
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
	logStart := timeutil.Now()
	Ops.Infof(context.Background(), "hello world")
	logDuration := timeutil.Since(logStart)
	if deadline > 0 && logDuration > deadline {
		t.Error("Log call exceeded timeout")
	}

	if hangServer {
		return
	}

	// Check if any requests received within the timeout satisfy the given predicate.
	timer := time.After(10 * time.Second)
outer:
	for {
		select {
		case <-timer:
			t.Fatal("timeout")
		case body := <-bodyCh:
			if err := fn(body); err != nil {
				// non-failing, in case there are extra log messages generated
				t.Log(err)
			} else {
				break outer
			}
		case err := <-serverErrCh:
			t.Fatal(err)
		}
	}
}

// TestMessageReceived verifies that the server receives the logged message.
func TestMessageReceived(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := 250 * time.Millisecond
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,
	}

	testFn := func(body string) error {
		t.Log(body)
		if !strings.Contains(body, `"message":"hello world"`) {
			return errors.New("Log message not found in request")
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0))
}

// TestTimeout verifies that a log call to a hanging server doesn't last
// to much longer than the configured timeout.
func TestTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := time.Millisecond
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,
	}

	testBase(t, defaults, nil /* testFn */, true /* hangServer */, 500*time.Millisecond)
}
