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
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// testBase sets the provided HTTPDefaults, logs "hello World", captures the
// resulting request to the server, and validates the body with the provided
// requestTestFunc.
// Options also given to cause the server to hang (which naturally skips the body valiation)
// and to set a maximum duration for the log call.
func testBase(
	t *testing.T,
	defaults logconfig.HTTPDefaults,
	fn func(header http.Header, body string) error,
	hangServer bool,
	deadline time.Duration,
) {
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// cancelCh ensures that async goroutines terminate if the test
	// goroutine terminates due to a Fatal call or a panic.
	cancelCh := make(chan struct{})
	defer func() { close(cancelCh) }()

	// seenMessage is true after the request predicate
	// has seen the expected message from the client.
	var seenMessage syncutil.AtomicBool

	handler := func(rw http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 5000)
		nbytes, err := r.Body.Read(buf)
		if err != nil && err != io.EOF {
			t.Error(err)
			return
		}
		buf = buf[:nbytes]

		if hangServer {
			// The test is requesting the server to simulate a timeout. Just
			// do nothing until the test terminates.
			<-cancelCh
		} else {
			// The test is expecting some message via a predicate.
			if err := fn(r.Header, string(buf)); err != nil {
				// non-failing, in case there are extra log messages generated
				t.Log(err)
			} else {
				seenMessage.Set(true)
			}
		}
	}

	{
		// Start the HTTP server that receives the logging events from the
		// test.

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

		// serverErrCh collects errors and signals the termination of the
		// server async goroutine.
		serverErrCh := make(chan error, 1)
		go func() {
			defer func() { close(serverErrCh) }()
			err := s.Serve(l)
			if !errors.Is(err, http.ErrServerClosed) {
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
			require.NoError(t, s.Close())
			serverErr := <-serverErrCh
			require.NoError(t, serverErr)
		}()
	}

	// Set up a logging configuration with the server we've just set up
	// as target for the OPS channel.
	cfg := logconfig.DefaultConfig()
	cfg.Sinks.HTTPServers = map[string]*logconfig.HTTPSinkConfig{
		"ops": {
			HTTPDefaults: defaults,
			Channels:     logconfig.SelectChannels(channel.OPS)},
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

	// Note: deadline is passed by the caller and already contains slack
	// to accommodate for the overhead of the logging call compared to
	// the timeout in the HTTP request.
	if deadline > 0 && logDuration > deadline {
		t.Error("Log call exceeded timeout")
	}

	if hangServer {
		return
	}

	// If the test was not requiring a timeout, it was requiring some
	// logging message to match the predicate. If we don't see the
	// predicate match, it is a test failure.
	if !seenMessage.Get() {
		t.Error("expected message matching predicate, found none")
	}
}

// TestMessageReceived verifies that the server receives the logged message.
func TestMessageReceived(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := 5 * time.Second
	tb := true
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
	}

	testFn := func(_ http.Header, body string) error {
		t.Log(body)
		if !strings.Contains(body, `"message":"hello world"`) {
			return errors.New("Log message not found in request")
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0))
}

// TestHTTPSinkTimeout verifies that a log call to a hanging server doesn't last
// to much longer than the configured timeout.
func TestHTTPSinkTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := time.Millisecond
	tb := true
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
	}

	testBase(t, defaults, nil /* testFn */, true /* hangServer */, 500*time.Millisecond)
}

// TestHTTPSinkContentTypeJSON verifies that the HTTP sink content type
// header is set to `application/json` when the format is json.
func TestHTTPSinkContentTypeJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := 5 * time.Second
	tb := true
	format := "json-fluent"
	expectedContentType := "application/json"
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Format: &format,
		},
	}

	testFn := func(header http.Header, body string) error {
		t.Log(body)
		contentType := header.Get("Content-Type")
		if contentType != expectedContentType {
			return errors.Newf("mismatched content type: expected %s, got %s", expectedContentType, contentType)
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0))
}

// TestHTTPSinkContentTypePlainText verifies that the HTTP sink content type
// header is set to `text/plain` when the format is json.
func TestHTTPSinkContentTypePlainText(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost" // testBase appends the port
	timeout := 5 * time.Second
	tb := true
	format := "crdb-v1"
	expectedContentType := "text/plain"
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Format: &format,
		},
	}

	testFn := func(header http.Header, body string) error {
		t.Log(body)
		contentType := header.Get("Content-Type")
		if contentType != expectedContentType {
			return errors.Newf("mismatched content type: expected %s, got %s", expectedContentType, contentType)
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0))
}
