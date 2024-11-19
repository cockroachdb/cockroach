// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var (
	zeroBytes            = logconfig.ByteSize(0)
	zeroDuration         = time.Duration(0)
	disabledBufferingCfg = logconfig.CommonBufferSinkConfigWrapper{
		CommonBufferSinkConfig: logconfig.CommonBufferSinkConfig{
			MaxStaleness:     &zeroDuration,
			FlushTriggerSize: &zeroBytes,
			MaxBufferSize:    &zeroBytes,
		},
	}
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
	recall time.Duration,
) {
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	logHangWg := sync.WaitGroup{}
	logHangWg.Add(1)

	// seenMessage is true after the request predicate
	// has seen the expected message from the client.
	var seenMessage atomic.Bool

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
			logHangWg.Wait()
		} else {
			// The test is expecting some message via a predicate.
			if err := fn(r.Header, string(buf)); err != nil {
				// non-failing, in case there are extra log messages generated
				t.Log(err)
			} else {
				seenMessage.Store(true)
			}
		}
	}

	// Start the HTTP server that receives the logging events from the
	// test.
	s2 := httptest.NewServer(http.HandlerFunc(handler))
	defer s2.Close()
	defaults.Address = &s2.URL

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
	cleanup, err := ApplyConfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
	require.NoError(t, err)
	defer cleanup()

	// Send a log event on the OPS channel.
	logStart := timeutil.Now()
	Ops.Infof(context.Background(), "hello world")
	logDuration := timeutil.Since(logStart)

	if deadline > 0 {
		// Note: deadline is passed by the caller and already contains slack
		// to accommodate for the overhead of the logging call compared to
		// the timeout in the HTTP request.
		require.LessOrEqualf(t, logDuration, deadline,
			"Log call exceeded timeout, expected to be less than %s, got %s", deadline.String(), logDuration.String())
		// If we don't properly hang in the handler when we want to test a
		// timeout, we'll just log very quickly. This check ensures that we
		// catch that testing error.
		require.Greaterf(t, logDuration, *defaults.Timeout,
			"Log call was too fast, expected to be greater than %s, got %s", defaults.Timeout.String(), logDuration.String())
	}

	if hangServer {
		logHangWg.Done()
		return
	}

	// Issue a second log event if recall is specified so that the test can be
	// run again.
	if recall > 0 {
		time.Sleep(recall)
		Ops.Infof(context.Background(), "hello world")
	}

	// If the test was not requiring a timeout, it was requiring some
	// logging message to match the predicate. If we don't see the
	// predicate match, it is a test failure.
	if !seenMessage.Load() {
		t.Error("expected message matching predicate, found none")
	}
}

// TestMessageReceived verifies that the server receives the logged message.
func TestMessageReceived(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeout := 5 * time.Second
	tb := true
	defaults := logconfig.HTTPDefaults{
		Timeout:     &timeout,
		Compression: &logconfig.NoneCompression,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Buffering: disabledBufferingCfg,
		},
	}

	testFn := func(_ http.Header, body string) error {
		t.Log(body)
		if !strings.Contains(body, `"message":"hello world"`) {
			return errors.New("Log message not found in request")
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0), time.Duration(0))
}

// TestHTTPSinkTimeout verifies that a log call to a hanging server doesn't last
// to much longer than the configured timeout.
func TestHTTPSinkTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeout := time.Millisecond * 100
	tb := true
	defaults := logconfig.HTTPDefaults{
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Buffering: disabledBufferingCfg,
		},
	}

	testBase(t, defaults, nil /* testFn */, true /* hangServer */, 10*time.Second, time.Duration(0))
}

// TestHTTPSinkContentTypeJSON verifies that the HTTP sink content type
// header is set to `application/json` when the format is json.
func TestHTTPSinkContentTypeJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeout := 5 * time.Second
	tb := true
	format := "json-fluent"
	expectedContentType := "application/json"
	defaults := logconfig.HTTPDefaults{
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Format:    &format,
			Buffering: disabledBufferingCfg,
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

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0), time.Duration(0))
}

// TestHTTPSinkContentTypePlainText verifies that the HTTP sink content type
// header is set to `text/plain` when the format is json.
func TestHTTPSinkContentTypePlainText(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeout := 5 * time.Second
	tb := true
	format := "crdb-v1"
	expectedContentType := "text/plain"
	defaults := logconfig.HTTPDefaults{
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Format:    &format,
			Buffering: disabledBufferingCfg,
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

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0), time.Duration(0))
}

func TestHTTPSinkHeadersAndCompression(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeout := 5 * time.Second
	tb := true
	format := "json"
	expectedContentType := "application/json"
	expectedContentEncoding := logconfig.GzipCompression
	val := "secret-value"
	filepathVal := "another-secret-value"
	filepathReplaceVal := "third-secret-value"
	// Test filepath method of providing header values.
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "filepath_test.txt")
	require.NoError(t, os.WriteFile(filename, []byte(filepathVal), 0777))
	defaults := logconfig.HTTPDefaults{
		Timeout: &timeout,

		// We need to disable keepalives otherwise the HTTP server in the
		// test will let an async goroutine run waiting for more requests.
		DisableKeepAlives: &tb,
		CommonSinkConfig: logconfig.CommonSinkConfig{
			Format:    &format,
			Buffering: disabledBufferingCfg,
		},

		Compression: &logconfig.GzipCompression,
		// Provide both the old format and new format in order to test backwards compatability.
		Headers:          map[string]string{"X-CRDB-TEST": val},
		FileBasedHeaders: map[string]string{"X-CRDB-TEST-2": filename},
	}

	var callCt int
	testFn := func(header http.Header, body string) error {
		t.Log(body)
		contentType := header.Get("Content-Type")
		if contentType != expectedContentType {
			return errors.Newf("mismatched content type: expected %s, got %s", expectedContentType, contentType)
		}
		contentEncoding := header.Get("Content-Encoding")
		if contentEncoding != expectedContentEncoding {
			return errors.Newf("mismatched content encoding: expected %s, got %s", expectedContentEncoding, contentEncoding)
		}

		var isGzipped = func(dat []byte) bool {
			gzipPrefix := []byte("\x1F\x8B\x08")
			return bytes.HasPrefix(dat, gzipPrefix)
		}

		if !isGzipped([]byte(body)) {
			return errors.New("expected gzipped body")
		}
		var matchCount int
		filepathExpectedVal := filepathVal
		if callCt > 0 {
			filepathExpectedVal = filepathReplaceVal
		}
		for k, v := range header {
			if k == "X-Crdb-Test" || k == "X-Crdb-Test-2" {
				for _, vv := range v {
					if vv == "secret-value" || vv == filepathExpectedVal {
						matchCount++
					}
				}
			}
		}
		if matchCount != 2 {
			return errors.New("expected to find special header in request")
		}
		// If this is the first time the testFn has been called, update file contents and send SIGHUP.
		if callCt == 0 {
			callCt++
			if err := os.WriteFile(filename, []byte(filepathReplaceVal), 0777); err != nil {
				return err
			}
			t.Log("issuing SIGHUP")
			if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}

	testBase(t, defaults, testFn, false /* hangServer */, time.Duration(0), 1*time.Second)
}
