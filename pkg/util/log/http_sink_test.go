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
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
)

// TODO: HTTP server needs bound to context -- still need to figure out the right context.
// http.NewRequestWithContext

type requestTestFunc func(body string) error

func testBase(t *testing.T, defaults logconfig.HTTPDefaults, fn requestTestFunc) {
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	bodyCh := make(chan string, 1)
	defer close(bodyCh)
	handler := func(rw http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 5000)
		r.Body.Read(buf)
		bodyCh <- string(buf)
	}

	serverErrCh := make(chan error)
	{
		l, err := net.Listen("tcp", "127.0.0.1:")
		if err != nil {
			t.Fatal(err)
		}
		port := strings.Split(l.Addr().String(), ":")[1]
		*defaults.Address += ":" + port
		s := http.Server{Handler: http.HandlerFunc(handler)}
		go func() {
			err := s.Serve(l)
			if err != http.ErrServerClosed {
				serverErrCh <- err
			}
		}()
		defer s.Close()
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
	Ops.Infof(context.Background(), "hello world")

	// Check if any requests received within the timeout satisfy the given predicate.
	timer := time.After(10 * time.Second)
	outer: for {
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

func TestSpecific(t *testing.T) {
	defer leaktest.AfterTest(t)()

	address := "http://localhost"  // testBase appends the port
	timeout := 250 * time.Millisecond
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		Timeout: &timeout,
	}

	test_fn := func(body string) error {
		t.Log(body)
		if !strings.Contains(body, `"message":"hello world"`) {
			return errors.New("Log message not found in request")
		}
		return nil
	}

	testBase(t, defaults, test_fn)
}

// Test cases:
// No timeout -- ensure correct behavior.
// Small timeout -- ensure timeout works.