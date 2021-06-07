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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/stretchr/testify/require"
)

type requestTestFunc func(*http.Request) error

func testBase(t *testing.T, defaults logconfig.HTTPDefaults, fn requestTestFunc) {
	defer leaktest.AfterTest(t)()
	sc := ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ch := make(chan *http.Request)
	defer close(ch)
	handler := func(rw http.ResponseWriter, r *http.Request) {
		ch <- r
	}

	s := http.Server{
		Addr:    ":80",
		Handler: http.HandlerFunc(handler),
	}
	go s.ListenAndServe()
	defer s.Close()

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

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case request := <-ch:
		if err := fn(request); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSpecific(t *testing.T) {
	address := string("http://localhost:80")
	//timeout := 250 * time.Millisecond
	defaults := logconfig.HTTPDefaults{
		Address: &address,
		//Timeout: &timeout,
	}

	test_fn := func(r *http.Request) error {
		buf := make([]byte, 5000)
		r.Body.Read(buf)
		body := string(buf)
		if !strings.Contains(body, `"message":"hello world"`) {
			return errors.New("Log message not found in request")
		}
		return nil
	}

	testBase(t, defaults, test_fn)
}
