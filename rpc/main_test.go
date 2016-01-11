// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package rpc

import (
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	defer func(retryOpts retry.Options) {
		clientRetryOptions = retryOpts
	}(clientRetryOptions)

	clientRetryOptions = retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     1 * time.Millisecond,
	}

	leaktest.TestMainWithLeakCheck(m)
}

// newNodeTestContext returns a rpc.Context for testing.
// It is meant to be used by nodes.
func newNodeTestContext(clock *hlc.Clock, stopper *stop.Stopper) *Context {
	if clock == nil {
		clock = hlc.NewClock(hlc.UnixNano)
	}
	ctx := NewContext(testutils.NewNodeTestBaseContext(), clock, stopper)
	ctx.heartbeatInterval = 10 * time.Millisecond
	return ctx
}

func newTestServer(t *testing.T, ctx *Context, manual bool) (*Server, net.Listener) {
	var s *Server
	if manual {
		s = &Server{
			insecure:    ctx.Insecure,
			activeConns: make(map[net.Conn]struct{}),
			methods:     map[string]method{},
		}
	} else {
		s = NewServer(ctx)
	}

	tlsConfig, err := ctx.GetServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}

	addr := util.CreateTestAddr("tcp")
	ln, err := util.ListenAndServe(ctx.Stopper, s, addr, tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

	return s, ln
}
