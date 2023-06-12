// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc"
)

func init() {
	// Disable GRPC tracing. This retains a subset of messages for
	// display on /debug/requests, which is very expensive for
	// snapshots. Until we can be more selective about what is retained
	// in traces, we must disable tracing entirely.
	// https://github.com/grpc/grpc-go/issues/695
	grpc.EnableTracing = false
}

var enableRPCCircuitBreakers = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"rpc.circuit_breaker.enabled",
	"enables stateful management of failed connections, including circuit breaking "+
		"when in unhealthy state; only use in case of issues - logging may be suboptimal, "+
		"metrics related to connection errors will not be populated correctly, and the "+
		"'rpc.dialback.enabled' setting will be overridden to 'false'",
	envutil.EnvOrDefaultBool("COCKROACH_RPC_CIRCUIT_BREAKERS_ENABLED", true),
)

// TODO(baptist): Remove in 23.2 (or 24.1) once validating dialback works for all scenarios.
var useDialback = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"rpc.dialback.enabled",
	"if true, require bidirectional RPC connections between nodes to prevent one-way network unavailability",
	true,
)

var enableRPCCompression = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_RPC_COMPRESSION", true)

func getWindowSize(name string, c ConnectionClass, defaultSize int) int32 {
	const maxWindowSize = defaultWindowSize * 32
	s := envutil.EnvOrDefaultInt(name, defaultSize)
	if s > maxWindowSize {
		log.Warningf(context.Background(), "%s value too large; trimmed to %d", name, maxWindowSize)
		s = maxWindowSize
	}
	if s <= defaultWindowSize {
		log.Warningf(context.Background(),
			"%s RPC will use dynamic window sizes due to %s value lower than %d", c, name, defaultSize)
	}
	return int32(s)
}

const (
	defaultWindowSize = 65535
	// The coefficient by which the tolerated offset is multiplied to determine
	// the maximum acceptable measurement latency.
	maximumPingDurationMult = 2
)

var (
	// for an RPC
	initialWindowSize = getWindowSize(
		"COCKROACH_RPC_INITIAL_WINDOW_SIZE", DefaultClass, defaultWindowSize*32)
	initialConnWindowSize = initialWindowSize * 16 // for a connection

	// for RangeFeed RPC
	rangefeedInitialWindowSize = getWindowSize(
		"COCKROACH_RANGEFEED_RPC_INITIAL_WINDOW_SIZE", RangefeedClass, 2*defaultWindowSize /* 128K */)
)

// sourceAddr is the environment-provided local address for outgoing
// connections.
var sourceAddr = func() net.Addr {
	const envKey = "COCKROACH_SOURCE_IP_ADDRESS"
	if sourceAddr, ok := envutil.EnvString(envKey, 0); ok {
		sourceIP := net.ParseIP(sourceAddr)
		if sourceIP == nil {
			panic(fmt.Sprintf("unable to parse %s '%s' as IP address", envKey, sourceAddr))
		}
		return &net.TCPAddr{
			IP: sourceIP,
		}
	}
	return nil
}()

type serverOpts struct {
	interceptor func(fullMethod string) error
}

// ServerOption is a configuration option passed to NewServer.
type ServerOption func(*serverOpts)

// WithInterceptor adds an additional interceptor. The interceptor is called before
// streaming and unary RPCs and may inject an error.
func WithInterceptor(f func(fullMethod string) error) ServerOption {
	return func(opts *serverOpts) {
		if opts.interceptor == nil {
			opts.interceptor = f
		} else {
			f := opts.interceptor
			opts.interceptor = func(fullMethod string) error {
				if err := f(fullMethod); err != nil {
					return err
				}
				return f(fullMethod)
			}
		}
	}
}
