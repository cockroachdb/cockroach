// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type serveModeHandler struct {
	mode serveMode
}

type serveMode int32

func (s *serveModeHandler) setMode(mode serveMode) {
	s.mode.set(mode)
}

func (s *serveModeHandler) operational() bool {
	sMode := s.mode.get()
	return sMode == modeOperational || sMode == modeDraining
}

// A list of the server states for bootstrap process.
const (
	// modeInitializing is intended for server initialization process.
	// It allows only bootstrap, heartbeat and gossip methods
	// to prevent calls to potentially uninitialized services.
	modeInitializing serveMode = iota
	// modeOperational is intended for completely initialized server
	// and thus allows all RPC methods.
	modeOperational
	// modeDraining is intended for an operational server in the process of
	// shutting down. The difference is that readiness checks will fail.
	modeDraining
)

var rpcsAllowedWhileBootstrapping = map[string]struct{}{
	"/cockroach.rpc.Heartbeat/Ping":             {},
	"/cockroach.gossip.Gossip/Gossip":           {},
	"/cockroach.server.serverpb.Init/Bootstrap": {},
	"/cockroach.server.serverpb.Admin/Health":   {},
}

// intercept implements filtering rules for each server state.
func (s *serveModeHandler) intercept(fullName string) error {
	if s.operational() {
		return nil
	}
	if _, allowed := rpcsAllowedWhileBootstrapping[fullName]; !allowed {
		return NewWaitingForInitError(fullName)
	}
	return nil
}

func (s *serveMode) set(mode serveMode) {
	atomic.StoreInt32((*int32)(s), int32(mode))
}

func (s *serveMode) get() serveMode {
	return serveMode(atomic.LoadInt32((*int32)(s)))
}

const (
	serverPrefix = "/cockroach.server"
	tsdbPrefix   = "/cockroach.ts"
)

// serverRPCRequestMetricsEnabled is a cluster setting that enables the
// collection of gRPC and DRPC request duration metrics. This uses export only
// metrics so the metrics are only exported to external sources such as
// /_status/vars and DataDog.
var serverRPCRequestMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.rpc.request_metrics.enabled",
	"enables the collection of rpc metrics",
	false, /* defaultValue */
	settings.WithRetiredName("server.grpc.request_metrics.enabled"),
)

func shouldRecordRequestDuration(settings *cluster.Settings, method string) bool {
	return serverRPCRequestMetricsEnabled.Get(&settings.SV) &&
		(strings.HasPrefix(method, serverPrefix) ||
			strings.HasPrefix(method, tsdbPrefix))
}

// NewWaitingForInitError creates an error indicating that the server cannot run
// the specified method until the node has been initialized.
func NewWaitingForInitError(methodName string) error {
	// NB: this error string is sadly matched in grpcutil.IsWaitingForInit().
	return grpcstatus.Errorf(codes.Unavailable, "node waiting for init; %s not available", methodName)
}
