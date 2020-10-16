// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var logAuthRequests = settings.RegisterPublicBoolSetting(
	"server.auth_log.web_requests.enabled",
	"if set, log HTTP requests by authenticated users (note: may hinder performance on loaded nodes)",
	false)

var logWebSessionAuth = settings.RegisterPublicBoolSetting(
	"server.auth_log.web_sessions.enabled",
	"if set, log HTTP session login/disconnection events (note: may hinder performance on loaded nodes)",
	false)

// authLogHandler intercepts requests to the RPC and debug endpoints
// via HTTP and logs requests if enabled via the cluster setting.
type authLogHandler struct {
	authLogger *log.SecondaryLogger
	st         *cluster.Settings
	inner      http.Handler
}

func newAuthConnLoggingHandler(
	authLogger *log.SecondaryLogger, st *cluster.Settings, httpService http.Handler,
) http.Handler {
	return &authLogHandler{
		authLogger: authLogger,
		st:         st,
		inner:      httpService,
	}
}

// ServeHTTP implements the http.Handler interface.
func (a *authLogHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if logAuthRequests.Get(&a.st.SV) {
		ctx := req.Context()
		// Note: the URL is not safe for logging, as it can contain
		// application data as part of the query path.
		a.authLogger.Logf(ctx, "HTTP %s: %s", log.Safe(req.Method), req.URL)
	}
	a.inner.ServeHTTP(w, req)
}

// StartAuthConnLogger starts the secondary logger for connection and authn events.
func StartAuthConnLogger(ctx context.Context, stopper *stop.Stopper) *log.SecondaryLogger {
	// Define the auth logger which logs connection and authentication
	// when enabled via cluster settings.
	// Note: the auth logger uses sync writes because we don't want an
	// attacker to easily "erase their traces" after an attack by
	// crashing the server before it has a chance to write the last
	// few log lines to disk.
	//
	// TODO(knz): We could worry about disk I/O activity incurred by
	// logging here in case a malicious user spams the server with
	// (failing) connection attempts to cause a DoS failure; this
	// would be a good reason to invest into a syslog sink for logs.
	loggerCtx, _ := stopper.WithCancelOnStop(ctx)
	authLogger := log.NewSecondaryLogger(
		loggerCtx, nil /* dirName */, "auth",
		true /* enableGc */, true /* forceSyncWrites */, true, /* enableMsgCount */
	)
	stopper.AddCloser(authLogger)
	return authLogger
}
