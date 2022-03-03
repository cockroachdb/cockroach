// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file provides generic interfaces that allow tests to set up test tenants
// without importing the server package (avoiding circular dependencies). This

package serverutils

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestTenantInterface defines SQL-only tenant functionality that tests need; it
// is implemented by server.Test{Tenant,Server}. Tests written against this
// interface are effectively agnostic to the type of tenant (host or secondary)
// they're dealing with.
type TestTenantInterface interface {
	// SQLInstanceID is the ephemeral ID assigned to a running instance of the
	// SQLServer. Each tenant can have zero or more running SQLServer instances.
	SQLInstanceID() base.SQLInstanceID

	// SQLAddr returns the tenant's SQL address.
	SQLAddr() string

	// HTTPAddr returns the tenant's http address.
	HTTPAddr() string

	// RPCAddr returns the tenant's RPC address.
	RPCAddr() string

	// PGServer returns the tenant's *pgwire.Server as an interface{}.
	PGServer() interface{}

	// DiagnosticsReporter returns the tenant's *diagnostics.Reporter as an
	// interface{}. The DiagnosticsReporter periodically phones home to report
	// diagnostics and usage.
	DiagnosticsReporter() interface{}

	// StatusServer returns the tenant's *server.SQLStatusServer as an
	// interface{}.
	StatusServer() interface{}

	// TenantStatusServer returns the tenant's *server.TenantStatusServer as an
	// interface{}.
	TenantStatusServer() interface{}

	// DistSQLServer returns the *distsql.ServerImpl as an interface{}.
	DistSQLServer() interface{}

	// JobRegistry returns the *jobs.Registry as an interface{}.
	JobRegistry() interface{}

	// RPCContext returns the *rpc.Context used by the test tenant.
	RPCContext() *rpc.Context

	// AnnotateCtx annotates a context.
	AnnotateCtx(context.Context) context.Context

	// ExecutorConfig returns a copy of the tenant's ExecutorConfig.
	// The real return type is sql.ExecutorConfig.
	ExecutorConfig() interface{}

	// RangeFeedFactory returns the range feed factory used by the tenant.
	// The real return type is *rangefeed.Factory.
	RangeFeedFactory() interface{}

	// ClusterSettings returns the ClusterSettings shared by all components of
	// this tenant.
	ClusterSettings() *cluster.Settings

	// Stopper returns the stopper used by the tenant.
	Stopper() *stop.Stopper

	// Clock returns the clock used by the tenant.
	Clock() *hlc.Clock

	// SpanConfigKVAccessor returns the underlying spanconfig.KVAccessor as an
	// interface{}.
	SpanConfigKVAccessor() interface{}

	// SpanConfigReconciler returns the underlying spanconfig.Reconciler as an
	// interface{}.
	SpanConfigReconciler() interface{}

	// SpanConfigSQLTranslator returns the underlying spanconfig.SQLTranslator as
	// an interface{}.
	SpanConfigSQLTranslator() interface{}

	// SpanConfigSQLWatcher returns the underlying spanconfig.SQLWatcher as an
	// interface{}.
	SpanConfigSQLWatcher() interface{}

	// TestingKnobs returns the TestingKnobs in use by the test server.
	TestingKnobs() *base.TestingKnobs

	// AmbientCtx retrieves the AmbientContext for this server,
	// so that a test can instantiate additional one-off components
	// using the same context details as the server. This should not
	// be used in non-test code.
	AmbientCtx() log.AmbientContext

	// AdminURL returns the URL for the admin UI.
	AdminURL() string
	// GetHTTPClient returns an http client configured with the client TLS
	// config required by the TestServer's configuration.
	GetHTTPClient() (http.Client, error)
	// GetAdminAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	// The user has admin privileges.
	GetAdminAuthenticatedHTTPClient() (http.Client, error)
	// GetAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error)

	// DrainClients shuts down client connections.
	DrainClients(ctx context.Context) error

	// SystemConfigProvider provides access to the system config.
	SystemConfigProvider() config.SystemConfigProvider

	// TODO(irfansharif): We'd benefit from an API to construct a *gosql.DB, or
	// better yet, a *sqlutils.SQLRunner. We use it all the time, constructing
	// it by hand each time.
}
