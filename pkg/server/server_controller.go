// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

const (
	// AcceptHeader is the canonical header name for accept.
	AcceptHeader = "Accept"
	// ContentTypeHeader is the canonical header name for content type.
	ContentTypeHeader = "Content-Type"
	// JSONContentType is the JSON content type.
	JSONContentType = "application/json"
)

// onDemandServer represents a server that can be started on demand.
type onDemandServer interface {
	stop(context.Context)

	// getHTTPHandlerFn retrieves the function that can serve HTTP
	// requests for this server.
	getHTTPHandlerFn() http.HandlerFunc

	// testingGetSQLAddr retrieves the address of the SQL listener.
	// Used until the following issue is resolved:
	// https://github.com/cockroachdb/cockroach/issues/84585
	testingGetSQLAddr() string
}

type serverEntry struct {
	// server is the actual server.
	server onDemandServer

	// shouldStop indicates whether shutting down the controller
	// should cause this server to stop. This is true for all
	// servers except the one serving the system tenant.
	//
	// TODO(knz): Investigate if the server for te system tenant can be
	// also included, so that this special case can be removed.
	shouldStop bool
}

// newServerFn is the type of a constructor for on-demand server.
//
// The value provided for index is guaranteed to be different for each
// simultaneously running server. This can be used to allocate
// distinct but predictable network listeners.
//
// If the specified tenant name is invalid (tenant does not exist or
// is not active), the returned error will contain the
// ErrInvalidTenant mark, which can be checked with errors.Is.
type newServerFn func(
	ctx context.Context,
	tenantName roachpb.TenantName,
	index int,
	deregister func(),
) (onDemandServer, error)

// serverController manages a fleet of multiple servers side-by-side.
// They are instantiated on demand the first time they are accessed.
// Instantiation can fail, e.g. if the target tenant doesn't exist or
// is not active.
type serverController struct {
	// newServerFn instantiates a new server.
	newServerFn newServerFn

	// stopper is the parent stopper.
	stopper *stop.Stopper

	mu struct {
		syncutil.Mutex

		// servers maps tenant names to the server for that tenant.
		//
		// TODO(knz): Detect when the mapping of name to tenant ID has
		// changed, and invalidate the entry.
		servers map[roachpb.TenantName]serverEntry

		// nextServerIdx is the index to provide to the next call to
		// newServerFn.
		nextServerIdx int
	}
}

func newServerController(
	ctx context.Context,
	parentStopper *stop.Stopper,
	newServerFn newServerFn,
	systemServer onDemandServer,
) *serverController {
	c := &serverController{
		stopper:     parentStopper,
		newServerFn: newServerFn,
	}
	c.mu.servers = map[roachpb.TenantName]serverEntry{
		catconstants.SystemTenantName: {
			server:     systemServer,
			shouldStop: false,
		},
	}
	parentStopper.AddCloser(c)
	return c
}

// getOrCreateServer retrieves a reference to the current server for
// the given tenant name, and instantiates the server if none exists
// yet.
func (c *serverController) getOrCreateServer(
	ctx context.Context, tenantName roachpb.TenantName,
) (onDemandServer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.mu.servers[tenantName]; ok {
		return s.server, nil
	}

	// deregisterFn will remove the server from
	// the map, when it shuts down.
	deregisterFn := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.mu.servers, tenantName)
	}

	// Server does not exist yet: instantiate and start it.
	c.mu.nextServerIdx++
	idx := c.mu.nextServerIdx
	s, err := c.newServerFn(ctx, tenantName, idx, deregisterFn)
	if err != nil {
		return nil, err
	}
	c.mu.servers[tenantName] = serverEntry{
		server:     s,
		shouldStop: true,
	}
	return s, nil
}

// Close implements the stop.Closer interface.
func (c *serverController) Close() {
	// Stop all stoppable servers.

	// We first collect the list of servers under lock and then stop
	// them without lock, so that their deregister function can operate
	// on the map.
	servers := c.getServers()
	for _, s := range servers {
		s.stop(context.Background())
	}
}

func (c *serverController) getServers() (res []onDemandServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.mu.servers {
		if e.shouldStop {
			res = append(res, e.server)
		}
	}
	return res
}

// TenantSelectHeader is the HTTP header used to select a particular tenant.
const TenantSelectHeader = `X-Cockroach-Tenant`

// TenantNameParamInQueryURL is the HTTP query URL parameter used to select a particular tenant.
const TenantNameParamInQueryURL = "tenant_name"

// TenantSelectCookieName is the name of the HTTP cookie used to select a particular tenant,
// if the custom header is not specified.
const TenantSelectCookieName = `tenant`

// httpMux redirects incoming HTTP requests to the server selected by
// the special HTTP request header.
// If no tenant is specified, the default tenant is used.
func (c *serverController) httpMux(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantName, nameProvided := getTenantNameFromHTTPRequest(r)
	// if the client didnt specify tenant name call these for login/logout.
	if !nameProvided {
		switch r.URL.Path {
		case loginPath:
			c.attemptLoginToAllTenants().ServeHTTP(w, r)
			return
		case logoutPath:
			c.attemptLogoutFromAllTenants().ServeHTTP(w, r)
			return
		}
	}
	s, err := c.getOrCreateServer(ctx, tenantName)
	if err != nil {
		log.Warningf(ctx, "unable to start server for tenant %q: %v", tenantName, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	s.getHTTPHandlerFn()(w, r)
}

func getTenantNameFromHTTPRequest(r *http.Request) (roachpb.TenantName, bool) {
	// Highest priority is manual override on the URL query parameters.
	if tenantName := r.URL.Query().Get(TenantNameParamInQueryURL); tenantName != "" {
		return roachpb.TenantName(tenantName), true
	}

	// If not in parameters, try an explicit header.
	if tenantName := r.Header.Get(TenantSelectHeader); tenantName != "" {
		return roachpb.TenantName(tenantName), true
	}

	// No parameter, no explicit header. Is there a cookie?
	if c, _ := r.Cookie(TenantSelectCookieName); c != nil && c.Value != "" {
		return roachpb.TenantName(c.Value), true
	}

	// No luck so far.
	//
	// TODO(knz): Make the default tenant route for HTTP configurable.
	// See: https://github.com/cockroachdb/cockroach/issues/91741
	return catconstants.SystemTenantName, false
}

func (c *serverController) getCurrentTenantNames() []roachpb.TenantName {
	var serverNames []roachpb.TenantName
	c.mu.Lock()
	for name := range c.mu.servers {
		serverNames = append(serverNames, name)
	}
	c.mu.Unlock()
	return serverNames
}

func getSessionFromCookie(sessionStr string, name roachpb.TenantName) string {
	sessionsInfo := strings.Split(sessionStr, ",")
	for idx, info := range sessionsInfo {
		if info == string(name) && idx != 0 {
			return sessionsInfo[idx-1]
		}
	}
	return sessionStr
}

// attemptLoginToAllTenants attempts login for each of the tenants and
// if successful, appends the encoded session and tenant name to the
// new session cookie. If login fails for all tenants, the StatusUnauthorized
// code will be set in the header.
func (c *serverController) attemptLoginToAllTenants() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantNames := c.getCurrentTenantNames()
		var tenantNameToSetCookieSlice []sessionCookieValue
		// The request body needs to be cloned since r.Clone() does not do it.
		clonedBody, err := io.ReadAll(r.Body)
		if err != nil {
			log.Warning(ctx, "unable to write body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		for _, name := range tenantNames {
			// Make a new sessionWriter for every tenant. A fresh header is needed
			// each time since the grpc method writes to it.
			sw := &sessionWriter{header: w.Header().Clone()}
			newReq := r.Clone(ctx)
			newReq.Body = io.NopCloser(bytes.NewBuffer(clonedBody))
			server, err := c.getOrCreateServer(ctx, name)
			if err != nil {
				log.Warningf(ctx, "unable to find tserver for tenant %q: %v", name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// Invoke the handler, passing the new sessionWriter and the cloned
			// request.
			server.getHTTPHandlerFn().ServeHTTP(sw, newReq)
			// Extract the entire set-cookie from the header. The session cookie will be
			// embedded within set-cookie.
			setCookieHeader := sw.Header().Get("set-cookie")
			if len(setCookieHeader) == 0 {
				log.Warningf(ctx, "unable to find session cookie for tenant %q", name)
			} else {
				tenantNameToSetCookieSlice = append(tenantNameToSetCookieSlice, sessionCookieValue{
					name:      string(name),
					setCookie: setCookieHeader,
				})
			}
		}
		// If the map has entries, the method to create the aggregated session should
		// be called and cookies should be set. Otherwise, login was not successful
		// for any of the tenants.
		if len(tenantNameToSetCookieSlice) > 0 {
			sessionsStr := createAggregatedSessionCookieValue(tenantNameToSetCookieSlice)
			cookie := http.Cookie{
				Name:     MultitenantSessionCookieName,
				Value:    sessionsStr,
				Path:     "/",
				HttpOnly: false,
			}
			http.SetCookie(w, &cookie)
			// The tenant cookie needs to be set at some point in order for
			// the dropdown to have a current selection on first load. Subject to change
			// once this issue is resolved: https://github.com/cockroachdb/cockroach/issues/91741.
			cookie = http.Cookie{
				Name:     TenantSelectCookieName,
				Value:    catconstants.SystemTenantName,
				Path:     "/",
				HttpOnly: false,
			}
			http.SetCookie(w, &cookie)
			if r.Header.Get(AcceptHeader) == JSONContentType {
				w.Header().Add(ContentTypeHeader, JSONContentType)
				_, err = w.Write([]byte("{}"))
				if err != nil {
					log.Warningf(ctx, "unable to write empty response :%q", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
	})
}

// attemptLogoutFromAllTenants attempts logout for each of the tenants and
// clears both the session cookie and tenant cookie. If logout fails, the
// StatusInternalServerError code will be set in the header.
func (c *serverController) attemptLogoutFromAllTenants() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tenantNames := c.getCurrentTenantNames()
		// The request body needs to be cloned since r.Clone() does not do it.
		clonedBody, err := io.ReadAll(r.Body)
		if err != nil {
			log.Warning(ctx, "unable to write body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sessionCookie, err := r.Cookie(SessionCookieName)
		if err != nil {
			log.Warning(ctx, "unable to find session cookie")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for _, name := range tenantNames {
			// Make a new sessionWriter for every tenant. A fresh header is needed
			// each time since the grpc method writes to it.
			sw := &sessionWriter{header: w.Header().Clone()}
			newReq := r.Clone(ctx)
			newReq.Body = io.NopCloser(bytes.NewBuffer(clonedBody))
			// Extract the session which matches to the current tenant name or fallback
			// to the value provided.
			relevantSession := getSessionFromCookie(sessionCookie.Value, name)

			// Set the matching session in the cookie so that the grpc method can
			// logout correctly.
			newReq.Header.Set("Cookie", "session="+relevantSession)
			server, err := c.getOrCreateServer(ctx, name)
			if err != nil {
				log.Warningf(ctx, "unable to find tserver for tenant %q: %v", name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			server.getHTTPHandlerFn().ServeHTTP(sw, newReq)
			// If a logout was unsuccessful, set cookie will be empty in which case
			// set the header to an error status and return.
			if sw.header.Get("Set-Cookie") == "" {
				log.Warningf(ctx, "logout for tenant %q failed", name)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		// Clear session and tenant cookies after all logouts have completed.
		cookie := http.Cookie{
			Name:     MultitenantSessionCookieName,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			Expires:  timeutil.Unix(0, 0),
		}
		http.SetCookie(w, &cookie)
		cookie = http.Cookie{
			Name:     TenantSelectCookieName,
			Value:    "",
			Path:     "/",
			HttpOnly: false,
			Expires:  timeutil.Unix(0, 0),
		}
		http.SetCookie(w, &cookie)
		if r.Header.Get(AcceptHeader) == JSONContentType {
			w.Header().Add(ContentTypeHeader, JSONContentType)
			_, err = w.Write([]byte("{}"))
			if err != nil {
				log.Warningf(ctx, "unable to write empty response :%q", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

// TestingGetSQLAddrForTenant extracts the SQL address for the target tenant.
// Used in tests until https://github.com/cockroachdb/cockroach/issues/84585 is resolved.
func (s *Server) TestingGetSQLAddrForTenant(ctx context.Context, tenant roachpb.TenantName) string {
	ts, err := s.serverController.getOrCreateServer(ctx, tenant)
	if err != nil {
		panic(err)
	}
	return ts.testingGetSQLAddr()
}

type errInvalidTenantMarker struct{}

func (errInvalidTenantMarker) Error() string { return "invalid tenant" }

// ErrInvalidTenant is reported as one of the error marks
// on the error result of newServerFn, i.e. errors.Is(err,
// ErrInvalidTenant) returns true, when the specified tenant
// name does not correspond to a valid tenant: either it does
// not exist or it is not currently active.
var ErrInvalidTenant error = errInvalidTenantMarker{}

// newServerForTenant is a constructor function suitable for use with
// newTenantController. It instantiates SQL servers for secondary
// tenants.
// If the specified tenant name is invalid (tenant does not exist or
// is not active), the returned error will contain the
// ErrInvalidTenant mark, which can be checked with errors.Is.
func (s *Server) newServerForTenant(
	ctx context.Context, tenantName roachpb.TenantName, index int, deregister func(),
) (onDemandServer, error) {
	// Look up the ID of the requested tenant.
	//
	// TODO(knz): use a flag or capability to decide whether to start in-memory.
	ie := s.sqlServer.internalExecutor
	datums, err := ie.QueryRow(ctx, "get-tenant-id", nil, /* txn */
		`SELECT id, active FROM system.tenants WHERE name = $1 LIMIT 1`, tenantName)
	if err != nil {
		return nil, err
	}
	if datums == nil {
		return nil, errors.Mark(errors.Newf("no tenant found with name %q", tenantName), ErrInvalidTenant)
	}

	tenantIDi := uint64(tree.MustBeDInt(datums[0]))
	isActive := tree.MustBeDBool(datums[1])
	if !isActive {
		return nil, errors.Mark(errors.Newf("tenant %q is not active", tenantName), ErrInvalidTenant)
	}
	tenantID, err := roachpb.MakeTenantID(tenantIDi)
	if err != nil {
		return nil, errors.Mark(
			errors.NewAssertionErrorWithWrappedErrf(err, "stored tenant ID %d does not convert to TenantID", tenantIDi),
			ErrInvalidTenant)
	}

	// Start the tenant server.
	tenantStopper, tenantServer, err := s.startInMemoryTenantServerInternal(ctx, tenantID, index)
	if err != nil {
		// Abandon any work done so far.
		tenantStopper.Stop(ctx)
		return nil, err
	}

	return &tenantServerWrapper{stopper: tenantStopper, server: tenantServer, deregister: deregister}, nil
}

// tenantServerWrapper implements the onDemandServer interface for SQLServerWrapper.
type tenantServerWrapper struct {
	stopper    *stop.Stopper
	server     *SQLServerWrapper
	deregister func()
}

var _ onDemandServer = (*tenantServerWrapper)(nil)

func (t *tenantServerWrapper) stop(ctx context.Context) {
	ctx = t.server.AnnotateCtx(ctx)
	t.stopper.Stop(ctx)
	t.deregister()
}

func (t *tenantServerWrapper) getHTTPHandlerFn() http.HandlerFunc {
	return t.server.http.baseHandler
}

func (t *tenantServerWrapper) testingGetSQLAddr() string {
	return t.server.sqlServer.cfg.SQLAddr
}

// systemServerWrapper implements the onDemandServer interface for Server.
//
// (We can imagine a future where the SQL service for the system
// tenant is served using the same code path as any other secondary
// tenant, in which case systemServerWrapper can disappear and we use
// tenantServerWrapper everywhere, but we're not there yet.)
//
// We do not implement the onDemandServer interface methods on *Server
// directly so as to not add noise to its go documentation.
type systemServerWrapper struct {
	server *Server
}

var _ onDemandServer = (*systemServerWrapper)(nil)

func (s *systemServerWrapper) stop(ctx context.Context) {
	// No-op: the SQL service for the system tenant never shuts down.
}

func (t *systemServerWrapper) getHTTPHandlerFn() http.HandlerFunc {
	return t.server.http.baseHandler
}

func (t *systemServerWrapper) testingGetSQLAddr() string {
	return t.server.cfg.SQLAddr
}

// startInMemoryTenantServerInternal starts an in-memory server for
// the given target tenant ID. The resulting stopper should be closed
// in any case, even when an error is returned.
//
// The value provided for index is guaranteed to be different for each
// simultaneously running server. This can be used to allocate
// distinct but predictable network listeners.
func (s *Server) startInMemoryTenantServerInternal(
	ctx context.Context, tenantID roachpb.TenantID, index int,
) (stopper *stop.Stopper, tenantServer *SQLServerWrapper, err error) {
	stopper = stop.NewStopper()

	// Create a configuration for the new tenant.
	// TODO(knz): Maybe enforce the SQL Instance ID to be equal to the KV node ID?
	// See: https://github.com/cockroachdb/cockroach/issues/84602
	parentCfg := s.cfg
	baseCfg, sqlCfg, err := makeInMemoryTenantServerConfig(ctx, tenantID, index, parentCfg, stopper)
	if err != nil {
		return stopper, nil, err
	}

	// Create a child stopper for this tenant's server.
	ambientCtx := baseCfg.AmbientCtx
	stopper.SetTracer(baseCfg.Tracer)

	// New context, since we're using a separate tracer.
	startCtx := ambientCtx.AnnotateCtx(context.Background())
	startCtx = logtags.AddTags(startCtx, logtags.FromContext(ctx))

	// Inform the logs we're starting a new server.
	log.Infof(startCtx, "starting tenant server")

	// Now start the tenant proper.
	tenantServer, err = NewTenantServer(startCtx, stopper, baseCfg, sqlCfg)
	if err != nil {
		return stopper, tenantServer, err
	}

	// Start a goroutine that watches for shutdown requests issued by
	// the server itself.
	// Note: we can't use stopper.RunAsyncTask() here because otherwise
	// the call to stopper.Stop() inside the goroutine would deadlock.
	go func() {
		select {
		case req := <-tenantServer.ShutdownRequested():
			log.Warningf(startCtx,
				"in-memory server for tenant %v is requesting shutdown: %d %v",
				tenantID, req.Reason, req.ShutdownCause())
			stopper.Stop(startCtx)
		case <-stopper.ShouldQuiesce():
		}
	}()

	if err := tenantServer.Start(startCtx); err != nil {
		return stopper, tenantServer, err
	}

	// Show the tenant details in logs.
	// TODO(knz): Remove this once we can use a single listener.
	if err := reportTenantInfo(startCtx, baseCfg, sqlCfg); err != nil {
		return stopper, tenantServer, err
	}

	return stopper, tenantServer, nil
}

func makeInMemoryTenantServerConfig(
	ctx context.Context,
	tenantID roachpb.TenantID,
	index int,
	kvServerCfg Config,
	stopper *stop.Stopper,
) (baseCfg BaseConfig, sqlCfg SQLConfig, err error) {
	st := cluster.MakeClusterSettings()

	// This version initialization is copied from cli/mt_start_sql.go.
	//
	// TODO(knz): Why is this even useful? The comment refers to v21.1
	// compatibility, yet if we don't do this, the server panics with
	// "version not initialized". This might be related to:
	// https://github.com/cockroachdb/cockroach/issues/84587
	if err := clusterversion.Initialize(
		ctx, st.Version.BinaryMinSupportedVersion(), &st.SV,
	); err != nil {
		return baseCfg, sqlCfg, err
	}

	tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV))

	// Find a suitable store directory.
	tenantDir := "tenant-" + tenantID.String()
	storeDir := ""
	for _, storeSpec := range kvServerCfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		storeDir = filepath.Join(storeSpec.Path, tenantDir)
		break
	}
	if storeDir == "" {
		storeDir = tenantDir
	}
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return baseCfg, sqlCfg, err
	}

	storeSpec, err := base.NewStoreSpec(storeDir)
	if err != nil {
		return baseCfg, sqlCfg, errors.Wrap(err, "cannot create store spec")
	}
	baseCfg = MakeBaseConfig(st, tr, storeSpec)

	// Uncontroversial inherited values.
	baseCfg.Config.Insecure = kvServerCfg.Config.Insecure
	baseCfg.Config.User = kvServerCfg.Config.User
	baseCfg.Config.DisableTLSForHTTP = kvServerCfg.Config.DisableTLSForHTTP
	baseCfg.Config.AcceptSQLWithoutTLS = kvServerCfg.Config.AcceptSQLWithoutTLS
	baseCfg.Config.RPCHeartbeatInterval = kvServerCfg.Config.RPCHeartbeatInterval
	baseCfg.Config.RPCHeartbeatTimeout = kvServerCfg.Config.RPCHeartbeatTimeout
	baseCfg.Config.ClockDevicePath = kvServerCfg.Config.ClockDevicePath
	baseCfg.Config.ClusterName = kvServerCfg.Config.ClusterName
	baseCfg.Config.DisableClusterNameVerification = kvServerCfg.Config.DisableClusterNameVerification

	baseCfg.MaxOffset = kvServerCfg.BaseConfig.MaxOffset
	baseCfg.StorageEngine = kvServerCfg.BaseConfig.StorageEngine
	baseCfg.TestingInsecureWebAccess = kvServerCfg.BaseConfig.TestingInsecureWebAccess
	baseCfg.Locality = kvServerCfg.BaseConfig.Locality
	baseCfg.SpanConfigsDisabled = kvServerCfg.BaseConfig.SpanConfigsDisabled
	baseCfg.EnableDemoLoginEndpoint = kvServerCfg.BaseConfig.EnableDemoLoginEndpoint
	baseCfg.TestingKnobs = kvServerCfg.BaseConfig.SecondaryTenantKnobs

	// TODO(knz): use a single network interface for all tenant servers.
	// See: https://github.com/cockroachdb/cockroach/issues/84585
	portOffset := kvServerCfg.Config.SecondaryTenantPortOffset
	var err1, err2, err3, err4 error
	baseCfg.Addr, err1 = rederivePort(index, kvServerCfg.Config.Addr, "", portOffset)
	baseCfg.AdvertiseAddr, err2 = rederivePort(index, kvServerCfg.Config.AdvertiseAddr, baseCfg.Addr, portOffset)
	baseCfg.SQLAddr, err3 = rederivePort(index, kvServerCfg.Config.SQLAddr, "", portOffset)
	baseCfg.SQLAdvertiseAddr, err4 = rederivePort(index, kvServerCfg.Config.SQLAdvertiseAddr, baseCfg.SQLAddr, portOffset)
	if err := errors.CombineErrors(err1,
		errors.CombineErrors(err2,
			errors.CombineErrors(err3, err4))); err != nil {
		return baseCfg, sqlCfg, err
	}

	// This will change when we can use a single SQL listener.
	const splitSQL = false
	if splitSQL {
		baseCfg.SplitListenSQL = true
	} else {
		baseCfg.SplitListenSQL = false
		baseCfg.Addr, baseCfg.SQLAddr = baseCfg.SQLAddr, baseCfg.Addr
		baseCfg.AdvertiseAddr, baseCfg.SQLAdvertiseAddr = baseCfg.SQLAdvertiseAddr, baseCfg.AdvertiseAddr
		baseCfg.SQLAddr = ""
		baseCfg.SQLAdvertiseAddr = ""
	}

	// The parent server will route HTTP requests to us.
	baseCfg.DisableHTTPListener = true
	// Nevertheless, we like to know our own HTTP address.
	baseCfg.HTTPAddr = kvServerCfg.Config.HTTPAddr
	baseCfg.HTTPAdvertiseAddr = kvServerCfg.Config.HTTPAdvertiseAddr

	// Define the unix socket intelligently.
	// See: https://github.com/cockroachdb/cockroach/issues/84585
	baseCfg.SocketFile = ""

	// TODO(knz): Make the TLS config separate per tenant.
	// See https://cockroachlabs.atlassian.net/browse/CRDB-14539.
	baseCfg.SSLCertsDir = kvServerCfg.BaseConfig.SSLCertsDir
	baseCfg.SSLCAKey = kvServerCfg.BaseConfig.SSLCAKey

	// TODO(knz): startSampleEnvironment() should not be part of startTenantInternal. For now,
	// disable the mechanism manually.
	// See: https://github.com/cockroachdb/cockroach/issues/84589
	baseCfg.GoroutineDumpDirName = ""
	baseCfg.HeapProfileDirName = ""
	baseCfg.CPUProfileDirName = ""
	baseCfg.InflightTraceDirName = ""

	// TODO(knz): Define a meaningful storage config for each tenant,
	// see: https://github.com/cockroachdb/cockroach/issues/84588.
	useStore := kvServerCfg.SQLConfig.TempStorageConfig.Spec
	tempStorageCfg := base.TempStorageConfigFromEnv(
		ctx, st, useStore, "" /* parentDir */, kvServerCfg.SQLConfig.TempStorageConfig.Mon.MaximumBytes())
	// TODO(knz): Make tempDir configurable.
	tempDir := useStore.Path
	if tempStorageCfg.Path, err = fs.CreateTempDir(tempDir, TempDirPrefix, stopper); err != nil {
		return baseCfg, sqlCfg, errors.Wrap(err, "could not create temporary directory for temp storage")
	}
	if useStore.Path != "" {
		recordPath := filepath.Join(useStore.Path, TempDirsRecordFilename)
		if err := fs.RecordTempDir(recordPath, tempStorageCfg.Path); err != nil {
			return baseCfg, sqlCfg, errors.Wrap(err, "could not record temp dir")
		}
	}

	sqlCfg = MakeSQLConfig(tenantID, tempStorageCfg)

	// Split for each tenant, see https://github.com/cockroachdb/cockroach/issues/84588.
	sqlCfg.ExternalIODirConfig = kvServerCfg.SQLConfig.ExternalIODirConfig

	// Use the internal connector instead of the network.
	// See: https://github.com/cockroachdb/cockroach/issues/84591
	sqlCfg.TenantKVAddrs = []string{kvServerCfg.BaseConfig.Config.AdvertiseAddr}

	// Use the same memory budget for each secondary tenant. The assumption
	// here is that we use max 2 tenants, and that under common loads one
	// of them will be mostly idle.
	// We might want to reconsider this if we use more than 1 in-memory tenant at a time.
	sqlCfg.MemoryPoolSize = kvServerCfg.SQLConfig.MemoryPoolSize
	sqlCfg.TableStatCacheSize = kvServerCfg.SQLConfig.TableStatCacheSize
	sqlCfg.QueryCacheSize = kvServerCfg.SQLConfig.QueryCacheSize

	return baseCfg, sqlCfg, nil
}

// rederivePort computes a host:port pair for a secondary tenant.
// TODO(knz): All this can be removed once we implement a single
// network listener.
// See https://github.com/cockroachdb/cockroach/issues/84604.
func rederivePort(index int, addrToChange string, prevAddr string, portOffset int) (string, error) {
	h, port, err := addr.SplitHostPort(addrToChange, "0")
	if err != nil {
		return "", errors.Wrapf(err, "%d: %q", index, addrToChange)
	}

	if portOffset == 0 {
		// Shortcut: random selection for base address.
		return net.JoinHostPort(h, "0"), nil
	}

	var pnum int
	if port != "" {
		pnum, err = strconv.Atoi(port)
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, addrToChange)
		}
	}

	if prevAddr != "" && pnum == 0 {
		// Try harder to find a port number, by taking one from
		// the previously computed addr.
		_, port2, err := addr.SplitHostPort(prevAddr, "0")
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, prevAddr)
		}
		pnum, err = strconv.Atoi(port2)
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, prevAddr)
		}
	}

	// Do we have a base port to go with now?
	if pnum == 0 {
		// No, bail.
		return "", errors.Newf("%d: no base port available for computation in %q / %q", index, addrToChange, prevAddr)
	}
	port = strconv.Itoa(pnum + portOffset + index)
	return net.JoinHostPort(h, port), nil
}

func reportTenantInfo(ctx context.Context, baseCfg BaseConfig, sqlCfg SQLConfig) error {
	var buf redact.StringBuilder
	buf.Printf("started tenant SQL server at %s\n", timeutil.Now())
	buf.Printf("webui:\t%s\n", baseCfg.AdminURL())
	clientConnOptions, serverParams := MakeServerOptionsForURL(baseCfg.Config)
	pgURL, err := clientsecopts.MakeURLForServer(clientConnOptions, serverParams, url.User(username.RootUser))
	if err != nil {
		log.Errorf(ctx, "failed computing the URL: %v", err)
	} else {
		buf.Printf("sql:\t%s\n", pgURL.ToPQ())
		buf.Printf("sql (JDBC):\t%s\n", pgURL.ToJDBC())
	}
	if baseCfg.SocketFile != "" {
		buf.Printf("socket:\t%s\n", baseCfg.SocketFile)
	}
	if tmpDir := sqlCfg.TempStorageConfig.Path; tmpDir != "" {
		buf.Printf("temp dir:\t%s\n", tmpDir)
	}
	buf.Printf("clusterID:\t%s\n", baseCfg.ClusterIDContainer.Get())
	buf.Printf("tenantID:\t%s\n", sqlCfg.TenantID)
	buf.Printf("instanceID:\t%d\n", baseCfg.IDContainer.Get())
	// Collect the formatted string and show it to the user.
	msg, err := util.ExpandTabsInRedactableBytes(buf.RedactableBytes())
	if err != nil {
		return err
	}
	msgS := msg.ToString()
	log.Ops.Infof(ctx, "tenant startup completed:\n%s", msgS)
	return nil
}
