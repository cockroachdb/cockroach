// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	// TenantSelectHeader is the HTTP header used to select a particular tenant.
	TenantSelectHeader = `X-Cockroach-Tenant`

	// ClusterNameParamInQueryURL is the HTTP query URL parameter used
	// to select a particular virtual cluster.
	ClusterNameParamInQueryURL = "cluster"

	NoFallbackParam = "nofallback"

	// AcceptHeader is the canonical header name for accept.
	AcceptHeader = "Accept"

	// ContentTypeHeader is the canonical header name for content type.
	ContentTypeHeader = "Content-Type"

	// JSONContentType is the JSON content type.
	JSONContentType = "application/json"
)

// ServerHTTPBasePath is a cluster setting that contains the path to
// route the user to after successful login. It is intended to be
// overridden in cases where DB Console is being proxied.
var ServerHTTPBasePath = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"server.http.base_path",
	"path to redirect the user to upon succcessful login",
	"/",
	settings.WithPublic,
)

// httpMux redirects incoming HTTP requests to the server selected by
// the special HTTP request header.
// If no tenant is specified, the default tenant is used.
func (c *serverController) httpMux(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// The Login/Logout fanout is currently **always** executed regardless of the
	// tenant selection in the request. This simplifies the flows in cases where a
	// user is already logged-in and issues a login request manually, or when a
	// user clicks on a login link from cockroach demo. These situations
	// previously would result in odd outcomes because a login request could get
	// routed to a specific node and skip the fanout, creating inconsistent
	// outcomes that were path-dependent on the user's existing cookies.
	switch r.URL.Path {
	case authserver.LoginPath, authserver.DemoLoginPath:
		c.attemptLoginToAllTenants().ServeHTTP(w, r)
		return
	case authserver.LogoutPath:
		// Since we do not support per-tenant logout until
		// https://github.com/cockroachdb/cockroach/issues/92855
		// is completed, we should always fanout a logout
		// request in order to clear the multi-tenant session
		// cookies properly.
		c.attemptLogoutFromAllTenants().ServeHTTP(w, r)
		return
	}
	tenantName := getTenantNameFromHTTPRequest(c.st, r)
	noFallback := false
	if noFallbackValue := r.URL.Query().Get(NoFallbackParam); noFallbackValue != "" {
		noFallback, _ = strconv.ParseBool(noFallbackValue)
	}

	s, _, err := c.getServer(ctx, tenantName)
	if err != nil {
		if noFallback {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		log.Warningf(ctx, "unable to find server for tenant %q: %v", tenantName, err)
		// Clear session and tenant cookies since it appears they reference invalid state.
		http.SetCookie(w, authserver.CreateEmptySessionCookieWithImmediateExpiry(!c.disableTLSForHTTP))
		http.SetCookie(w, authserver.CreateEmptyTenantSelectCookieWithImmediateExpiry(!c.disableTLSForHTTP))
		// Fall back to serving requests from the default tenant. This helps us serve
		// the root path along with static assets even when the browser contains invalid
		// tenant names or sessions (common during development). Otherwise the user can
		// get into a state where they cannot load DB Console assets at all due to invalid
		// cookies.
		defaultTenantName := roachpb.TenantName(multitenant.DefaultTenantSelect.Get(&c.st.SV))
		s, _, err = c.getServer(ctx, defaultTenantName)
		if err != nil {
			if log.V(1) {
				// This could get triggered often if a customer has the default
				// tenant set up but not active yet. Every DB Console HTTP
				// request will go through this branch in that scenario.
				log.Warningf(ctx, "unable to find server for default tenant %q: %v", defaultTenantName, err)
			}
			sys, _, errSystem := c.getServer(ctx, catconstants.SystemTenantName)
			if errSystem != nil {
				log.Warningf(ctx, "unable to find server for default tenant %q: %v", defaultTenantName, err)
				log.Warningf(ctx, "unable to find server for system tenant: %v", errSystem)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			sys.getHTTPHandlerFn()(w, r)
			return
		}
		s.getHTTPHandlerFn()(w, r)
		return
	}
	s.getHTTPHandlerFn()(w, r)
}

func getTenantNameFromHTTPRequest(st *cluster.Settings, r *http.Request) roachpb.TenantName {
	// Highest priority is manual override on the URL query parameters.
	if tenantName := r.URL.Query().Get(ClusterNameParamInQueryURL); tenantName != "" {
		return roachpb.TenantName(tenantName)
	}

	// If not in parameters, try an explicit header.
	if tenantName := r.Header.Get(TenantSelectHeader); tenantName != "" {
		return roachpb.TenantName(tenantName)
	}

	// No parameter, no explicit header. Is there a cookie?
	if c, _ := r.Cookie(authserver.TenantSelectCookieName); c != nil && c.Value != "" {
		return roachpb.TenantName(c.Value)
	}

	// No luck so far. Use the configured default.
	return roachpb.TenantName(multitenant.DefaultTenantSelect.Get(&st.SV))
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
		var tenantNameToSetCookieSlice []authserver.SessionCookieValue
		// The request body needs to be cloned since r.Clone() does not do it.
		clonedBody, err := io.ReadAll(r.Body)
		if err != nil {
			log.Warning(ctx, "unable to write body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		redirect := false
		redirectLocation := "/" // default to home page
		collectedErrors := make([]string, len(tenantNames))
		for i, name := range tenantNames {
			server, _, err := c.getServer(ctx, name)
			if err != nil {
				if errors.Is(err, errNoTenantServerRunning) {
					// Server has stopped after the call to
					// getCurrentTenantNames(), or may not be fully started yet.
					// This is OK. Just skip over it.
					continue
				}
				log.Warningf(ctx, "looking up server for tenant %q: %v", name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Make a new sessionWriter for every tenant. A fresh header is needed
			// each time since the grpc method writes to it.
			sw := &sessionWriter{header: w.Header().Clone()}
			newReq := r.Clone(ctx)
			newReq.Body = io.NopCloser(bytes.NewBuffer(clonedBody))
			// Invoke the handler, passing the new sessionWriter and the cloned
			// request.
			server.getHTTPHandlerFn().ServeHTTP(sw, newReq)
			// Extract the entire set-cookie from the header. The session cookie will be
			// embedded within set-cookie.
			setCookieHeader := sw.Header().Get("set-cookie")
			if len(setCookieHeader) == 0 {
				collectedErrors[i] = sw.buf.String()
				log.Warningf(ctx, "unable to find session cookie for tenant %q: HTTP %d - %s", name, sw.code, &sw.buf)
			} else {
				tenantNameToSetCookieSlice = append(tenantNameToSetCookieSlice, authserver.MakeSessionCookieValue(
					string(name),
					setCookieHeader,
				))
				// In the case of /demologin, we want to redirect to the provided location
				// in the header. If we get back a cookie along with an
				// http.StatusTemporaryRedirect code, be sure to transfer the response code
				// along with the Location into the ResponseWriter later.
				if sw.code == http.StatusTemporaryRedirect {
					redirect = true
					if locationHeader, ok := sw.Header()["Location"]; ok && len(locationHeader) > 0 {
						redirectLocation = locationHeader[0]
					}
				}
			}
		}
		// If the map has entries, the method to create the aggregated session should
		// be called and cookies should be set. Otherwise, login was not successful
		// for any of the tenants.
		if len(tenantNameToSetCookieSlice) > 0 {
			sessionsStr := authserver.CreateAggregatedSessionCookieValue(tenantNameToSetCookieSlice)
			http.SetCookie(w, authserver.CreateSessionCookie(sessionsStr, !c.disableTLSForHTTP /* forHTTPSOnly */))
			// The tenant cookie needs to be set at some point in order for
			// the dropdown to have a current selection on first load.

			// We only set the default selection from the cluster setting
			// if it's one of the valid logins. Otherwise, we just use the
			// first one in the list.
			tenantSelection := tenantNameToSetCookieSlice[0].Name()
			defaultName := multitenant.DefaultTenantSelect.Get(&c.st.SV)
			for _, t := range tenantNameToSetCookieSlice {
				if t.Name() == defaultName {
					tenantSelection = t.Name()
					break
				}
			}
			http.SetCookie(w, authserver.CreateTenantSelectCookie(tenantSelection, !c.disableTLSForHTTP /* forHTTPSOnly */))
			if r.Header.Get(AcceptHeader) == JSONContentType {
				w.Header().Add(ContentTypeHeader, JSONContentType)
				_, err = w.Write([]byte("{}"))
				if err != nil {
					log.Warningf(ctx, "unable to write empty response :%q", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
			if redirect {
				http.Redirect(w, r, redirectLocation, http.StatusTemporaryRedirect)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			_, err := w.Write([]byte(strings.Join(collectedErrors, "\n")))
			if err != nil {
				log.Warningf(ctx, "unable to write error to http request :%q", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
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
		sessionCookie, err := r.Cookie(authserver.SessionCookieName)
		if errors.Is(err, http.ErrNoCookie) {
			sessionCookie, err = r.Cookie(authserver.SessionCookieName)
			if err != nil {
				log.Warningf(ctx, "unable to find session cookie: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		if err != nil {
			log.Warningf(ctx, "unable to find multi-tenant session cookie: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for _, name := range tenantNames {
			server, _, err := c.getServer(ctx, name)
			if err != nil {
				if errors.Is(err, errNoTenantServerRunning) {
					// Server has stopped after the call to
					// getCurrentTenantNames(), or may not be started yet. This
					// is OK. Just skip over it.
					continue
				}
				log.Warningf(ctx, "looking up server for tenant %q: %v", name, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

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
			server.getHTTPHandlerFn().ServeHTTP(sw, newReq)
			// If a logout was unsuccessful, set cookie will be empty. In
			// that case, just report the failure and move to the next
			// server. This may be because that particular server is in the
			// process of shutting down.
			if sw.Header().Get("Set-Cookie") == "" {
				log.Warningf(ctx, "logout for tenant %q failed: HTTP %d - %s", name, sw.code, &sw.buf)
			}
		}
		// Clear session and tenant cookies after all logouts have completed.
		http.SetCookie(w, authserver.CreateEmptySessionCookieWithImmediateExpiry(!c.disableTLSForHTTP /* forHTTPSOnly */))
		http.SetCookie(w, authserver.CreateEmptyTenantSelectCookieWithImmediateExpiry(!c.disableTLSForHTTP /* forHTTPSOnly */))

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
