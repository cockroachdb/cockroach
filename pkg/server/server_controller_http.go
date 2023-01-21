// Copyright 2023 The Cockroach Authors.
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
	"io"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// TenantSelectHeader is the HTTP header used to select a particular tenant.
	TenantSelectHeader = `X-Cockroach-Tenant`

	// TenantNameParamInQueryURL is the HTTP query URL parameter used to select a particular tenant.
	TenantNameParamInQueryURL = "tenant_name"

	// TenantSelectCookieName is the name of the HTTP cookie used to select a particular tenant,
	// if the custom header is not specified.
	TenantSelectCookieName = `tenant`

	// AcceptHeader is the canonical header name for accept.
	AcceptHeader = "Accept"

	// ContentTypeHeader is the canonical header name for content type.
	ContentTypeHeader = "Content-Type"

	// JSONContentType is the JSON content type.
	JSONContentType = "application/json"
)

// httpMux redirects incoming HTTP requests to the server selected by
// the special HTTP request header.
// If no tenant is specified, the default tenant is used.
func (c *serverController) httpMux(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantName, nameProvided := getTenantNameFromHTTPRequest(c.st, r)
	// if the client didnt specify tenant name call these for login/logout.
	if !nameProvided {
		switch r.URL.Path {
		case loginPath, DemoLoginPath:
			c.attemptLoginToAllTenants().ServeHTTP(w, r)
			return
		case logoutPath:
			c.attemptLogoutFromAllTenants().ServeHTTP(w, r)
			return
		}
	}
	s, err := c.getServer(ctx, tenantName)
	if err != nil {
		log.Warningf(ctx, "unable to find server for tenant %q: %v", tenantName, err)
		// Clear session and tenant cookies after all logouts have completed.
		http.SetCookie(w, &http.Cookie{
			Name:     MultitenantSessionCookieName,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			Expires:  timeutil.Unix(0, 0),
		})
		http.SetCookie(w, &http.Cookie{
			Name:     TenantSelectCookieName,
			Value:    "",
			Path:     "/",
			HttpOnly: false,
			Expires:  timeutil.Unix(0, 0),
		})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	s.getHTTPHandlerFn()(w, r)
}

func getTenantNameFromHTTPRequest(
	st *cluster.Settings, r *http.Request,
) (roachpb.TenantName, bool) {
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

	// No luck so far. Use the configured default.
	return roachpb.TenantName(defaultTenantSelect.Get(&st.SV)), false
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
			server, err := c.getServer(ctx, name)
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
				log.Warningf(ctx, "unable to find session cookie for tenant %q: HTTP %d - %s", name, sw.code, &sw.buf)
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
			// the dropdown to have a current selection on first load.
			cookie = http.Cookie{
				Name:     TenantSelectCookieName,
				Value:    defaultTenantSelect.Get(&c.st.SV),
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
			server, err := c.getServer(ctx, name)
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
