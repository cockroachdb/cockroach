// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver

import (
	"context"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// SessionCookieName is the name of the cookie used for HTTP auth.
	SessionCookieName = "session"

	// TenantSelectCookieName is the name of the HTTP cookie used to select a particular tenant,
	// if the custom header is not specified.
	TenantSelectCookieName = `tenant`
)

// SessionCookieValue defines the data needed to construct the
// aggregate session cookie in the order provided.
type SessionCookieValue struct {
	// The name of the tenant.
	name string
	// The value of set-cookie.
	setCookie string
}

// MakeSessionCookieValue creates a SessionCookieValue from the provided
// tenant name and set-cookie value.
func MakeSessionCookieValue(name, setCookie string) SessionCookieValue {
	return SessionCookieValue{
		name:      name,
		setCookie: setCookie,
	}
}

// Name returns the name in the tenant in the cookie value.
func (s SessionCookieValue) Name() string {
	return s.name
}

// CreateAggregatedSessionCookieValue is used for multi-tenant login.
// It takes a slice of SessionCookieValue and converts it to a single
// string which is the aggregated session. Currently the format of the
// aggregated session is: `session,tenant_name,session2,tenant_name2` etc.
func CreateAggregatedSessionCookieValue(sessionCookieValue []SessionCookieValue) string {
	var sessionsStr string
	for _, val := range sessionCookieValue {
		sessionCookieSlice := strings.Split(strings.ReplaceAll(val.setCookie, "session=", ""), ";")
		sessionsStr += sessionCookieSlice[0] + "," + val.name + ","
	}
	if len(sessionsStr) > 0 {
		sessionsStr = sessionsStr[:len(sessionsStr)-1]
	}
	return sessionsStr
}

// FindAndDecodeSessionCookie looks for multitenant-session and session cookies
// in the cookies slice. If they are found the value will need to be processed if
// it is a multitenant-session cookie (see findSessionCookieValueForTenant for details)
// and then decoded. If there is an error in decoding or processing, the function
// will return an error.
func FindAndDecodeSessionCookie(
	ctx context.Context, st *cluster.Settings, cookies []*http.Cookie,
) (*serverpb.SessionCookie, error) {
	found := false
	var sessionCookie *serverpb.SessionCookie
	tenantSelectCookieVal := findTenantSelectCookieValue(cookies)
	for _, cookie := range cookies {
		if cookie.Name != SessionCookieName {
			continue
		}
		found = true
		mtSessionVal, err := FindSessionCookieValueForTenant(
			st,
			cookie,
			tenantSelectCookieVal)
		if err != nil {
			return sessionCookie, srverrors.APIInternalError(ctx, err)
		}
		if mtSessionVal != "" {
			cookie.Value = mtSessionVal
		}
		sessionCookie, err = decodeSessionCookie(cookie)
		if err != nil {
			// Multiple cookies with the same name may be included in the
			// header. We continue searching even if we find a matching
			// name with an invalid value.
			log.Infof(ctx, "found a matching cookie that failed decoding: %v", err)
			found = false
			continue
		}
		break
	}
	if !found {
		return nil, http.ErrNoCookie
	}
	return sessionCookie, nil
}

// FindSessionCookieValueForTenant finds the encoded session in the provided
// aggregated session cookie value established in multi-tenant clusters that's
// associated with the provided tenant name. If an empty tenant name is provided,
// we default to the DefaultTenantSelect cluster setting value.
//
// If the method cannot find a match between the tenant name and session, or
// if the provided session cookie is nil, it will return an empty string.
//
// e.g. tenant name is "system" and session cookie's value is
// "abcd1234,system,efgh5678,app" the output will be "abcd1234".
//
// In the case of legacy session cookies, where tenant names are not encoded
// into the cookie value, we assume that the session belongs to defaultTenantSelect.
// Note that these legacy session cookies only contained a single session string
// as the cookie's value.
func FindSessionCookieValueForTenant(
	st *cluster.Settings, sessionCookie *http.Cookie, tenantName string,
) (string, error) {
	if sessionCookie == nil {
		return "", nil
	}
	if mtSessionStr := sessionCookie.Value; sessionCookie.Value != "" {
		sessionSlice := strings.Split(mtSessionStr, ",")
		if len(sessionSlice) == 1 {
			// If no separator was found in the cookie value, this is likely
			// a cookie from a previous CRDB version where the cookie value
			// contained a single session string without any tenant names encoded.
			// To maintain backwards compatibility, assume this session belongs
			// to the default tenant. In this case, the entire cookie value is
			// the session string.
			return mtSessionStr, nil
		}
		if tenantName == "" {
			tenantName = multitenant.DefaultTenantSelect.Get(&st.SV)
		}
		var encodedSession string
		for idx, val := range sessionSlice {
			if val == tenantName && idx > 0 {
				encodedSession = sessionSlice[idx-1]
			}
		}
		if encodedSession == "" {
			return "", errors.Newf("unable to find session cookie value that matches tenant %q", tenantName)
		}
		return encodedSession, nil
	}
	return "", nil
}

// findTenantSelectCookieValue iterates through all request cookies in order
// to find the value of the tenant select cookie. If the tenant select cookie
// is not found, it returns the empty string.
func findTenantSelectCookieValue(cookies []*http.Cookie) string {
	for _, c := range cookies {
		if c.Name == TenantSelectCookieName {
			return c.Value
		}
	}
	return ""
}

// CreateSessionCookie constructs an HTTP cookie that holds a DB
// Console session. This cookie is always marked `HttpOnly` and can be
// optionally marked `Secure` if the cluster is running in secure mode,
// based on the `forHTTPSOnly` arg.
func CreateSessionCookie(value string, forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   forHTTPSOnly,
	}
}

// CreateEmptySessionCookieWithImmediateExpiry constructs an HTTP
// cookie that clears the session cookie by setting an empty cookie
// with the same name and an expiry at the Unix epoch. This will cause
// the browser to clear the cookie since it expires immediately.
func CreateEmptySessionCookieWithImmediateExpiry(forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   forHTTPSOnly,
		Expires:  timeutil.Unix(0, 0),
	}
}

// CreateTenantSelectCookie constructs an HTTP cookie that holds a DB Console
// tenant selection. This cookis is **not** marked `HttpOnly` because
// its value can be inspected and modified by the client-side
// Javascript code if the user changes their tenant selection. It can
// be optionally marked `Secure` if you're runinning in secure mode by
// setting the `forHttpsOnly` argument to true.
func CreateTenantSelectCookie(value string, forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     TenantSelectCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: false,
		Secure:   forHTTPSOnly,
	}
}

// CreateEmptyTenantSelectCookieWithImmediateExpiry constructs an HTTP
// cookie that clears the tenant cookie by setting an empty cookie
// with the same name and an expiry at the Unix epoch. This will cause
// the browser to clear the cookie since it expires immediately.
func CreateEmptyTenantSelectCookieWithImmediateExpiry(forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     TenantSelectCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: false,
		Secure:   forHTTPSOnly,
		Expires:  timeutil.Unix(0, 0),
	}
}

// CreateOIDCCookie constructs a cookie to hold the OIDC secret that's
// used to validate requests between `/login` and `/callback` requests.
// This cookie contains a hash that's shared by the browser between the
// two requests and is used to validate that the `/callback` was
// triggered in response to a valid login attempt from this cluster.
// This cookie is **always** `Secure` and `HttpOnly` since OIDC doesn't
// work on an insecure cluster. Its name is an argument to avoid a CCL
// dependency.
func CreateOIDCCookie(name string, value string) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	}
}
