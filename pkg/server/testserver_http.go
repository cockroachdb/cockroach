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
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// httpTestServer is embedded in TestServer / TenantServer to
// provide the HTTP API subset of TestTenantInterface.
type httpTestServer struct {
	t struct {
		// We need a sub-struct to avoid ambiguous overlap with the fields
		// of *Server, which are also embedded in TestServer.
		authentication *authenticationServer
		sqlServer      *SQLServer
		tenantName     roachpb.TenantName
	}

	// authClient is an http.Client that has been authenticated to access the
	// Admin UI.
	authClient [2]struct {
		httpClient http.Client
		cookie     *serverpb.SessionCookie
		once       sync.Once
		err        error
	}
}

type tenantHeaderDecorator struct {
	http.RoundTripper

	tenantName roachpb.TenantName
}

func (t tenantHeaderDecorator) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.tenantName != "" {
		req.Header.Add(TenantSelectHeader, string(t.tenantName))
	}
	return t.RoundTripper.RoundTrip(req)
}

var _ http.RoundTripper = &tenantHeaderDecorator{}

// AdminURL implements TestServerInterface.
func (ts *httpTestServer) AdminURL() string {
	u := ts.t.sqlServer.execCfg.RPCContext.Config.AdminURL()
	if ts.t.tenantName != "" {
		q := u.Query()
		q.Add(TenantNameParamInQueryURL, string(ts.t.tenantName))
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// GetUnauthenticatedHTTPClient implements TestServerInterface.
func (ts *httpTestServer) GetUnauthenticatedHTTPClient() (http.Client, error) {
	client, err := ts.t.sqlServer.execCfg.RPCContext.GetHTTPClient()
	if err != nil {
		return client, err
	}
	client.Transport = &tenantHeaderDecorator{
		RoundTripper: client.Transport,
		tenantName:   ts.t.tenantName,
	}
	return client, nil
}

// GetAdminHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAdminHTTPClient() (http.Client, error) {
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(
		authenticatedUserName(), true, serverutils.SingleTenantSession,
	)
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAuthenticatedHTTPClient(
	isAdmin bool, session serverutils.SessionType,
) (http.Client, error) {
	authUser := authenticatedUserName()
	if !isAdmin {
		authUser = authenticatedUserNameNoAdmin()
	}
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authUser, isAdmin, session)
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAuthSession(isAdmin bool) (*serverpb.SessionCookie, error) {
	authUser := authenticatedUserName()
	if !isAdmin {
		authUser = authenticatedUserNameNoAdmin()
	}
	_, cookie, err := ts.getAuthenticatedHTTPClientAndCookie(authUser, isAdmin, serverutils.SingleTenantSession)
	return cookie, err
}

func (ts *httpTestServer) getAuthenticatedHTTPClientAndCookie(
	authUser username.SQLUsername, isAdmin bool, session serverutils.SessionType,
) (http.Client, *serverpb.SessionCookie, error) {
	authIdx := 0
	if isAdmin {
		authIdx = 1
	}
	authClient := &ts.authClient[authIdx]
	authClient.once.Do(func() {
		// Create an authentication session for an arbitrary admin user.
		authClient.err = func() error {
			// The user needs to exist as the admin endpoints will check its role.
			if err := ts.createAuthUser(authUser, isAdmin); err != nil {
				return err
			}

			id, secret, err := ts.t.authentication.newAuthSession(context.TODO(), authUser)
			if err != nil {
				return err
			}
			rawCookie := &serverpb.SessionCookie{
				ID:     id,
				Secret: secret,
			}
			// Encode a session cookie and store it in a cookie jar.
			cookie, err := EncodeSessionCookie(rawCookie, false /* forHTTPSOnly */)
			if err != nil {
				return err
			}
			cookieJar, err := cookiejar.New(nil)
			if err != nil {
				return err
			}
			url, err := url.Parse(ts.t.sqlServer.execCfg.RPCContext.Config.AdminURL().String())
			if err != nil {
				return err
			}
			if session == serverutils.MultiTenantSession {
				cookie.Name = SessionCookieName
				cookie.Value = fmt.Sprintf("%s,%s", cookie.Value, ts.t.tenantName)
			}
			cookieJar.SetCookies(url, []*http.Cookie{cookie})
			// Create an httpClient and attach the cookie jar to the client.
			authClient.httpClient, err = ts.t.sqlServer.execCfg.RPCContext.GetHTTPClient()
			if err != nil {
				return err
			}
			rawCookieBytes, err := protoutil.Marshal(rawCookie)
			if err != nil {
				return err
			}
			authClient.httpClient.Transport = &tenantHeaderDecorator{
				RoundTripper: &v2AuthDecorator{
					RoundTripper: authClient.httpClient.Transport,
					session:      base64.StdEncoding.EncodeToString(rawCookieBytes),
				},
				tenantName: ts.t.tenantName,
			}
			authClient.httpClient.Jar = cookieJar
			authClient.cookie = rawCookie
			return nil
		}()
	})

	return authClient.httpClient, authClient.cookie, authClient.err
}

func (ts *httpTestServer) createAuthUser(userName username.SQLUsername, isAdmin bool) error {
	if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
		"create-auth-user", nil,
		sessiondata.RootUserSessionDataOverride,
		fmt.Sprintf("CREATE USER %s", userName.Normalized()),
	); err != nil {
		return err
	}
	if isAdmin {
		if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
			"grant-admin", nil,
			sessiondata.RootUserSessionDataOverride,
			fmt.Sprintf("GRANT admin TO %s WITH ADMIN OPTION", userName.Normalized()),
		); err != nil {
			return err
		}
	}
	return nil
}
