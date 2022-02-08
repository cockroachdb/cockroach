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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

// AdminURL implements TestServerInterface.
func (ts *httpTestServer) AdminURL() string {
	return ts.t.sqlServer.execCfg.RPCContext.Config.AdminURL().String()
}

// GetHTTPClient implements TestServerInterface.
func (ts *httpTestServer) GetHTTPClient() (http.Client, error) {
	return ts.t.sqlServer.execCfg.RPCContext.GetHTTPClient()
}

// GetAdminAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAdminAuthenticatedHTTPClient() (http.Client, error) {
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authenticatedUserName(), true)
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error) {
	authUser := authenticatedUserName()
	if !isAdmin {
		authUser = authenticatedUserNameNoAdmin()
	}
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authUser, isAdmin)
	return httpClient, err
}

func (ts *httpTestServer) getAuthenticatedHTTPClientAndCookie(
	authUser security.SQLUsername, isAdmin bool,
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
			authClient.httpClient.Transport = &v2AuthDecorator{
				RoundTripper: authClient.httpClient.Transport,
				session:      base64.StdEncoding.EncodeToString(rawCookieBytes),
			}
			authClient.httpClient.Jar = cookieJar
			authClient.cookie = rawCookie
			return nil
		}()
	})

	return authClient.httpClient, authClient.cookie, authClient.err
}

func (ts *httpTestServer) createAuthUser(userName security.SQLUsername, isAdmin bool) error {
	if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
		"create-auth-user", nil,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf("CREATE USER %s", userName.Normalized()),
	); err != nil {
		return err
	}
	if isAdmin {
		// We can't use the GRANT statement here because we don't want
		// to rely on CCL code.
		if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
			"grant-admin", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"INSERT INTO system.role_members (role, member, \"isAdmin\") VALUES ('admin', $1, true)", userName.Normalized(),
		); err != nil {
			return err
		}
	}
	return nil
}
