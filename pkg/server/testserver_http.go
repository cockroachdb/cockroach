// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// httpTestServer is embedded in testServer / TenantServer to
// provide the HTTP API subset of ApplicationLayerInterface.
type httpTestServer struct {
	t struct {
		// We need a sub-struct to avoid ambiguous overlap with the fields
		// of *Server, which are also embedded in testServer.
		authentication authserver.Server
		sqlServer      *SQLServer
		tenantName     roachpb.TenantName
		admin          *adminServer
		status         *statusServer
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
func (ts *httpTestServer) AdminURL() *serverutils.TestURL {
	u := ts.t.sqlServer.cfg.Config.AdminURL()
	if ts.t.tenantName != "" {
		q := u.Query()
		q.Add(ClusterNameParamInQueryURL, string(ts.t.tenantName))
		u.RawQuery = q.Encode()
	}
	return &serverutils.TestURL{URL: u}
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
	if util.RaceEnabled {
		client.Timeout = 30 * time.Second
	}
	return client, nil
}

// GetAdminHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAdminHTTPClient() (http.Client, error) {
	httpClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(
		apiconstants.TestingUserName(), true, serverutils.SingleTenantSession,
	)
	if util.RaceEnabled {
		httpClient.Timeout = 30 * time.Second
	}
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *httpTestServer) GetAuthenticatedHTTPClient(
	isAdmin bool, session serverutils.SessionType,
) (http.Client, error) {
	authUser := apiconstants.TestingUserName()
	if !isAdmin {
		authUser = apiconstants.TestingUserNameNoAdmin()
	}
	httpClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(authUser, isAdmin, session)
	if util.RaceEnabled {
		httpClient.Timeout = 30 * time.Second
	}
	return httpClient, err
}

// GetAuthSession implements the TestServerInterface.
func (ts *httpTestServer) GetAuthSession(isAdmin bool) (*serverpb.SessionCookie, error) {
	authUser := apiconstants.TestingUserName()
	if !isAdmin {
		authUser = apiconstants.TestingUserNameNoAdmin()
	}
	_, cookie, err := ts.GetAuthenticatedHTTPClientAndCookie(authUser, isAdmin, serverutils.SingleTenantSession)
	return cookie, err
}

// GetAuthenticatedHTTPClientAndCookie returns an authenticated HTTP
// client and the session cookie for the client.
func (ts *httpTestServer) GetAuthenticatedHTTPClientAndCookie(
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
			if err := ts.CreateAuthUser(authUser, isAdmin); err != nil {
				return err
			}

			id, secret, err := ts.t.authentication.NewAuthSession(context.TODO(), authUser)
			if err != nil {
				return err
			}
			rawCookie := &serverpb.SessionCookie{
				ID:     id,
				Secret: secret,
			}
			// Encode a session cookie and store it in a cookie jar.
			cookie, err := authserver.EncodeSessionCookie(rawCookie, false /* forHTTPSOnly */)
			if err != nil {
				return err
			}
			cookieJar, err := cookiejar.New(nil)
			if err != nil {
				return err
			}
			url, err := url.Parse(ts.t.sqlServer.cfg.Config.AdminURL().String())
			if err != nil {
				return err
			}
			if session == serverutils.MultiTenantSession {
				cookie.Name = authserver.SessionCookieName
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

// CreateAuthUser is exported for use in tests.
func (ts *httpTestServer) CreateAuthUser(userName username.SQLUsername, isAdmin bool) error {
	if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
		"create-auth-user", nil,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("CREATE USER %s", userName.Normalized()),
	); err != nil {
		return err
	}
	if isAdmin {
		if _, err := ts.t.sqlServer.internalExecutor.ExecEx(context.TODO(),
			"grant-admin", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf("GRANT admin TO %s WITH ADMIN OPTION", userName.Normalized()),
		); err != nil {
			return err
		}
	}
	return nil
}
