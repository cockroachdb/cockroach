// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver_test

import (
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCookieSameSite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name   string
		cookie *http.Cookie
	}{
		{name: "session cookie", cookie: authserver.CreateSessionCookie("v", true)},
		{name: "empty session cookie", cookie: authserver.CreateEmptySessionCookieWithImmediateExpiry(true)},
		{name: "tenant select cookie", cookie: authserver.CreateTenantSelectCookie("v", true)},
		{name: "empty tenant select cookie", cookie: authserver.CreateEmptyTenantSelectCookieWithImmediateExpiry(true)},
		{name: "oidc cookie", cookie: authserver.CreateOIDCCookie("oidc", "v")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, http.SameSiteLaxMode, tc.cookie.SameSite)
		})
	}
}
