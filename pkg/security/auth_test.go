// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Construct a fake tls.ConnectionState object. The spec is a semicolon
// separated list of peer certificate specifications. Each peer certificate
// specification can have an optional OU in parenthesis followed by
// a comma separated list of names where the first name is the
// CommonName and the remaining names are SubjectAlternateNames.
// The SubjectAlternateNames can go under DNSNames or URIs. To distinguish
// the two, prefix the SAN with the type dns: or uri:. For example,
// "foo" creates a single peer certificate with the CommonName "foo". The spec
// "foo,dns:bar,dns:blah" creates a single peer certificate with the CommonName "foo" and a
// DNSNames "bar" and "blah". "(Tenants)foo,dns:bar" creates a single
// tenant client certificate with OU=Tenants, CN=foo and DNSName=bar.
// A spec with "foo,dns:bar,uri:crdb://tenant/123" creates a single peer certificate
// with CommonName foo, DNSName bar and URI set to crdb://tenant/123.
// Contrast that with "foo;bar" which creates two peer certificates with the
// CommonNames "foo" and "bar" respectively.
func makeFakeTLSState(t *testing.T, spec string) *tls.ConnectionState {
	tls := &tls.ConnectionState{}
	uriPrefix := "uri:"
	dnsPrefix := "dns:"
	if spec != "" {
		for _, peerSpec := range strings.Split(spec, ";") {
			var ou []string
			if strings.HasPrefix(peerSpec, "(") {
				ouAndRest := strings.Split(peerSpec[1:], ")")
				ou = ouAndRest[:1]
				peerSpec = ouAndRest[1]
			}
			names := strings.Split(peerSpec, ",")
			if len(names) == 0 {
				continue
			}
			peerCert := &x509.Certificate{}
			peerCert.Subject = pkix.Name{
				CommonName:         names[0],
				OrganizationalUnit: ou,
			}
			for i := 1; i < len(names); i++ {
				if strings.HasPrefix(names[i], dnsPrefix) {
					peerCert.DNSNames = append(peerCert.DNSNames, strings.TrimPrefix(names[i], dnsPrefix))
				} else if strings.HasPrefix(names[i], uriPrefix) {
					rawURI := strings.TrimPrefix(names[i], uriPrefix)
					url, err := url.Parse(rawURI)
					if err != nil {
						t.Fatalf("unable to create tls spec due to invalid URI %s", rawURI)
					}
					peerCert.URIs = append(peerCert.URIs, url)
				} else {
					t.Fatalf("subject altername names are expected to have uri: or dns: prefix")
				}
			}
			tls.PeerCertificates = append(tls.PeerCertificates, peerCert)
		}
	}
	return tls
}

func TestGetCertificateUserScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Nil TLS state.
	if _, err := security.GetCertificateUserScope(nil); err == nil {
		t.Error("unexpected success")
	}

	// No certificates.
	if _, err := security.GetCertificateUserScope(makeFakeTLSState(t, "")); err == nil {
		t.Error("unexpected success")
	}

	// Good request: single certificate.
	if userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, "foo")); err != nil {
		t.Error(err)
	} else {
		require.Equal(t, 1, len(userScopes))
		require.Equal(t, "foo", userScopes[0].Username)
		require.True(t, userScopes[0].Global)
	}

	// Request with multiple certs, but only one chain (eg: origin certs are client and CA).
	if userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, "foo;CA")); err != nil {
		t.Error(err)
	} else {
		require.Equal(t, 1, len(userScopes))
		require.Equal(t, "foo", userScopes[0].Username)
		require.True(t, userScopes[0].Global)
	}

	// Always use the first certificate.
	if userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, "foo;bar")); err != nil {
		t.Error(err)
	} else {
		require.Equal(t, 1, len(userScopes))
		require.Equal(t, "foo", userScopes[0].Username)
		require.True(t, userScopes[0].Global)
	}

	// Extract all of the principals from the first certificate.
	if userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, "foo,dns:bar,dns:blah;CA")); err != nil {
		t.Error(err)
	} else {
		require.Equal(t, 3, len(userScopes))
		require.True(t, userScopes[0].Global)
	}
	if userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, "foo,uri:crdb://tenant/123/user/foo;CA")); err != nil {
		t.Error(err)
	} else {
		require.Equal(t, 1, len(userScopes))
		require.Equal(t, "foo", userScopes[0].Username)
		require.Equal(t, roachpb.MakeTenantID(123), userScopes[0].TenantID)
		require.False(t, userScopes[0].Global)
	}
}

func TestSetCertPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	testCases := []struct {
		vals     []string
		expected string
	}{
		{[]string{}, ""},
		{[]string{"foo"}, "invalid <cert-principal>:<db-principal> mapping:"},
		{[]string{"foo:bar"}, ""},
		{[]string{"foo:bar", "blah:blah"}, ""},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			err := security.SetCertPrincipalMap(c.vals)
			if !testutils.IsError(err, c.expected) {
				t.Fatalf("expected %q, but found %v", c.expected, err)
			}
		})
	}
}

func TestGetCertificateUsersMapped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	testCases := []struct {
		spec     string
		val      string
		expected string
	}{
		// No mapping present.
		{"foo", "", "foo"},
		// The basic mapping case.
		{"foo", "foo:bar", "bar"},
		// Identity mapping.
		{"foo", "foo:foo", "foo"},
		// Mapping does not apply to cert principals.
		{"foo", "bar:bar", "foo"},
		// The last mapping for a principal takes precedence.
		{"foo", "foo:bar,foo:blah", "blah"},
		// First principal mapped, second principal unmapped.
		{"foo,dns:bar", "foo:blah", "blah,bar"},
		// First principal unmapped, second principal mapped.
		{"bar,dns:foo", "foo:blah", "bar,blah"},
		// Both principals mapped.
		{"foo,dns:bar", "foo:bar,bar:foo", "bar,foo"},
		// Verify desired string splits.
		{"foo:has:colon", "foo:has:colon:bar", "bar"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			vals := strings.Split(c.val, ",")
			if err := security.SetCertPrincipalMap(vals); err != nil {
				t.Fatal(err)
			}
			userScopes, err := security.GetCertificateUserScope(makeFakeTLSState(t, c.spec))
			if err != nil {
				t.Fatal(err)
			}
			var names string
			for _, scope := range userScopes {
				if len(names) == 0 {
					names = scope.Username
				} else {
					names += "," + scope.Username
				}
			}
			require.EqualValues(t, names, c.expected)
		})
	}
}

func TestAuthenticationHook(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	blahUser := security.MakeSQLUsernameFromPreNormalizedString("blah")

	testCases := []struct {
		insecure           bool
		tlsSpec            string
		username           security.SQLUsername
		principalMap       string
		buildHookSuccess   bool
		publicHookSuccess  bool
		privateHookSuccess bool
		tenantID           roachpb.TenantID
	}{
		// Insecure mode, empty username.
		{true, "", security.SQLUsername{}, "", true, false, false, roachpb.SystemTenantID},
		// Insecure mode, non-empty username.
		{true, "", fooUser, "", true, true, false, roachpb.SystemTenantID},
		// Secure mode, no TLS state.
		{false, "", security.SQLUsername{}, "", false, false, false, roachpb.SystemTenantID},
		// Secure mode, bad user.
		{false, "foo", security.NodeUserName(), "", true, false, false, roachpb.SystemTenantID},
		// Secure mode, node user.
		{false, security.NodeUser, security.NodeUserName(), "", true, true, true, roachpb.SystemTenantID},
		// Secure mode, node cert and unrelated user.
		{false, security.NodeUser, fooUser, "", true, false, false, roachpb.SystemTenantID},
		// Secure mode, root user.
		{false, security.RootUser, security.NodeUserName(), "", true, false, false, roachpb.SystemTenantID},
		// Secure mode, tenant cert, foo user.
		{false, "(Tenants)foo", fooUser, "", true, false, false, roachpb.SystemTenantID},
		// Secure mode, multiple cert principals.
		{false, "foo,dns:bar", fooUser, "", true, true, false, roachpb.SystemTenantID},
		{false, "foo,dns:bar", barUser, "", true, true, false, roachpb.SystemTenantID},
		// Secure mode, principal map.
		{false, "foo,dns:bar", blahUser, "foo:blah", true, true, false, roachpb.SystemTenantID},
		{false, "foo,dns:bar", blahUser, "bar:blah", true, true, false, roachpb.SystemTenantID},
		{false, "foo,uri:crdb://tenant/123/user/foo", fooUser, "", true, true, false, roachpb.MakeTenantID(123)},
		{false, "foo,uri:crdb://tenant/123/user/foo", fooUser, "", true, false, false, roachpb.SystemTenantID},
		{false, "foo", fooUser, "", true, true, false, roachpb.MakeTenantID(123)},
		{false, "foo,uri:crdb://tenant/1/user/foo", fooUser, "", true, false, false, roachpb.MakeTenantID(123)},
		{false, "foo,uri:crdb://tenant/123/user/foo", blahUser, "", true, false, false, roachpb.MakeTenantID(123)},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := security.SetCertPrincipalMap(strings.Split(tc.principalMap, ","))
			if err != nil {
				t.Fatal(err)
			}
			hook, err := security.UserAuthCertHook(tc.insecure, makeFakeTLSState(t, tc.tlsSpec), tc.tenantID)
			if (err == nil) != tc.buildHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.buildHookSuccess, err)
			}
			if err != nil {
				return
			}
			err = hook(ctx, tc.username, true /* clientConnection */)
			if (err == nil) != tc.publicHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.publicHookSuccess, err)
			}
			err = hook(ctx, tc.username, false /* clientConnection */)
			if (err == nil) != tc.privateHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.privateHookSuccess, err)
			}
		})
	}
}
