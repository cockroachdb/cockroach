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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Construct a fake tls.ConnectionState object. The spec is a semicolon
// separated list if peer certificate specifications. Each peer certificate
// specification can have an optional OU in parenthesis followed by
// a comma separated list of names where the first name is the
// CommonName and the remaining names are SubjectAlternateNames. For example,
// "foo" creates a single peer certificate with the CommonName "foo". The spec
// "foo,bar" creates a single peer certificate with the CommonName "foo" and a
// single SubjectAlternateName "bar". "(Tenants)foo,bar" creates a single
// tenant client certificate with OU=Tenants, CN=foo and subjectAlternativeName=bar
// Contrast that with "foo;bar" which creates two peer certificates with the
// CommonNames "foo" and "bar" respectively.
func makeFakeTLSState(spec string) *tls.ConnectionState {
	tls := &tls.ConnectionState{}
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
			peerCert.DNSNames = names[1:]
			tls.PeerCertificates = append(tls.PeerCertificates, peerCert)
		}
	}
	return tls
}

func TestGetCertificateUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Nil TLS state.
	if _, err := security.GetCertificateUsers(nil); err == nil {
		t.Error("unexpected success")
	}

	// No certificates.
	if _, err := security.GetCertificateUsers(makeFakeTLSState("")); err == nil {
		t.Error("unexpected success")
	}

	// Good request: single certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Request with multiple certs, but only one chain (eg: origin certs are client and CA).
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo;CA")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Always use the first certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo;bar")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Extract all of the principals from the first certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo,bar,blah;CA")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo", "bar", "blah"})
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
		{"foo,bar", "foo:blah", "blah,bar"},
		// First principal unmapped, second principal mapped.
		{"bar,foo", "foo:blah", "bar,blah"},
		// Both principals mapped.
		{"foo,bar", "foo:bar,bar:foo", "bar,foo"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			vals := strings.Split(c.val, ",")
			if err := security.SetCertPrincipalMap(vals); err != nil {
				t.Fatal(err)
			}
			names, err := security.GetCertificateUsers(makeFakeTLSState(c.spec))
			if err != nil {
				t.Fatal(err)
			}
			require.EqualValues(t, strings.Join(names, ","), c.expected)
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
	}{
		// Insecure mode, empty username.
		{true, "", security.SQLUsername{}, "", true, false, false},
		// Insecure mode, non-empty username.
		{true, "", fooUser, "", true, true, false},
		// Secure mode, no TLS state.
		{false, "", security.SQLUsername{}, "", false, false, false},
		// Secure mode, bad user.
		{false, "foo", security.NodeUserName(), "", true, false, false},
		// Secure mode, node user.
		{false, security.NodeUser, security.NodeUserName(), "", true, true, true},
		// Secure mode, root user.
		{false, security.RootUser, security.NodeUserName(), "", true, false, false},
		// Secure mode, tenant cert, foo user.
		{false, "(Tenants)foo", fooUser, "", true, false, false},
		// Secure mode, multiple cert principals.
		{false, "foo,bar", fooUser, "", true, true, false},
		{false, "foo,bar", barUser, "", true, true, false},
		// Secure mode, principal map.
		{false, "foo,bar", blahUser, "foo:blah", true, true, false},
		{false, "foo,bar", blahUser, "bar:blah", true, true, false},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := security.SetCertPrincipalMap(strings.Split(tc.principalMap, ","))
			if err != nil {
				t.Fatal(err)
			}
			hook, err := security.UserAuthCertHook(tc.insecure, makeFakeTLSState(tc.tlsSpec))
			if (err == nil) != tc.buildHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.buildHookSuccess, err)
			}
			if err != nil {
				return
			}
			_, err = hook(ctx, tc.username, true /* clientConnection */)
			if (err == nil) != tc.publicHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.publicHookSuccess, err)
			}
			_, err = hook(ctx, tc.username, false /* clientConnection */)
			if (err == nil) != tc.privateHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.privateHookSuccess, err)
			}
		})
	}
}
