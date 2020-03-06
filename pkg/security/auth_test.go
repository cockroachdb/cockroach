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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Construct a fake tls.ConnectionState object with one peer certificate
// for each 'commonNames', and one chain of length 'chainLengths[i]' for each 'chainLengths'.
func makeFakeTLSState(commonNames []string, chainLengths []int) *tls.ConnectionState {
	tls := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{},
		VerifiedChains:   [][]*x509.Certificate{},
	}
	for _, name := range commonNames {
		tls.PeerCertificates = append(tls.PeerCertificates, &x509.Certificate{Subject: pkix.Name{CommonName: name}})
	}
	for i, length := range chainLengths {
		chain := []*x509.Certificate{}
		for j := 0; j < length; j++ {
			name := fmt.Sprintf("chain%d:%d", i, j)
			chain = append(chain, &x509.Certificate{Subject: pkix.Name{CommonName: name}})
		}
		tls.VerifiedChains = append(tls.VerifiedChains, chain)
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
	if _, err := security.GetCertificateUsers(makeFakeTLSState(nil, nil)); err == nil {
		t.Error("unexpected success")
	}

	// Good request: single certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState([]string{"foo"}, []int{2})); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Request with multiple certs, but only one chain (eg: origin certs are client and CA).
	if names, err := security.GetCertificateUsers(makeFakeTLSState([]string{"foo", "CA"}, []int{2})); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Always use the first certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState([]string{"foo", "bar"}, []int{2, 1})); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}
}

func TestSetCertPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	testCases := []struct {
		user     string
		val      string
		expected string
	}{
		{"foo", "", "foo"},
		{"foo", "foo:foo", "foo"},
		{"foo", "foo:bar", "bar"},
		{"foo", "bar:bar", "foo"},
		{"foo", "foo:bar,foo:blah", "blah"},
		{"node.cockroachlabs.com", "node.cockroachlabs.com:node", "node"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			vals := strings.Split(c.val, ",")
			if err := security.SetCertPrincipalMap(vals); err != nil {
				t.Fatal(err)
			}
			names, err := security.GetCertificateUsers(makeFakeTLSState([]string{c.user}, []int{2}))
			if err != nil {
				t.Fatal(err)
			}
			require.EqualValues(t, names, []string{c.expected})
		})
	}
}

func TestAuthenticationHook(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		insecure           bool
		tls                *tls.ConnectionState
		username           string
		buildHookSuccess   bool
		publicHookSuccess  bool
		privateHookSuccess bool
	}{
		// Insecure mode, empty username.
		{true, nil, "", true, false, false},
		// Insecure mode, non-empty username.
		{true, nil, "foo", true, true, false},
		// Secure mode, no TLS state.
		{false, nil, "", false, false, false},
		// Secure mode, bad user.
		{false, makeFakeTLSState([]string{"foo"}, []int{1}), "node", true, false, false},
		// Secure mode, node user.
		{false, makeFakeTLSState([]string{security.NodeUser}, []int{1}), "node", true, true, true},
		// Secure mode, root user.
		{false, makeFakeTLSState([]string{security.RootUser}, []int{1}), "node", true, false, false},
	}

	for tcNum, tc := range testCases {
		hook, err := security.UserAuthCertHook(tc.insecure, tc.tls)
		if (err == nil) != tc.buildHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.buildHookSuccess, err)
		}
		if err != nil {
			continue
		}
		err = hook(tc.username, true /*public*/)
		if (err == nil) != tc.publicHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.publicHookSuccess, err)
		}
		err = hook(tc.username, false /*not public*/)
		if (err == nil) != tc.privateHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.privateHookSuccess, err)
		}
	}
}
