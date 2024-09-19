// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
)

// Construct a fake tls.ConnectionState object. The spec is a semicolon
// separated list of peer certificate specifications. Each peer certificate
// specification has a subject DN in parenthesis followed by a comma separated
// list of SubjectAlternateNames. The SubjectAlternateNames can go under
// DNSNames or URIs. To distinguish the two, prefix the SAN with the type dns:
// or uri:. For example, "(CN=foo)" creates a single peer certificate with the
// CommonName "foo". The spec "(CN=foo)dns:bar,dns:blah" creates a single peer
// certificate with the CommonName "foo" and a DNSNames "bar" and "blah".
// "(OU=Tenants,CN=foo)dns:bar" creates a single tenant client certificate with
// OU=Tenants, CN=foo and DNSName=bar. A spec with
// "(CN=foo)dns:bar,uri:crdb://tenant/123" creates a single peer certificate
// with CommonName foo, DNSName bar and URI set to crdb://tenant/123. Contrast
// that with "(CN=foo);(CN=bar)" which creates two peer certificates with the
// CommonNames "foo" and "bar" respectively. To create a certificate with full
// DN subject, required spec would be "(O=Cockroach,OU=Order Processing
// Team,CN=foo),dns:bar" which creates a certificate with O=Cockroach,OU=Order
// Processing Team,CN=foo, DNSName=bar.
func makeFakeTLSState(t *testing.T, spec string) *tls.ConnectionState {
	tls := &tls.ConnectionState{}
	uriPrefix := "uri:"
	dnsPrefix := "dns:"
	if spec != "" {
		for _, peerSpec := range strings.Split(spec, ";") {
			subjectDN := [][]string{}
			specSAN := ""
			if strings.HasPrefix(peerSpec, "(") {
				subjectDNAndRest := strings.Split(peerSpec[1:], ")")
				fieldsSubjectDN := strings.Split(subjectDNAndRest[0], ",")
				for _, field := range fieldsSubjectDN {
					fieldKeyAndValue := strings.Split(field, "=")
					subjectDN = append(subjectDN, []string{fieldKeyAndValue[0], fieldKeyAndValue[1]})
				}
				if len(subjectDNAndRest) == 2 {
					specSAN = subjectDNAndRest[1]
				}
			}
			peerCert := &x509.Certificate{}
			RDNSeq, err := generateRDNSequenceFromSpecMap(subjectDN)
			if err != nil {
				t.Fatalf("unable to generate RDN Sequence from subjectDN spec, err: %v", err)
			}
			peerCert.Subject.FillFromRDNSequence(&RDNSeq)
			peerCert.RawSubject, err = asn1.Marshal(RDNSeq)
			if err != nil {
				t.Fatalf("unable to marshal subject, err: %v", err)
			}

			if len(specSAN) != 0 {
				listSANs := strings.Split(specSAN, ",")
				for i := 0; i < len(listSANs); i++ {
					if strings.HasPrefix(listSANs[i], dnsPrefix) {
						peerCert.DNSNames = append(peerCert.DNSNames, strings.TrimPrefix(listSANs[i], dnsPrefix))
					} else if strings.HasPrefix(listSANs[i], uriPrefix) {
						rawURI := strings.TrimPrefix(listSANs[i], uriPrefix)
						uri, err := url.Parse(rawURI)
						if err != nil {
							t.Fatalf("unable to create tls spec due to invalid URI %s", rawURI)
						}
						peerCert.URIs = append(peerCert.URIs, uri)
					} else {
						t.Fatalf("subject altername names are expected to have uri: or dns: prefix")
					}
				}
			}
			tls.PeerCertificates = append(tls.PeerCertificates, peerCert)
		}
	}
	return tls
}

// generateRDNSequenceFromSpecMap takes a list subject DN fields and
// corresponding values. It generates pkix.RDNSequence for these fields. The
// returned  sequence could then used to generate cert.Subject and
// cert.RawSubject for creating a mock crypto/x509 certificate object.
func generateRDNSequenceFromSpecMap(
	subjectSpecMap [][]string,
) (RDNSeq pkix.RDNSequence, err error) {
	var (
		oidCountry            = []int{2, 5, 4, 6}
		oidOrganization       = []int{2, 5, 4, 10}
		oidOrganizationalUnit = []int{2, 5, 4, 11}
		oidCommonName         = []int{2, 5, 4, 3}
		oidLocality           = []int{2, 5, 4, 7}
		oidProvince           = []int{2, 5, 4, 8}
		oidStreetAddress      = []int{2, 5, 4, 9}
		oidUID                = []int{0, 9, 2342, 19200300, 100, 1, 1}
		oidDC                 = []int{0, 9, 2342, 19200300, 100, 1, 25}
	)

	for _, fieldAndValue := range subjectSpecMap {
		field := fieldAndValue[0]
		fieldValue := fieldAndValue[1]
		var attrTypeAndValue pkix.AttributeTypeAndValue
		switch field {
		case "CN":
			attrTypeAndValue.Type = oidCommonName
		case "L":
			attrTypeAndValue.Type = oidLocality
		case "ST":
			attrTypeAndValue.Type = oidProvince
		case "O":
			attrTypeAndValue.Type = oidOrganization
		case "OU":
			attrTypeAndValue.Type = oidOrganizationalUnit
		case "C":
			attrTypeAndValue.Type = oidCountry
		case "STREET":
			attrTypeAndValue.Type = oidStreetAddress
		case "DC":
			attrTypeAndValue.Type = oidDC
		case "UID":
			attrTypeAndValue.Type = oidUID
		default:
			return nil, fmt.Errorf("found unknown field value %q in spec map", field)
		}
		attrTypeAndValue.Value = fieldValue
		RDNSeq = append(RDNSeq, pkix.RelativeDistinguishedNameSET{
			attrTypeAndValue,
		})
	}

	return RDNSeq, nil
}

func TestGetCertificateUserScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("good request: single certificate", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.True(t, userScopes[0].Global)
		}
	})

	t.Run("request with multiple certs, but only one chain (eg: origin certs are client and CA)", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo);(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.True(t, userScopes[0].Global)
		}
	})

	t.Run("always use the first certificate", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo);(CN=bar)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.True(t, userScopes[0].Global)
		}
	})

	t.Run("extract all of the principals from the first certificate", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)dns:bar,dns:blah;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 3, len(userScopes))
			require.True(t, userScopes[0].Global)
		}
	})

	t.Run("extracts username, tenantID from tenant URI SAN", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:crdb://tenant/123/user/foo;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.Equal(t, roachpb.MustMakeTenantID(123), userScopes[0].TenantID)
			require.False(t, userScopes[0].Global)
		}
	})

	t.Run("extracts tenant URI SAN even when multiple URIs, where one URI is not of CRBD format", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:mycompany:sv:rootclient:dev:usw1,uri:crdb://tenant/123/user/foo;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.Equal(t, roachpb.MustMakeTenantID(123), userScopes[0].TenantID)
			require.False(t, userScopes[0].Global)
		}
	})

	t.Run("errors when tenant URI SAN is not of expected format, even if other URI SAN is provided", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:mycompany:sv:rootclient:dev:usw1,uri:crdb://tenant/bad/format/123;(CN=CA)")
		cert := state.PeerCertificates[0]
		userScopes, err := security.GetCertificateUserScope(cert)
		require.Nil(t, userScopes)
		require.ErrorContains(t, err, "invalid tenant URI SAN")
	})

	t.Run("falls back to global client cert when crdb URI SAN scheme is not followed", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=sanuri)uri:mycompany:sv:rootclient:dev:usw1;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "sanuri", userScopes[0].Username)
			require.True(t, userScopes[0].Global)
		}
	})

	t.Run("extracts username, tenantName from tenant-name URI SAN", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:crdb://tenant-name/tenant10/user/foo;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.Equal(t, roachpb.TenantName("tenant10"), userScopes[0].TenantName)
			require.False(t, userScopes[0].Global)
		}
	})

	t.Run("extracts username, tenantName from tenant-name URI SAN with URI scheme in upper case", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:CRDB://tenant-name/tenant10/user/foo;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 1, len(userScopes))
			require.Equal(t, "foo", userScopes[0].Username)
			require.Equal(t, roachpb.TenantName("tenant10"), userScopes[0].TenantName)
			require.False(t, userScopes[0].Global)
		}
	})

	t.Run("extracts both tenant URI SAN and tenant name URI SAN when both are present", func(t *testing.T) {
		state := makeFakeTLSState(t, "(CN=foo)uri:crdb://tenant-name/tenant10/user/bar,uri:crdb://tenant/123/user/foo;(CN=CA)")
		cert := state.PeerCertificates[0]
		if userScopes, err := security.GetCertificateUserScope(cert); err != nil {
			t.Error(err)
		} else {
			require.Equal(t, 2, len(userScopes))

			require.Equal(t, "bar", userScopes[0].Username)
			require.Equal(t, roachpb.TenantName("tenant10"), userScopes[0].TenantName)
			require.False(t, userScopes[0].Global)

			require.Equal(t, "foo", userScopes[1].Username)
			require.Equal(t, roachpb.MustMakeTenantID(123), userScopes[1].TenantID)
			require.False(t, userScopes[1].Global)
		}
	})
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
		{"(CN=foo)", "", "foo"},
		// The basic mapping case.
		{"(CN=foo)", "foo:bar", "bar"},
		// Identity mapping.
		{"(CN=foo)", "foo:foo", "foo"},
		// Mapping does not apply to cert principals.
		{"(CN=foo)", "bar:bar", "foo"},
		// The last mapping for a principal takes precedence.
		{"(CN=foo)", "foo:bar,foo:blah", "blah"},
		// First principal mapped, second principal unmapped.
		{"(CN=foo)dns:bar", "foo:blah", "blah,bar"},
		// First principal unmapped, second principal mapped.
		{"(CN=bar)dns:foo", "foo:blah", "bar,blah"},
		// Both principals mapped.
		{"(CN=foo)dns:bar", "foo:bar,bar:foo", "bar,foo"},
		// Verify desired string splits.
		{"(CN=foo:has:colon)", "foo:has:colon:bar", "bar"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			vals := strings.Split(c.val, ",")
			if err := security.SetCertPrincipalMap(vals); err != nil {
				t.Fatal(err)
			}
			state := makeFakeTLSState(t, c.spec)
			cert := state.PeerCertificates[0]
			userScopes, err := security.GetCertificateUserScope(cert)
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

	fooUser := "foo"
	barUser := "bar"
	blahUser := "blah"
	subjectDNString := "O=Cockroach,OU=Order Processing Team,UID=b8b40653-7f74-4f14-8a61-59f7f3b18184,CN=foo"
	fieldMismatchSubjectDNString := "O=Cockroach,OU=Marketing Team,UID=b8b40653-7f74-4f14-8a61-59f7f3b18184,CN=foo"
	subsetSubjectDNString := "O=Cockroach,OU=Order Processing Team,CN=foo"
	fieldMismatchOnlyOnCommonNameString := "O=Cockroach,OU=Order Processing Team,UID=b8b40653-7f74-4f14-8a61-59f7f3b18184,CN=bar"
	rootDNString := "O=Cockroach,OU=Order Processing Team,UID=b8b40653-7f74-4f14-8a61-59f7f3b18184,CN=root"
	nodeDNString := "O=Cockroach,OU=Order Processing Team,UID=b8b40653-7f74-4f14-8a61-59f7f3b18184,CN=node"

	testCases := []struct {
		insecure                   bool
		tlsSpec                    string
		username                   string
		distinguishedNameString    string
		principalMap               string
		buildHookSuccess           bool
		publicHookSuccess          bool
		privateHookSuccess         bool
		tenantID                   roachpb.TenantID
		isSubjectRoleOptionOrDNSet bool
		subjectRequired            bool
		expectedErr                string
	}{
		// Insecure mode, empty username.
		{true, "", username.EmptyRole, "", "", true, false, false, roachpb.SystemTenantID, false, false, `user is missing`},
		// Insecure mode, non-empty username.
		{true, "", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, `user "foo" is not allowed`},
		// Secure mode, no TLS state.
		{false, "", username.EmptyRole, "", "", false, false, false, roachpb.SystemTenantID, false, false, `no client certificates in request`},
		// Secure mode, bad user.
		{false, "(CN=foo)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, `certificate authentication failed for user "node"`},
		// Secure mode, node user.
		{false, "(CN=node)", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, ``},
		// Secure mode, node cert and unrelated user.
		{false, "(CN=node)", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, `certificate authentication failed for user "foo"`},
		// Secure mode, root user.
		{false, "(CN=root)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, `certificate authentication failed for user "node"`},
		// Secure mode, tenant cert, foo user.
		{false, "(OU=Tenants,CN=foo)", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, `using tenant client certificate as user certificate is not allowed`},
		// Secure mode, multiple cert principals.
		{false, "(CN=foo)dns:bar", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, `user "foo" is not allowed`},
		{false, "(CN=foo)dns:bar", barUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, `user "bar" is not allowed`},
		// Secure mode, principal map.
		{false, "(CN=foo)dns:bar", blahUser, "", "foo:blah", true, true, false, roachpb.SystemTenantID, false, false, `user "blah" is not allowed`},
		{false, "(CN=foo)dns:bar", blahUser, "", "bar:blah", true, true, false, roachpb.SystemTenantID, false, false, `user "blah" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", fooUser, "", "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, `user "foo" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, `certificate authentication failed for user "foo"`},
		{false, "(CN=foo)", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, `user "foo" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/1/user/foo", fooUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, false, `certificate authentication failed for user "foo"`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", blahUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, false, `certificate authentication failed for user "blah"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option not set.
		{false, "(" + subjectDNString + ")", fooUser, "", "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, `user "foo" is not allowed`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set matching TLS cert subject.
		{false, "(" + subjectDNString + ")", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, `user "foo" is not allowed`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN empty.
		{false, "(CN=foo)", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN mismatches on OU field.
		{false, "(" + fieldMismatchSubjectDNString + ")", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN subset of role subject DN.
		{false, "(" + subsetSubjectDNString + ")", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set mismatching TLS cert subject only on CN(required for
		// matching) having DNS as foo.
		{false, "(" + fieldMismatchOnlyOnCommonNameString + ")dns:foo", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, `certificate authentication failed for user "foo"`},
		{false, "(" + rootDNString + ")", username.RootUser, rootDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, `user "root" is not allowed`},
		{false, "(" + nodeDNString + ")", username.NodeUser, nodeDNString, "", true, true, true, roachpb.MustMakeTenantID(123),
			true, false, ""},
		// tls cert dn matching root dn set, where CN != root
		{false, "(" + subjectDNString + ")", username.RootUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, `user "root" is not allowed`},
		{false, "(" + subjectDNString + ")", fooUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, true, `user "foo" does not have a distinguished name set which subject_required cluster setting mandates`},
		{false, "(" + subjectDNString + ")", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, true, `user "foo" is not allowed`},
	}

	ctx := context.Background()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: tls:%s user:%v tenant:%v", i, tc.tlsSpec, tc.username, tc.tenantID), func(t *testing.T) {
			defer func() {
				security.UnsetRootSubject()
				security.UnsetNodeSubject()
			}()
			err := security.SetCertPrincipalMap(strings.Split(tc.principalMap, ","))
			if err != nil {
				t.Fatal(err)
			}

			var roleSubject *ldap.DN
			if tc.isSubjectRoleOptionOrDNSet {
				switch tc.username {
				case username.RootUser:
					err = security.SetRootSubject(tc.distinguishedNameString)
					if err != nil {
						t.Fatalf("could not set root subject DN, err: %v", err)
					}
				case username.NodeUser:
					err = security.SetNodeSubject(tc.distinguishedNameString)
					if err != nil {
						t.Fatalf("could not set node subject DN, err: %v", err)
					}
				default:
					roleSubject, err = distinguishedname.ParseDN(tc.distinguishedNameString)
					if err != nil {
						t.Fatalf("could not set role subject, err: %v", err)
					}
				}
			}

			hook, err := security.UserAuthCertHook(
				tc.insecure,
				makeFakeTLSState(t, tc.tlsSpec),
				tc.tenantID,
				nil, /* certManager */
				roleSubject,
				tc.subjectRequired,
			)
			if (err == nil) != tc.buildHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.buildHookSuccess, err)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			err = hook(ctx, tc.username, true /* clientConnection */)
			if (err == nil) != tc.publicHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.publicHookSuccess, err)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			err = hook(ctx, tc.username, false /* clientConnection */)
			if (err == nil) != tc.privateHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.privateHookSuccess, err)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
		})
	}
}
