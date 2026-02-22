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

func TestSetSANsWithString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		security.UnsetRootSAN()
		security.UnsetNodeSAN()
	}()

	testCases := []struct {
		name        string
		input       string
		expectError string
		expectedLen int
		expected    []string
	}{
		{
			name:        "empty string clears SANs",
			input:       "",
			expectError: "",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "single DNS SAN",
			input:       "DNS=example.com",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:DNS:example.com"},
		},
		{
			name:        "single URI SAN",
			input:       "URI=spiffe://example.org/service",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:URI:spiffe://example.org/service"},
		},
		{
			name:        "multiple SANs",
			input:       "DNS=example.com,URI=spiffe://example.org/service",
			expectError: "",
			expectedLen: 2,
			expected:    []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org/service"},
		},
		{
			name:        "multiple SANs with spaces",
			input:       "DNS=example.com, URI=spiffe://example.org/service , DNS=*.example.com",
			expectError: "",
			expectedLen: 3,
			expected:    []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org/service", "SAN:DNS:*.example.com"},
		},
		{
			name:        "attribute type with spaces",
			input:       " DNS = example.com ",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:DNS:example.com"},
		},
		{
			name:        "lowercase attribute type gets uppercased",
			input:       "dns=example.com",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:DNS:example.com"},
		},
		{
			name:        "mixed case attribute type gets uppercased",
			input:       "DnS=example.com,uRi=spiffe://example.org",
			expectError: "",
			expectedLen: 2,
			expected:    []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org"},
		},
		{
			name:        "entry without equals sign",
			input:       "DNS example.com",
			expectError: "invalid SAN entry format: \"DNS example.com\" (expected format: attribute-type=attribute-value)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "empty attribute type",
			input:       "=example.com",
			expectError: "invalid SAN entry format: \"=example.com\" (attribute type cannot be empty)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "empty attribute value",
			input:       "DNS=",
			expectError: "invalid SAN entry format: \"DNS=\" (attribute value cannot be empty)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "attribute type only spaces",
			input:       "   =example.com",
			expectError: "invalid SAN entry format: \"=example.com\" (attribute type cannot be empty)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "attribute value only spaces",
			input:       "DNS=   ",
			expectError: "invalid SAN entry format: \"DNS=\" (attribute value cannot be empty)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "whitespace-only entries are skipped",
			input:       "DNS=example.com,  ,URI=spiffe://example.org",
			expectError: "",
			expectedLen: 2,
			expected:    []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org"},
		},
		{
			name:        "all whitespace entries",
			input:       " , , ",
			expectError: "invalid SAN string: \" , , \" (no valid entries found)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "multiple equals signs uses first",
			input:       "DNS=example.com=extra",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:DNS:example.com=extra"},
		},
		{
			name:        "IP address SAN",
			input:       "IP=192.168.1.1",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:IP:192.168.1.1"},
		},
		{
			name:        "IP address SAN lowercase",
			input:       "ip=192.168.1.1",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:IP:192.168.1.1"},
		},
		{
			name:        "multiple allowed SAN types",
			input:       "DNS=example.com,URI=spiffe://example.org,IP=192.168.1.1",
			expectError: "",
			expectedLen: 3,
			expected:    []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org", "SAN:IP:192.168.1.1"},
		},
		{
			name:        "invalid attribute type - CUSTOM",
			input:       "CUSTOM=value",
			expectError: "invalid SAN attribute type: \"CUSTOM\" (only DNS, URI, and IP are allowed)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "invalid attribute type - EMAIL",
			input:       "EMAIL=user@example.com",
			expectError: "invalid SAN attribute type: \"EMAIL\" (only DNS, URI, and IP are allowed)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "invalid attribute type mixed with valid",
			input:       "DNS=example.com,EMAIL=user@example.com",
			expectError: "invalid SAN attribute type: \"EMAIL\" (only DNS, URI, and IP are allowed)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "value with special characters",
			input:       "URI=https://example.com:8080/path?query=value&foo=bar",
			expectError: "",
			expectedLen: 1,
			expected:    []string{"SAN:URI:https://example.com:8080/path?query=value&foo=bar"},
		},
		{
			name:        "first entry invalid",
			input:       "invalid,DNS=example.com",
			expectError: "invalid SAN entry format: \"invalid\" (expected format: attribute-type=attribute-value)",
			expectedLen: 0,
			expected:    nil,
		},
		{
			name:        "second entry invalid",
			input:       "DNS=example.com,invalid",
			expectError: "invalid SAN entry format: \"invalid\" (expected format: attribute-type=attribute-value)",
			expectedLen: 0,
			expected:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with SetRootSAN
			err := security.SetRootSAN(tc.input)
			if tc.expectError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			} else {
				require.NoError(t, err)
				rootSANs := security.GetRootSANs()
				require.Equal(t, tc.expectedLen, len(rootSANs))
				require.Equal(t, tc.expected, rootSANs)
			}

			// Test with SetNodeSAN
			err = security.SetNodeSAN(tc.input)
			if tc.expectError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			} else {
				require.NoError(t, err)
				nodeSANs := security.GetNodeSANs()
				require.Equal(t, tc.expectedLen, len(nodeSANs))
				require.Equal(t, tc.expected, nodeSANs)
			}

			// Clear for next test
			security.UnsetRootSAN()
			security.UnsetNodeSAN()
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
		disallowRootLogin          bool
		allowDebugUser             bool
		clientCertSANRequired      bool
		rootSANsToSet              string
		nodeSANsToSet              string
		expectedErr                string
	}{
		// Insecure mode, empty username.
		{true, "", username.EmptyRole, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user is missing`},
		// Insecure mode, non-empty username.
		{true, "", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user "foo" is not allowed`},
		// Secure mode, no TLS state.
		{false, "", username.EmptyRole, "", "", false, false, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `no client certificates in request`},
		// Secure mode, bad user.
		{false, "(CN=foo)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, false, false, false, "", "", `certificate authentication failed for user "node"`},
		// Secure mode, node user.
		{false, "(CN=node)", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, false, false, "", "", ``},
		// Secure mode, node cert and unrelated user.
		{false, "(CN=node)", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		// Secure mode, root user.
		{false, "(CN=root)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, false, false, false, "", "", `certificate authentication failed for user "node"`},
		// Secure mode, tenant cert, foo user.
		{false, "(OU=Tenants,CN=foo)", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, false, false, false, "", "", `using tenant client certificate as user certificate is not allowed`},
		// Secure mode, multiple cert principals.
		{false, "(CN=foo)dns:bar", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user "foo" is not allowed`},
		{false, "(CN=foo)dns:bar", barUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user "bar" is not allowed`},
		// Secure mode, principal map.
		{false, "(CN=foo)dns:bar", blahUser, "", "foo:blah", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user "blah" is not allowed`},
		{false, "(CN=foo)dns:bar", blahUser, "", "bar:blah", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `user "blah" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", fooUser, "", "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, false, false, false, "", "", `user "foo" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", fooUser, "", "", true, false, false, roachpb.SystemTenantID,
			false, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		{false, "(CN=foo)", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, false, false, false, "", "", `user "foo" is not allowed`},
		{false, "(CN=foo)uri:crdb://tenant/1/user/foo", fooUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		{false, "(CN=foo)uri:crdb://tenant/123/user/foo", blahUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, false, false, false, false, "", "", `certificate authentication failed for user "blah"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option not set.
		{false, "(" + subjectDNString + ")", fooUser, "", "", true, true, false, roachpb.MustMakeTenantID(123),
			false, false, false, false, false, "", "", `user "foo" is not allowed`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set matching TLS cert subject.
		{false, "(" + subjectDNString + ")", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `user "foo" is not allowed`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN empty.
		{false, "(CN=foo)", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN mismatches on OU field.
		{false, "(" + fieldMismatchSubjectDNString + ")", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set, TLS cert DN subset of role subject DN.
		{false, "(" + subsetSubjectDNString + ")", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		// Secure mode, client cert having full DN, foo user with subject role
		// option set mismatching TLS cert subject only on CN(required for
		// matching) having DNS as foo.
		{false, "(" + fieldMismatchOnlyOnCommonNameString + ")dns:foo", fooUser, subjectDNString, "", true, false, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `certificate authentication failed for user "foo"`},
		{false, "(" + rootDNString + ")", username.RootUser, rootDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `user "root" is not allowed`},
		{false, "(" + nodeDNString + ")", username.NodeUser, nodeDNString, "", true, true, true, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", ""},
		// tls cert dn matching root dn set, where CN != root
		{false, "(" + subjectDNString + ")", username.RootUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, false, false, false, false, "", "", `user "root" is not allowed`},
		{false, "(" + subjectDNString + ")", fooUser, "", "", true, false, false, roachpb.MustMakeTenantID(123),
			false, true, false, false, false, "", "", `user "foo" does not have a distinguished name set which subject_required cluster setting mandates`},
		{false, "(" + subjectDNString + ")", fooUser, subjectDNString, "", true, true, false, roachpb.MustMakeTenantID(123),
			true, true, false, false, false, "", "", `user "foo" is not allowed`},

		// Test cases for disallow root login functionality
		// Root user with disallow root login disabled - should succeed
		{false, "(CN=root)", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", ``},
		// Root user with disallow root login enabled - should fail
		{false, "(CN=root)", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, true, false, false, "", "", `certificate authentication failed for user "root"`},
		// Non-root user with disallow root login enabled - should succeed
		{false, "(CN=foo)", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, true, false, false, "", "", `user "foo" is not allowed`},
		// Node user with disallow root login enabled - should succeed
		{false, "(CN=node)", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, true, false, false, "", "", ``},
		// Root user with tenant-scoped certificate and disallow root login enabled - should fail
		{false, "(CN=root)uri:crdb://tenant/123/user/root", username.RootUser, "", "", true, false, false, roachpb.MustMakeTenantID(123), false, false, true, false, false, "", "", `certificate authentication failed for user "root"`},
		// Root user with DNS SAN and disallow root login enabled - should fail
		{false, "(CN=root)dns:root", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, true, false, false, "", "", `certificate authentication failed for user "root"`},

		// Test cases for allow debug user functionality
		// Debug user with allow debug user disabled (default) - should fail
		{false, "(CN=debug_user)", username.DebugUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `certificate authentication failed for user "debug_user"`},
		// Debug user with allow debug user enabled - should succeed
		{false, "(CN=debug_user)", username.DebugUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, true, false, "", "", `user "debug_user" is not allowed`},
		// Non-debug user with allow debug user enabled - should succeed (no impact)
		{false, "(CN=foo)", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, true, false, "", "", `user "foo" is not allowed`},
		// Node user with allow debug user enabled - should succeed (no impact)
		{false, "(CN=node)", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, true, false, "", "", ``},
		// Debug user with tenant-scoped certificate and allow debug user disabled - should fail
		{false, "(CN=debug_user)uri:crdb://tenant/123/user/debug_user", username.DebugUser, "", "", true, false, false, roachpb.MustMakeTenantID(123), false, false, false, false, false, "", "", `certificate authentication failed for user "debug_user"`},
		// Debug user with tenant-scoped certificate and allow debug user enabled - should succeed
		{false, "(CN=debug_user)uri:crdb://tenant/123/user/debug_user", username.DebugUser, "", "", true, true, false, roachpb.MustMakeTenantID(123), false, false, false, true, false, "", "", `user "debug_user" is not allowed`},
		// Debug user with DNS SAN and allow debug user disabled - should fail
		{false, "(CN=debug_user)dns:debug_user", username.DebugUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, false, "", "", `certificate authentication failed for user "debug_user"`},
		// Debug user with DNS SAN and allow debug user enabled - should succeed
		{false, "(CN=debug_user)dns:debug_user", username.DebugUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, true, false, "", "", `user "debug_user" is not allowed`},

		// SAN validation test cases with clientCertSANRequired=true
		// Root user SAN validation tests
		{false, "(CN=root)dns:root.example.com", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(CN=root)uri:spiffe://example.org/root", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "URI=spiffe://example.org/root", "", `user "root" is not allowed`},
		{false, "(CN=root)dns:wrong.example.com", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "", `certificate authentication failed for user "root"`},
		{false, "(CN=root)", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "", `certificate authentication failed for user "root"`},
		{false, "(" + rootDNString + ")dns:root.example.com", username.RootUser, rootDNString, "", true, true, false, roachpb.SystemTenantID, true, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(CN=root)dns:root.example.com", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(" + rootDNString + ")", username.RootUser, rootDNString, "", true, true, false, roachpb.SystemTenantID, true, false, false, false, true, "", "", `user "root" is not allowed`},
		{false, "(CN=root)", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "", `certificate authentication failed for user "root"`},
		{false, "(CN=root)", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com", `certificate authentication failed for user "root"`},

		// Node user SAN validation tests
		{false, "(CN=node)dns:node.example.com", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com", ``},
		{false, "(CN=node)uri:spiffe://example.org/node", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, false, true, "", "URI=spiffe://example.org/node", ``},
		{false, "(CN=node)dns:wrong.example.com", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com", `certificate authentication failed for user "node"`},
		{false, "(CN=node)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "", `certificate authentication failed for user "node"`},
		{false, "(" + nodeDNString + ")dns:node.example.com", username.NodeUser, nodeDNString, "", true, true, true, roachpb.SystemTenantID, true, false, false, false, true, "", "DNS=node.example.com", ``},
		{false, "(CN=node)dns:node.example.com", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com", ``},
		{false, "(" + nodeDNString + ")", username.NodeUser, nodeDNString, "", true, true, true, roachpb.SystemTenantID, true, false, false, false, true, "", "", ``},
		{false, "(CN=node)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "", `certificate authentication failed for user "node"`},
		{false, "(CN=node)", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "", `certificate authentication failed for user "node"`},

		// Regular user SAN validation tests (SAN validation doesn't apply to non-root/node users)
		{false, "(CN=foo)dns:random.example.com", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "DNS=node.example.com", `user "foo" is not allowed`},
		{false, "(CN=foo)", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "DNS=node.example.com", `user "foo" is not allowed`},
		{false, "(" + subjectDNString + ")dns:foo.example.com", fooUser, subjectDNString, "", true, true, false, roachpb.SystemTenantID, true, false, false, false, true, "", "", `user "foo" is not allowed`},
		{false, "(CN=foo)", fooUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "", "", `user "foo" is not allowed`},

		// Mixed scenarios with both subject DN and SAN (OR logic)
		{false, "(" + rootDNString + ")dns:root.example.com", username.RootUser, fieldMismatchSubjectDNString, "", true, true, false, roachpb.SystemTenantID, true, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(" + rootDNString + ")dns:wrong.example.com", username.RootUser, rootDNString, "", true, true, false, roachpb.SystemTenantID, true, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(" + nodeDNString + ")dns:node.example.com", username.NodeUser, fieldMismatchSubjectDNString, "", true, true, true, roachpb.SystemTenantID, true, false, false, false, true, "", "DNS=node.example.com", ``},
		{false, "(" + nodeDNString + ")dns:wrong.example.com", username.NodeUser, nodeDNString, "", true, true, true, roachpb.SystemTenantID, true, false, false, false, true, "", "DNS=node.example.com", ``},

		// Edge cases
		{false, "(CN=root)dns:root.example.com,dns:extra.example.com", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(CN=node)dns:node.example.com,dns:extra.example.com", username.NodeUser, "", "", true, true, true, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com", ``},
		{false, "(CN=root)dns:root.example.com", username.RootUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=root.example.com,DNS=extra.example.com", "", `certificate authentication failed for user "root"`},
		{false, "(CN=node)dns:node.example.com", username.NodeUser, "", "", true, false, false, roachpb.SystemTenantID, false, false, false, false, true, "", "DNS=node.example.com,DNS=extra.example.com", `certificate authentication failed for user "node"`},
		{false, "(CN=root)dns:root.example.com", username.RootUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, true, false, true, "DNS=root.example.com", "", `user "root" is not allowed`},
		{false, "(CN=debug_user)dns:debug.example.com", username.DebugUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, false, true, "DNS=debug.example.com", "", `user "debug_user" is not allowed`},
		{false, "(CN=debug_user)dns:debug.example.com", username.DebugUser, "", "", true, true, false, roachpb.SystemTenantID, false, false, false, true, true, "DNS=debug.example.com", "", `user "debug_user" is not allowed`},
	}

	ctx := context.Background()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: tls:%s user:%v tenant:%v", i, tc.tlsSpec, tc.username, tc.tenantID), func(t *testing.T) {
			defer func() {
				security.UnsetRootSubject()
				security.UnsetNodeSubject()
				security.UnsetRootSAN()
				security.UnsetNodeSAN()
				security.SetDisallowRootLogin(false)
				security.SetAllowDebugUser(false)
			}()
			err := security.SetCertPrincipalMap(strings.Split(tc.principalMap, ","))
			if err != nil {
				t.Fatal(err)
			}

			// Set the global flags for disallowing root login and allowing debug user
			security.SetDisallowRootLogin(tc.disallowRootLogin)
			security.SetAllowDebugUser(tc.allowDebugUser)

			// Configure root and node SANs if specified
			if tc.rootSANsToSet != "" {
				err = security.SetRootSAN(tc.rootSANsToSet)
				if err != nil {
					t.Fatalf("could not set root SANs, err: %v", err)
				}
			}
			if tc.nodeSANsToSet != "" {
				err = security.SetNodeSAN(tc.nodeSANsToSet)
				if err != nil {
					t.Fatalf("could not set node SANs, err: %v", err)
				}
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
				"",
				nil, /* certManager */
				roleSubject,
				tc.subjectRequired,
				tc.clientCertSANRequired,
			)
			if (err == nil) != tc.buildHookSuccess {
				t.Fatalf("expected success=%t, got err=%v expected err=%s", tc.buildHookSuccess, err, tc.expectedErr)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			err = hook(ctx, tc.username, true /* clientConnection */)
			if (err == nil) != tc.publicHookSuccess {
				t.Fatalf("expected success=%t, got err=%v expected err=%s", tc.publicHookSuccess, err, tc.expectedErr)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
			err = hook(ctx, tc.username, false /* clientConnection */)
			if (err == nil) != tc.privateHookSuccess {
				t.Fatalf("expected success=%t, got err=%v, expected err=%s", tc.privateHookSuccess, err, tc.expectedErr)
			}
			if err != nil {
				require.Regexp(t, tc.expectedErr, err.Error())
				return
			}
		})
	}
}
