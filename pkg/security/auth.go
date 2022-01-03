// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var certPrincipalMap struct {
	syncutil.RWMutex
	m map[string]string
}

// UserAuthHook authenticates a user based on their username and whether their
// connection originates from a client or another node in the cluster. It
// returns an optional func that is run at connection close.
//
// The systemIdentity is the external identity, from GSSAPI or an X.509
// certificate, while databaseUsername reflects any username mappings
// that may have been applied to the given connection.
type UserAuthHook func(
	ctx context.Context,
	systemIdentity SQLUsername,
	clientConnection bool,
) error

// SetCertPrincipalMap sets the global principal map. Each entry in the mapping
// list must either be empty or have the format <source>:<dest>. The principal
// map is used to transform principal names found in the Subject.CommonName or
// DNS-type SubjectAlternateNames fields of certificates. This function splits
// each list entry on the final colon, allowing <source> to contain colons.
func SetCertPrincipalMap(mappings []string) error {
	m := make(map[string]string, len(mappings))
	for _, v := range mappings {
		if v == "" {
			continue
		}
		idx := strings.LastIndexByte(v, ':')
		if idx == -1 {
			return errors.Errorf("invalid <cert-principal>:<db-principal> mapping: %q", v)
		}
		m[v[:idx]] = v[idx+1:]
	}
	certPrincipalMap.Lock()
	certPrincipalMap.m = m
	certPrincipalMap.Unlock()
	return nil
}

func transformPrincipal(commonName string) string {
	certPrincipalMap.RLock()
	mappedName, ok := certPrincipalMap.m[commonName]
	certPrincipalMap.RUnlock()
	if !ok {
		return commonName
	}
	return mappedName
}

func getCertificatePrincipals(cert *x509.Certificate) []string {
	results := make([]string, 0, 1+len(cert.DNSNames))
	results = append(results, transformPrincipal(cert.Subject.CommonName))
	for _, name := range cert.DNSNames {
		results = append(results, transformPrincipal(name))
	}
	return results
}

// GetCertificateUsers extract the users from a client certificate.
func GetCertificateUsers(tlsState *tls.ConnectionState) ([]string, error) {
	if tlsState == nil {
		return nil, errors.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.Errorf("no client certificates in request")
	}
	// The go server handshake code verifies the first certificate, using
	// any following certificates as intermediates. See:
	// https://github.com/golang/go/blob/go1.8.1/src/crypto/tls/handshake_server.go#L723:L742
	peerCert := tlsState.PeerCertificates[0]
	return getCertificatePrincipals(peerCert), nil
}

// Contains returns true if the specified string is present in the given slice.
func Contains(sl []string, s string) bool {
	for i := range sl {
		if sl[i] == s {
			return true
		}
	}
	return false
}

// UserAuthCertHook builds an authentication hook based on the security
// mode and client certificate.
func UserAuthCertHook(insecureMode bool, tlsState *tls.ConnectionState) (UserAuthHook, error) {
	var certUsers []string

	if !insecureMode {
		var err error
		certUsers, err = GetCertificateUsers(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(ctx context.Context, systemIdentity SQLUsername, clientConnection bool) error {
		// TODO(marc): we may eventually need stricter user syntax rules.
		if systemIdentity.Undefined() {
			return errors.New("user is missing")
		}

		if !clientConnection && !systemIdentity.IsNodeUser() {
			return errors.Errorf("user %s is not allowed", systemIdentity)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate should not be a tenant client type. For now just
		// check that it doesn't have OU=Tenants. It would make sense to add
		// explicit OU=Users to all client certificates and to check for match.
		if IsTenantCertificate(tlsState.PeerCertificates[0]) {
			return errors.Errorf("using tenant client certificate as user certificate is not allowed")
		}

		// The client certificate user must match the requested user.
		if !Contains(certUsers, systemIdentity.Normalized()) {
			return errors.Errorf("requested user is %s, but certificate is for %s", systemIdentity, certUsers)
		}

		return nil
	}, nil
}

// IsTenantCertificate returns true if the passed certificate indicates an
// inbound Tenant connection.
func IsTenantCertificate(cert *x509.Certificate) bool {
	return Contains(cert.Subject.OrganizationalUnit, TenantsOU)
}

// UserAuthPasswordHook builds an authentication hook based on the security
// mode, password, and its potentially matching hash.
func UserAuthPasswordHook(
	insecureMode bool, password string, hashedPassword PasswordHash,
) UserAuthHook {
	return func(ctx context.Context, systemIdentity SQLUsername, clientConnection bool) error {
		if systemIdentity.Undefined() {
			return errors.New("user is missing")
		}

		if !clientConnection {
			return errors.New("password authentication is only available for client connections")
		}

		if insecureMode {
			return nil
		}

		// If the requested user has an empty password, disallow authentication.
		if len(password) == 0 {
			return NewErrPasswordUserAuthFailed(systemIdentity)
		}
		ok, err := CompareHashAndCleartextPassword(ctx, hashedPassword, password)
		if err != nil {
			return err
		}
		if !ok {
			return NewErrPasswordUserAuthFailed(systemIdentity)
		}

		return nil
	}
}

// NewErrPasswordUserAuthFailed constructs an error that represents
// failed password authentication for a user. It should be used when
// the password is incorrect or the user does not exist.
func NewErrPasswordUserAuthFailed(username SQLUsername) error {
	return errors.Newf("password authentication failed for user %s", username)
}
