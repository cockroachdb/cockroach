// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var certPrincipalMap struct {
	syncutil.RWMutex
	m map[string]string
}

// CertificateUserScope indicates the scope of a user certificate i.e.
// which tenant the user is allowed to authenticate on. Older client certificates
// without a tenant scope are treated as global certificates which can
// authenticate on any tenant strictly for backward compatibility with the
// older certificates.
type CertificateUserScope struct {
	Username string
	TenantID roachpb.TenantID
	// global is set to true to indicate that the certificate unscoped to
	// any tenant is a global client certificate which can authenticate
	// on any tenant. This is ONLY for backward compatibility with old
	// client certificates without a tenant scope.
	Global bool
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
	systemIdentity username.SQLUsername,
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

// GetCertificateUserScope extracts the certificate scopes from a client certificate.
func GetCertificateUserScope(
	peerCert *x509.Certificate,
) (userScopes []CertificateUserScope, _ error) {
	for _, uri := range peerCert.URIs {
		uriString := uri.String()
		if URISANHasCRDBPrefix(uriString) {
			tenantID, user, err := ParseTenantURISAN(uriString)
			if err != nil {
				return nil, err
			}
			scope := CertificateUserScope{
				Username: user,
				TenantID: tenantID,
			}
			userScopes = append(userScopes, scope)
		}
	}
	if len(userScopes) == 0 {
		users := getCertificatePrincipals(peerCert)
		for _, user := range users {
			scope := CertificateUserScope{
				Username: user,
				Global:   true,
			}
			userScopes = append(userScopes, scope)
		}
	}
	return userScopes, nil
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
func UserAuthCertHook(
	insecureMode bool,
	tlsState *tls.ConnectionState,
	tenantID roachpb.TenantID,
	certManager *CertificateManager,
) (UserAuthHook, error) {
	var certUserScope []CertificateUserScope
	if !insecureMode {
		if tlsState == nil {
			return nil, errors.Errorf("request is not using TLS")
		}
		if len(tlsState.PeerCertificates) == 0 {
			return nil, errors.Errorf("no client certificates in request")
		}
		peerCert := tlsState.PeerCertificates[0]

		var err error
		certUserScope, err = GetCertificateUserScope(peerCert)
		if err != nil {
			return nil, err
		}
	}

	return func(ctx context.Context, systemIdentity username.SQLUsername, clientConnection bool) error {
		// TODO(marc): we may eventually need stricter user syntax rules.
		if systemIdentity.Undefined() {
			return errors.New("user is missing")
		}

		if !clientConnection && !systemIdentity.IsNodeUser() {
			return errors.Errorf("user %q is not allowed", systemIdentity)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		peerCert := tlsState.PeerCertificates[0]

		// The client certificate should not be a tenant client type. For now just
		// check that it doesn't have OU=Tenants. It would make sense to add
		// explicit OU=Users to all client certificates and to check for match.
		if IsTenantCertificate(peerCert) {
			return errors.Errorf("using tenant client certificate as user certificate is not allowed")
		}

		if ValidateUserScope(certUserScope, systemIdentity.Normalized(), tenantID) {
			if certManager != nil {
				certManager.MaybeUpsertClientExpiration(
					ctx,
					systemIdentity,
					peerCert.NotAfter.Unix(),
				)
			}
			return nil
		}
		return errors.WithDetailf(errors.Errorf("certificate authentication failed for user %q", systemIdentity),
			"The client certificate is valid for %s.", FormatUserScopes(certUserScope))
	}, nil
}

// FormatUserScopes formats a list of scopes in a human-readable way,
// suitable for e.g. inclusion in error messages.
func FormatUserScopes(certUserScope []CertificateUserScope) string {
	var buf strings.Builder
	comma := ""
	for _, scope := range certUserScope {
		fmt.Fprintf(&buf, "%s%q on ", comma, scope.Username)
		if scope.Global {
			buf.WriteString("all tenants")
		} else {
			fmt.Fprintf(&buf, "tenant %v", scope.TenantID)
		}
		comma = ", "
	}
	return buf.String()
}

// IsTenantCertificate returns true if the passed certificate indicates an
// inbound Tenant connection.
func IsTenantCertificate(cert *x509.Certificate) bool {
	return Contains(cert.Subject.OrganizationalUnit, TenantsOU)
}

// UserAuthPasswordHook builds an authentication hook based on the security
// mode, password, and its potentially matching hash.
func UserAuthPasswordHook(
	insecureMode bool, passwordStr string, hashedPassword password.PasswordHash, gauge *metric.Gauge,
) UserAuthHook {
	return func(ctx context.Context, systemIdentity username.SQLUsername, clientConnection bool) error {
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
		if len(passwordStr) == 0 {
			return NewErrPasswordUserAuthFailed(systemIdentity)
		}
		ok, err := password.CompareHashAndCleartextPassword(ctx,
			hashedPassword, passwordStr, GetExpensiveHashComputeSemWithGauge(ctx, gauge))
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
func NewErrPasswordUserAuthFailed(user username.SQLUsername) error {
	return &PasswordUserAuthError{errors.Newf("password authentication failed for user %s", user)}
}

// PasswordUserAuthError indicates that an error was encountered
// during the initial set-up of a SQL connection.
type PasswordUserAuthError struct {
	err error
}

// Error implements the error interface.
func (i *PasswordUserAuthError) Error() string { return i.err.Error() }

// Cause implements causer for compatibility with pkg/errors.
// NB: this is obsolete. Use Unwrap() instead.
func (i *PasswordUserAuthError) Cause() error { return i.err }

// Unwrap implements errors.Wrapper.
func (i *PasswordUserAuthError) Unwrap() error { return i.err }

// Format implements fmt.Formatter.
func (i *PasswordUserAuthError) Format(s fmt.State, verb rune) { errors.FormatError(i, s, verb) }

// FormatError implements errors.Formatter.
func (i *PasswordUserAuthError) FormatError(p errors.Printer) error {
	return i.err
}

// ValidateUserScope returns true if the user is a valid user for the tenant based on the certificate
// user scope. It also returns true if the certificate is a global certificate. A client certificate
// is considered global only when it doesn't contain a tenant SAN which is only possible for older
// client certificates created prior to introducing tenant based scoping for the client.
func ValidateUserScope(
	certUserScope []CertificateUserScope, user string, tenantID roachpb.TenantID,
) bool {
	for _, scope := range certUserScope {
		if scope.Username == user {
			// If username matches, allow authentication to succeed if the tenantID is a match
			// or if the certificate scope is global.
			if scope.TenantID == tenantID || scope.Global {
				return true
			}
		}
	}
	// No user match, return false.
	return false
}
