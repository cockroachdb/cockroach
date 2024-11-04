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
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

var certPrincipalMap struct {
	syncutil.RWMutex
	m map[string]string
}

type userCertDistinguishedNameMu struct {
	syncutil.RWMutex
	dn *ldap.DN
}

func (c *userCertDistinguishedNameMu) setDN(dn *ldap.DN) {
	c.Lock()
	defer c.Unlock()
	c.dn = dn
}

func (c *userCertDistinguishedNameMu) unsetDN() {
	c.Lock()
	defer c.Unlock()
	c.dn = nil
}

func (c *userCertDistinguishedNameMu) getDN() (dn *ldap.DN) {
	c.Lock()
	defer c.Unlock()
	dn = c.dn
	return dn
}

func (c *userCertDistinguishedNameMu) setDNWithString(dnString string) error {
	if len(dnString) == 0 {
		c.unsetDN()
		return nil
	}

	dn, err := distinguishedname.ParseDN(dnString)
	if err != nil {
		return errors.Errorf("invalid distinguished name string: %q", dnString)
	}
	c.setDN(dn)
	return nil
}

var rootSubjectMu, nodeSubjectMu userCertDistinguishedNameMu

func SetRootSubject(rootDNString string) error {
	return rootSubjectMu.setDNWithString(rootDNString)
}

func UnsetRootSubject() {
	rootSubjectMu.unsetDN()
}

func SetNodeSubject(nodeDNString string) error {
	return nodeSubjectMu.setDNWithString(nodeDNString)
}

func UnsetNodeSubject() {
	nodeSubjectMu.unsetDN()
}

// CertificateUserScope indicates the scope of a user certificate i.e. which
// tenant the user is allowed to authenticate on. Older client certificates
// without a tenant scope are treated as global certificates which can
// authenticate on any tenant strictly for backward compatibility with the older
// certificates. A certificate must specify the SQL user name in either the CN
// or a DNS SAN entry, so one certificate has multiple candidate usernames. The
// GetCertificateUserScope function expands a cert into a set of "scopes" with
// each possible username (and tenant ID).
type CertificateUserScope struct {
	Username   string
	TenantID   roachpb.TenantID
	TenantName roachpb.TenantName
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
	systemIdentity string,
	clientConnection bool,
) error

// applyRootOrNodeDNFlag returns distinguished name set for root or node user
// via root-cert-distinguished-name and node-cert-distinguished flags
// respectively if systemIdentity conforms to one of these 2 users. It may also
// return previously set subject option and systemIdentity is not root or node.
// Root and Node roles cannot have subject role option set for them.
func applyRootOrNodeDNFlag(previouslySetRoleSubject *ldap.DN, systemIdentity string) (dn *ldap.DN) {
	dn = previouslySetRoleSubject
	switch {
	case systemIdentity == username.RootUser:
		dn = rootSubjectMu.getDN()
	case systemIdentity == username.NodeUser:
		dn = nodeSubjectMu.getDN()
	}
	return dn
}

// CheckCertDNMatchesRootDNorNodeDN returns `rootOrNodeDNSet` which validates
// whether rootDN or nodeDN is currently set using their respective CLI flags
// *-cert-distinguished-name. It also returns `certDNMatchesRootOrNodeDN` which
// validates whether DN contained in cert being presented exactly matches rootDN
// or nodeDN (provided they are set).
func CheckCertDNMatchesRootDNorNodeDN(
	cert *x509.Certificate,
) (rootOrNodeDNSet bool, certDNMatchesRootOrNodeDN bool) {
	rootDN := rootSubjectMu.getDN()
	nodeDN := nodeSubjectMu.getDN()

	if rootDN != nil || nodeDN != nil {
		rootOrNodeDNSet = true
		certDN, err := distinguishedname.ParseDNFromCertificate(cert)
		if err != nil {
			return rootOrNodeDNSet, certDNMatchesRootOrNodeDN
		}
		// certDNMatchesRootOrNodeDN is true if certDN exactly matches set rootDN or set nodeDN
		if (rootDN != nil && certDN.Equal(rootDN)) || (nodeDN != nil && certDN.Equal(nodeDN)) {
			certDNMatchesRootOrNodeDN = true
		}
	}
	return rootOrNodeDNSet, certDNMatchesRootOrNodeDN
}

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

// GetCertificateUserScope extracts the certificate scopes from a client
// certificate. It tries to get CRDB prefixed SAN URIs and extracts tenantID and
// user information. If there is no such URI, then it gets principal transformed
// CN and SAN DNSNames with global scope.
func GetCertificateUserScope(
	peerCert *x509.Certificate,
) (userScopes []CertificateUserScope, _ error) {
	collectFn := func(userScope CertificateUserScope) (halt bool, err error) {
		userScopes = append(userScopes, userScope)
		return false, nil
	}
	err := forEachCertificateUserScope(peerCert, collectFn)
	return userScopes, err
}

// CertificateUserScopeContainsFunc returns true if the given function returns
// true for any of the scopes in the client certificate.
func CertificateUserScopeContainsFunc(
	peerCert *x509.Certificate, fn func(CertificateUserScope) bool,
) (ok bool, _ error) {
	res := false
	containsFn := func(userScope CertificateUserScope) (halt bool, err error) {
		if fn(userScope) {
			res = true
			return true, nil
		}
		return false, nil
	}
	err := forEachCertificateUserScope(peerCert, containsFn)
	return res, err
}

func forEachCertificateUserScope(
	peerCert *x509.Certificate, fn func(userScope CertificateUserScope) (halt bool, err error),
) error {
	hasCRDBSANURI := false
	for _, uri := range peerCert.URIs {
		if !isCRDBSANURI(uri) {
			continue
		}
		hasCRDBSANURI = true
		var scope CertificateUserScope
		if isTenantNameSANURI(uri) {
			tenantName, user, err := parseTenantNameURISAN(uri)
			if err != nil {
				return err
			}
			scope = CertificateUserScope{
				Username:   user,
				TenantName: tenantName,
			}
		} else {
			tenantID, user, err := parseTenantURISAN(uri)
			if err != nil {
				return err
			}
			scope = CertificateUserScope{
				Username: user,
				TenantID: tenantID,
			}
		}
		if halt, err := fn(scope); halt || err != nil {
			return err
		}
	}
	if !hasCRDBSANURI {
		globalScope := func(user string) CertificateUserScope {
			return CertificateUserScope{
				Username: user,
				Global:   true,
			}
		}
		halt, err := fn(globalScope(transformPrincipal(peerCert.Subject.CommonName)))
		if halt || err != nil {
			return err
		}
		for _, name := range peerCert.DNSNames {
			if halt, err := fn(globalScope(transformPrincipal(name))); halt || err != nil {
				return err
			}
		}
	}
	return nil
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
	roleSubject *ldap.DN,
	subjectRequired bool,
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

	return func(ctx context.Context, systemIdentity string, clientConnection bool) error {
		if systemIdentity == "" {
			return errors.New("user is missing")
		}

		if !clientConnection && systemIdentity != username.NodeUser {
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

		roleSubject = applyRootOrNodeDNFlag(roleSubject, systemIdentity)
		if subjectRequired && roleSubject == nil {
			return errors.Newf(
				"user %q does not have a distinguished name set which subject_required cluster setting mandates",
				systemIdentity,
			)
		}

		var certSubject *ldap.DN
		if roleSubject != nil {
			var err error
			if certSubject, err = distinguishedname.ParseDNFromCertificate(peerCert); err != nil {
				return errors.Wrapf(err, "could not parse certificate subject DN")
			}
		}

		if ValidateUserScope(certUserScope, systemIdentity, tenantID, roleSubject, certSubject) {
			if certManager != nil {
				certManager.MaybeUpsertClientExpiration(
					ctx,
					systemIdentity,
					peerCert.NotAfter.Unix(),
				)
			}
			return nil
		}
		return errors.WithDetailf(
			errors.Errorf(
				"certificate authentication failed for user %q (DN: %s)",
				systemIdentity,
				roleSubject,
			),
			"The client certificate (DN: %s) is valid for %s.", certSubject, FormatUserScopes(certUserScope))
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
	return func(ctx context.Context, systemIdentity string, clientConnection bool) error {
		u, err := username.MakeSQLUsernameFromUserInput(systemIdentity, username.PurposeValidation)
		if err != nil {
			return err
		}
		if u.Undefined() {
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
			return NewErrPasswordUserAuthFailed(u)
		}
		ok, err := password.CompareHashAndCleartextPassword(ctx,
			hashedPassword, passwordStr, GetExpensiveHashComputeSemWithGauge(ctx, gauge))
		if err != nil {
			return err
		}
		if !ok {
			return NewErrPasswordUserAuthFailed(u)
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

// ValidateUserScope returns true if the user is a valid user for the tenant
// based on the certificate user scope. It also returns true if the certificate
// is a global certificate. A client certificate is considered global only when
// it doesn't contain a tenant SAN which is only possible for older client
// certificates created prior to introducing tenant based scoping for the
// client. Additionally, if subject role option is set for a user, we check if
// certificate parsed subject DN matches the set subject.
func ValidateUserScope(
	certUserScope []CertificateUserScope,
	user string,
	tenantID roachpb.TenantID,
	roleSubject *ldap.DN,
	certSubject *ldap.DN,
) bool {
	// if subject role option is set, it must match the certificate subject
	if roleSubject != nil {
		return roleSubject.Equal(certSubject)
	}
	for _, scope := range certUserScope {
		if scope.Username == user {
			// If username matches, allow authentication to succeed if
			// the tenantID is a match or if the certificate scope is global.
			if scope.TenantID == tenantID || scope.Global {
				return true
			}
		}
	}
	// No user match, return false.
	return false
}
