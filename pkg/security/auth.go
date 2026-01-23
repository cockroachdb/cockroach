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

// disallowRootLoginMu controls whether root login should be disallowed.
var disallowRootLoginMu struct {
	syncutil.RWMutex
	disallowed bool
}

// allowDebugUserMu controls whether debuguser login should be allowed.
var allowDebugUserMu struct {
	syncutil.RWMutex
	allowed bool
}

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

// SetDisallowRootLogin sets whether root login should be disallowed.
func SetDisallowRootLogin(disallow bool) {
	disallowRootLoginMu.Lock()
	defer disallowRootLoginMu.Unlock()
	disallowRootLoginMu.disallowed = disallow
}

// CheckRootLoginDisallowed returns whether root login is currently disallowed.
func CheckRootLoginDisallowed() bool {
	disallowRootLoginMu.RLock()
	defer disallowRootLoginMu.RUnlock()
	return disallowRootLoginMu.disallowed
}

// SetAllowDebugUser sets whether debuguser login should be allowed.
func SetAllowDebugUser(allow bool) {
	allowDebugUserMu.Lock()
	defer allowDebugUserMu.Unlock()
	allowDebugUserMu.allowed = allow
}

// CheckDebugUserLoginAllowed returns whether debuguser login is currently allowed.
func CheckDebugUserLoginAllowed() bool {
	allowDebugUserMu.RLock()
	defer allowDebugUserMu.RUnlock()
	return allowDebugUserMu.allowed
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

type userCertSANMu struct {
	syncutil.RWMutex
	sans []string
}

func (c *userCertSANMu) setSANs(sans []string) {
	c.Lock()
	defer c.Unlock()
	c.sans = sans
}

func (c *userCertSANMu) unsetSANs() {
	c.Lock()
	defer c.Unlock()
	c.sans = nil
}

func (c *userCertSANMu) getSANs() []string {
	c.RLock()
	defer c.RUnlock()
	return c.sans
}

type SANAttributeType string

const (
	SANAttributeTypeDNS SANAttributeType = "DNS"
	SANAttributeTypeURI SANAttributeType = "URI"
	SANAttributeTypeIP  SANAttributeType = "IP"
)

func (s SANAttributeType) String() string {
	return string(s)
}

// setSANsWithString sets the SANs from a comma-separated string of entries in
// the format attribute-type=attribute-value. For example:
// "DNS=example.com,URI=spiffe://example.org/service"
// is converted to a list SAN:DNS:example.com, SAN:URI:spiffe://example.org/service.
// Only DNS, URI, and IP attribute types are allowed.
func (c *userCertSANMu) setSANsWithString(sanString string) error {
	if len(sanString) == 0 {
		c.unsetSANs()
		return nil
	}

	// Split comma-separated SAN attributes
	sanEntries := strings.Split(sanString, ",")

	// Parse each entry from attribute-type=attribute-value format
	// and convert to SAN:attribute-type:attribute-value format
	validSANs := make([]string, 0, len(sanEntries))
	for _, entry := range sanEntries {
		trimmed := strings.TrimSpace(entry)
		if trimmed == "" {
			continue
		}

		// Split on '=' to get attribute-type and attribute-value
		idx := strings.IndexByte(trimmed, '=')
		if idx == -1 {
			return errors.Errorf("invalid SAN entry format: %q (expected format: attribute-type=attribute-value)", trimmed)
		}

		attrType := strings.TrimSpace(trimmed[:idx])
		attrValue := strings.TrimSpace(trimmed[idx+1:])

		if attrType == "" {
			return errors.Errorf("invalid SAN entry format: %q (attribute type cannot be empty)", trimmed)
		}

		if attrValue == "" {
			return errors.Errorf("invalid SAN entry format: %q (attribute value cannot be empty)", trimmed)
		}

		// Normalize attribute type to uppercase
		attrTypeUpper := strings.ToUpper(attrType)

		// Validate that attribute type is one of the allowed types
		if attrTypeUpper != SANAttributeTypeDNS.String() && attrTypeUpper != SANAttributeTypeURI.String() && attrTypeUpper != SANAttributeTypeIP.String() {
			return errors.Errorf("invalid SAN attribute type: %q (only DNS, URI, and IP are allowed)", attrType)
		}

		// Convert to SAN:TYPE:value format and store
		validSANs = append(validSANs, fmt.Sprintf("SAN:%s:%s", attrTypeUpper, attrValue))
	}

	if len(validSANs) == 0 {
		return errors.Errorf("invalid SAN string: %q (no valid entries found)", sanString)
	}

	c.setSANs(validSANs)
	return nil
}

var rootSANMu, nodeSANMu userCertSANMu

// SetRootSAN sets the global root SANs from a comma-separated string
// and stores them as a list similar to
// SAN:DNS:example.com, SAN:URI:spiffe://example.org/service.
func SetRootSAN(rootSANString string) error {
	return rootSANMu.setSANsWithString(rootSANString)
}

func UnsetRootSAN() {
	rootSANMu.unsetSANs()
}

func GetRootSANs() []string {
	return rootSANMu.getSANs()
}

// SetNodeSAN sets the global node SANs from a comma-separated string
// and stores them as a list similar to
// SAN:DNS:example.com, SAN:URI:spiffe://example.org/service.
func SetNodeSAN(nodeSANString string) error {
	return nodeSANMu.setSANsWithString(nodeSANString)
}

func UnsetNodeSAN() {
	nodeSANMu.unsetSANs()
}

func GetNodeSANs() []string {
	return nodeSANMu.getSANs()
}

// ExtractSANsFromCertificate extracts SAN entries from certificate in
// SAN:TYPE:value format (e.g., SAN:URI:spiffe://..., SAN:DNS:example.com)
// This maintains the order of SAN attributes as URI -> IP -> DNS.
func ExtractSANsFromCertificate(cert *x509.Certificate) []string {
	var sans []string

	// Extract URI SANs
	for _, uri := range cert.URIs {
		sans = append(sans, fmt.Sprintf("SAN:%s:%s", SANAttributeTypeURI.String(), uri.String()))
	}

	// Extract IP SANs
	for _, ip := range cert.IPAddresses {
		sans = append(sans, fmt.Sprintf("SAN:%s:%s", SANAttributeTypeIP.String(), ip.String()))
	}

	// Extract DNS SANs
	for _, dns := range cert.DNSNames {
		sans = append(sans, fmt.Sprintf("SAN:%s:%s", SANAttributeTypeDNS.String(), dns))
	}

	return sans
}

// sanListIsSubset checks if SAN subset is a subset of SAN Superset
// Returns true if all SANs in subset are present in superset.
func sanListIsSubset(sanSubset, sanSuperset []string) bool {
	// Create a map of superset for O(1) lookups
	supersetMap := make(map[string]bool, len(sanSuperset))
	for _, san := range sanSuperset {
		supersetMap[san] = true
	}

	// Check if all entries in subset exist in superset
	for _, san := range sanSubset {
		if !supersetMap[san] {
			return false
		}
	}

	return true
}

// checkCertSANMatchesRootSANorNodeSAN returns `rootOrNodeSANSet` which validates
// whether rootSAN or nodeSAN is currently set using their respective CLI flags
// *-cert-san. It also returns `certSANMatchesRootOrNodeSAN` which
// validates whether SANs contained in cert being presented contain all the
// configured rootSAN or nodeSAN entries for the specific user type
// (i.e., configured SANs are a subset of cert SANs).
func checkCertSANMatchesRootSANorNodeSAN(
	certSANs []string, user string,
) (rootOrNodeSANSet bool, certSANMatchesRootOrNodeSAN bool) {
	// Check user-specific SANs based on username
	switch user {
	case username.RootUser:
		rootSANs := rootSANMu.getSANs()
		if len(rootSANs) > 0 {
			rootOrNodeSANSet = true
			// For root user, only check root SANs
			if sanListIsSubset(rootSANs, certSANs) {
				certSANMatchesRootOrNodeSAN = true
			}
		}
	case username.NodeUser:
		nodeSANs := nodeSANMu.getSANs()
		if len(nodeSANs) > 0 {
			rootOrNodeSANSet = true
			// For node user, only check node SANs
			if sanListIsSubset(nodeSANs, certSANs) {
				certSANMatchesRootOrNodeSAN = true
			}
		}
	}
	return rootOrNodeSANSet, certSANMatchesRootOrNodeSAN
}

func isRootOrNodeUser(user string) bool {
	return user == username.RootUser || user == username.NodeUser
}

// Helper function to validate SAN for specific users
// For root and node users, performs actual SAN validation
// For all other users SAN is only used to map to a
// SQL user using identity mapping and not validate the
// SAN itself, hence we return true.
func validateSANMatchForUser(certSANs []string, user string) bool {
	// Only validate SAN for root and node users
	if !isRootOrNodeUser(user) {
		return true
	}

	// For root/node users, perform actual validation
	_, match := checkCertSANMatchesRootSANorNodeSAN(certSANs, user)
	return match
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
	tenantName roachpb.TenantName,
	certManager *CertificateManager,
	roleSubject *ldap.DN,
	subjectRequired bool,
	clientCertSANRequired bool,
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
		// If both subjectRequired and clientCertSANRequired are true, we use OR logic,
		// so we don't fail here if subject is missing, let ValidateUserScope handle it
		if subjectRequired && !clientCertSANRequired && roleSubject == nil {
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

		certSANs := ExtractSANsFromCertificate(peerCert)
		if ValidateUserScope(certUserScope, systemIdentity, tenantID, tenantName, roleSubject, certSubject, clientCertSANRequired, certSANs) {
			if certManager != nil {
				certManager.MaybeUpsertClientExpiration(
					ctx,
					systemIdentity,
					peerCert.SerialNumber.String(),
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
			if scope.TenantID.IsSet() {
				fmt.Fprintf(&buf, "tenantID %v", scope.TenantID)
			}
			if scope.TenantName != "" {
				fmt.Fprintf(&buf, "tenantName %v", scope.TenantName)
			}
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
	tenantName roachpb.TenantName,
	roleSubject *ldap.DN,
	certSubject *ldap.DN,
	clientCertSANRequired bool,
	certSANs []string,
) bool {
	if clientCertSANRequired {
		// Try SAN validation for root and node users. For now, other users
		// SAN mapping is done using the HBA identity map regex and not exact match.
		sanMatches := validateSANMatchForUser(certSANs, user)
		if sanMatches {
			return true
		}
	}

	// if subject role option is set, it must match the certificate subject
	if roleSubject != nil {
		return roleSubject.Equal(certSubject)
	}

	if clientCertSANRequired {
		// Do not validate CN in case SAN is required.
		// We check for SAN OR Subject mapping in case san in enabled.
		return false
	}

	for _, scope := range certUserScope {
		// Check if root login is disallowed and certificate contains "root" as a principal
		if CheckRootLoginDisallowed() && scope.Username == username.RootUser {
			return false
		}

		// Check if debuguser login is not allowed and certificate contains "debuguser" as a principal
		if !CheckDebugUserLoginAllowed() && scope.Username == username.DebugUser {
			return false
		}

		if scope.Username == user {
			// If username matches, allow authentication to succeed if
			// the tenantID is a match or if the certificate scope is global.
			if scope.Global {
				return true
			}
			if scope.TenantID.IsSet() && scope.TenantID == tenantID {
				return true
			}
			if scope.TenantName != "" && scope.TenantName == tenantName {
				return true
			}
		}
	}
	// No user match, return false.
	return false
}
