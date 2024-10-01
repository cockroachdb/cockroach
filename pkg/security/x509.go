// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Utility to generate x509 certificates, both CA and not.
// This is mostly based on http://golang.org/src/crypto/tls/generate_cert.go
// Most fields and settings are hard-coded. TODO(marc): allow customization.

const (
	// Make certs valid a day before to handle clock issues, specifically
	// boot2docker: https://github.com/boot2docker/boot2docker/issues/69
	validFrom                    = -time.Hour * 24
	maxPathLength                = 1
	caCommonName                 = "Cockroach CA"
	tenantURISANSchemeString     = "crdb"
	tenantNamePrefixString       = "tenant-name"
	tenantURISANFormatString     = tenantURISANSchemeString + "://" + "tenant/%d/user/%s"
	tenantNameURISANFormatString = tenantURISANSchemeString + "://" + tenantNamePrefixString + "/%s/user/%s"

	// TenantsOU is the OrganizationalUnit that determines a client certificate should be treated as a tenant client
	// certificate (as opposed to a KV node client certificate).
	TenantsOU = "Tenants"
)

// newTemplate returns a partially-filled template.
// It should be further populated based on whether the cert is for a CA or node.
func newTemplate(
	commonName string, lifetime time.Duration, orgUnits ...string,
) (*x509.Certificate, error) {
	// Generate a random serial number.
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	now := timeutil.Now()
	notBefore := now.Add(validFrom)
	notAfter := now.Add(lifetime)

	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:       []string{"Cockroach"},
			OrganizationalUnit: orgUnits,
			CommonName:         commonName,
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}

	return cert, nil
}

// GenerateCA generates a CA certificate and signs it using the signer (a private key).
// It returns the DER-encoded certificate.
func GenerateCA(signer crypto.Signer, lifetime time.Duration) ([]byte, error) {
	template, err := newTemplate(caCommonName, lifetime)
	if err != nil {
		return nil, err
	}

	// Set CA-specific fields.
	template.BasicConstraintsValid = true
	template.IsCA = true
	template.MaxPathLen = maxPathLength
	template.KeyUsage |= x509.KeyUsageCertSign
	template.KeyUsage |= x509.KeyUsageContentCommitment

	certBytes, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		signer.Public(),
		signer)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

func checkLifetimeAgainstCA(cert, ca *x509.Certificate) error {
	if ca.NotAfter.After(cert.NotAfter) || ca.NotAfter.Equal(cert.NotAfter) {
		return nil
	}

	now := timeutil.Now()
	// Truncate the lifetime to round hours, the maximum "pretty" duration.
	niceCALifetime := ca.NotAfter.Sub(now).Hours()
	niceCertLifetime := cert.NotAfter.Sub(now).Hours()
	return errors.Errorf("CA lifetime is %fh, shorter than the requested %fh. "+
		"Renew CA certificate, or rerun with --lifetime=%dh for a shorter duration.",
		niceCALifetime, niceCertLifetime, int64(niceCALifetime))
}

// GenerateServerCert generates a server certificate and returns the cert bytes.
// Takes in the CA cert and private key, the node public key, the certificate lifetime,
// and the list of hosts/ip addresses this certificate applies to.
func GenerateServerCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	nodePublicKey crypto.PublicKey,
	lifetime time.Duration,
	user username.SQLUsername,
	hosts []string,
) ([]byte, error) {
	// Create template for user.
	template, err := newTemplate(user.Normalized(), lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Both server and client authentication are allowed (for inter-node RPC).
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	addHostsToTemplate(template, hosts)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, nodePublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

func addHostsToTemplate(template *x509.Certificate, hosts []string) {
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}
}

// GenerateUIServerCert generates a server certificate for the Admin UI and returns the cert bytes.
// Takes in the CA cert and private key, the UI cert public key, the certificate lifetime,
// and the list of hosts/ip addresses this certificate applies to.
func GenerateUIServerCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	certPublicKey crypto.PublicKey,
	lifetime time.Duration,
	hosts []string,
) ([]byte, error) {
	// Use the first host as the CN. We still place all in the alternative subject name.
	template, err := newTemplate(hosts[0], lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Only server authentication is allowed.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	addHostsToTemplate(template, hosts)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, certPublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

// GenerateTenantCert generates a tenant client certificate and returns the cert bytes.
// Takes in the CA cert and private key, the tenant client public key, the certificate lifetime,
// and the tenant id.
//
// Tenant client certificates add OU=Tenants in the subject field to prevent
// using them as user certificates.
func GenerateTenantCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	clientPublicKey crypto.PublicKey,
	lifetime time.Duration,
	tenantID uint64,
	hosts []string,
) ([]byte, error) {

	if tenantID == 0 {
		return nil, errors.Errorf("tenantId %d is invalid (requires != 0)", tenantID)
	}

	// Create template for user.
	template, err := newTemplate(fmt.Sprintf("%d", tenantID), lifetime, TenantsOU)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Set client-specific fields.
	// Client authentication to authenticate to KV nodes.
	// Server authentication to authenticate to other SQL servers.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
	addHostsToTemplate(template, hosts)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, clientPublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

// GenerateClientCert generates a client certificate and returns the cert bytes.
// Takes in the CA cert and private key, the client public key, the certificate lifetime,
// username and tenant scope(s).
//
// This is used both for vanilla CockroachDB user client certs as well as for the
// multi-tenancy KV auth broker (in which case the user is a SQL tenant).
func GenerateClientCert(
	caCert *x509.Certificate,
	caPrivateKey crypto.PrivateKey,
	clientPublicKey crypto.PublicKey,
	lifetime time.Duration,
	user username.SQLUsername,
	tenantIDs []roachpb.TenantID,
	tenantNames []roachpb.TenantName,
) ([]byte, error) {

	// TODO(marc): should we add extra checks?
	if user.Undefined() {
		return nil, errors.Errorf("user cannot be empty")
	}

	// Create template for user.
	template, err := newTemplate(user.Normalized(), lifetime)
	if err != nil {
		return nil, err
	}

	// Don't issue certificates that outlast the CA cert.
	if err := checkLifetimeAgainstCA(template, caCert); err != nil {
		return nil, err
	}

	// Set client-specific fields.
	// Client authentication only.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	tenantIDURLs, err := MakeTenantURISANs(user, tenantIDs)
	if err != nil {
		return nil, err
	}
	tenantNameURLs, err := MakeTenantNameURISANs(user, tenantNames)
	if err != nil {
		return nil, err
	}
	template.URIs = append(template.URIs, tenantIDURLs...)
	template.URIs = append(template.URIs, tenantNameURLs...)

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, clientPublicKey, caPrivateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

// GenerateTenantSigningCert generates a signing certificate and returns the
// cert bytes. Takes in the signing keypair and the certificate lifetime.
func GenerateTenantSigningCert(
	publicKey crypto.PublicKey, privateKey crypto.PrivateKey, lifetime time.Duration, tenantID uint64,
) ([]byte, error) {
	now := timeutil.Now()
	template := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("Tenant %d Token Signing Certificate", tenantID),
		},
		SerialNumber:          big.NewInt(1), // The serial number does not matter because we are not using a certificate authority.
		BasicConstraintsValid: true,
		IsCA:                  false, // This certificate CANNOT sign other certificates.
		PublicKey:             publicKey,
		NotBefore:             now.Add(validFrom),
		NotAfter:              now.Add(lifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature, // This certificate can ONLY make signatures.
	}

	certBytes, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		publicKey,
		privateKey)
	if err != nil {
		return nil, err
	}

	return certBytes, nil
}

// MakeTenantURISANs constructs the tenant SAN URI for the client certificate.
func MakeTenantURISANs(
	username username.SQLUsername, tenantIDs []roachpb.TenantID,
) (urls []*url.URL, _ error) {
	for _, tenantID := range tenantIDs {
		uri, err := url.Parse(fmt.Sprintf(tenantURISANFormatString, tenantID.ToUint64(), username.Normalized()))
		if err != nil {
			return nil, err
		}
		urls = append(urls, uri)
	}
	return urls, nil
}

// MakeTenantNameURISANs constructs the tenant name SAN URI for the client certificate.
func MakeTenantNameURISANs(
	username username.SQLUsername, tenantNames []roachpb.TenantName,
) ([]*url.URL, error) {
	urls := make([]*url.URL, 0, len(tenantNames))
	for _, tenantName := range tenantNames {
		uri, err := url.Parse(fmt.Sprintf(tenantNameURISANFormatString, tenantName, username.Normalized()))
		if err != nil {
			return nil, err
		}
		urls = append(urls, uri)
	}
	return urls, nil
}

// isCRDBSANURI indicates whether the URI uses CRDB scheme.
func isCRDBSANURI(uri *url.URL) bool {
	return uri.Scheme == tenantURISANSchemeString
}

// isTenantNameSANURI indicates whether the URI is using tenant name to identify the tenant.
func isTenantNameSANURI(uri *url.URL) bool {
	return uri.Host == tenantNamePrefixString
}

// parseTenantURISAN extracts the user and tenant ID contained within a tenant URI SAN.
func parseTenantURISAN(uri *url.URL) (roachpb.TenantID, string, error) {
	rawURL := uri.String()
	r := strings.NewReader(rawURL)
	var tID uint64
	var username string
	_, err := fmt.Fscanf(r, tenantURISANFormatString, &tID, &username)
	if err != nil {
		return roachpb.TenantID{}, "", errors.Errorf("invalid tenant URI SAN %s", rawURL)
	}
	return roachpb.MustMakeTenantID(tID), username, nil
}

// parseTenantNameURISAN extracts the user and tenant name contained within a tenant URI SAN.
func parseTenantNameURISAN(uri *url.URL) (roachpb.TenantName, string, error) {
	if !isCRDBSANURI(uri) || !isTenantNameSANURI(uri) {
		return roachpb.TenantName(""), "", errors.Errorf("invalid tenant-name URI SAN %q", uri.String())
	}
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 4 {
		return roachpb.TenantName(""), "", errors.Errorf("invalid tenant-name URI SAN %q", uri.String())
	}
	tenantName, username := parts[1], parts[3]
	return roachpb.TenantName(tenantName), username, nil
}
