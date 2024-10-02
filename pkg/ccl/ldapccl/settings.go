// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"crypto/x509"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// All cluster settings necessary for the LDAP authN/authZ feature.
const (
	baseLDAPAuthSettingName            = "server.ldap_authentication."
	ldapDomainCACertificateSettingName = baseLDAPAuthSettingName + "domain.custom_ca"
	ldapClientTLSCertSettingName       = baseLDAPAuthSettingName + "client.tls_certificate"
	ldapClientTLSKeySettingName        = baseLDAPAuthSettingName + "client.tls_key"
)

// LDAPDomainCACertificate is the CA cert PEM (appended to system's default CAs)
// for verifying LDAP server domain certificate.
var LDAPDomainCACertificate = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapDomainCACertificateSettingName,
	"sets the PEM encoded custom root CA for verifying domain certificates when establishing connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateCertificate),
)

// LDAPClientTLSCertSetting is the optional cert that client can use for mTLS.
var LDAPClientTLSCertSetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapClientTLSCertSettingName,
	"sets the client certificate PEM for establishing mTLS connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateCertificate),
)

// LDAPClientTLSKeySetting is the optional PEM key that client can use for mTLS.
var LDAPClientTLSKeySetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapClientTLSKeySettingName,
	"sets the client key PEM for establishing mTLS connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validatePrivateKey),
)

func validateCertificate(_ *settings.Values, s string) error {
	if len(s) != 0 {
		if ok := x509.NewCertPool().AppendCertsFromPEM([]byte(s)); !ok {
			return errors.Newf("LDAP initialization failed: invalid cert PEM block provided")
		}
	}
	return nil
}

// validatePrivateKey validates if post parsing private key PEM block we can
// obtain a `crypto.PrivateKey` object. It does not check if it pairs with the
// public key from provided certificate.
func validatePrivateKey(_ *settings.Values, s string) error {
	if len(s) != 0 {
		if _, err := security.PEMToPrivateKey([]byte(s)); err != nil {
			return errors.Newf("LDAP initialization failed: invalid private key PEM provided")
		}
	}
	return nil
}
