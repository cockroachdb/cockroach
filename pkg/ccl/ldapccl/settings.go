// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ldapccl

import (
	"crypto/x509"

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

// LDAPDomainCACertificate is CA cert PEM (appended to system's default CAs) for
// verifying LDAP server domain certificate.
var LDAPDomainCACertificate = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapDomainCACertificateSettingName,
	"sets the custom root CA for verifying domain certificates when establishing connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateLDAPDomainCACertificate),
)

// LDAPClientTLSCertSetting is optional cert key that client can use for mTLS.
var LDAPClientTLSCertSetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapClientTLSCertSettingName,
	"sets the client certificate for establishing mTLS connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
)

// LDAPClientTLSCertSetting is optional certificate that client can use for mTLS.
var LDAPClientTLSKeySetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	ldapClientTLSKeySettingName,
	"sets the client key for establishing mTLS connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
)

func validateLDAPDomainCACertificate(values *settings.Values, s string) error {
	if len(s) != 0 {
		if ok := x509.NewCertPool().AppendCertsFromPEM([]byte(s)); !ok {
			return errors.Newf("LDAP authentication could not parse domain CA cert PEM")
		}
	}
	return nil
}
