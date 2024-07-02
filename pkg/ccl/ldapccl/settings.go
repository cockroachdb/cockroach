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
	LDAPDomainCACertificateSettingName = baseLDAPAuthSettingName + "domain.custom_ca"
	LDAPClientTLSCertSettingName       = baseLDAPAuthSettingName + "client.tls_certificate"
	LDAPClientTLSKeySettingName        = baseLDAPAuthSettingName + "client.tls_key"
)

var LDAPDomainCACertificate = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	LDAPDomainCACertificateSettingName,
	"sets the custom root CA for verifying domain certificates when establishing connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateLDAPDomainCACertificate),
)

var LDAPClientTLSCertSetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	LDAPClientTLSCertSettingName,
	"sets the client certificate for establishing mTLS connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
)

var LDAPClientTLSKeySetting = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	LDAPClientTLSKeySettingName,
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
