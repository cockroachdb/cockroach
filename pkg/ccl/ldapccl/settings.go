// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ldapccl

import (
	"crypto/x509"
	"encoding/pem"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// All cluster settings necessary for the LDAP authN/authZ feature.
const (
	baseLDAPAuthSettingName            = "server.ldap_authentication."
	LDAPDomainCertificateSettingName   = baseLDAPAuthSettingName + "domain_certificate"
	LDAPDomainCACertificateSettingName = baseLDAPAuthSettingName + "domain_ca"
)

//// LDAPDomainCertificate sets the LDAP server certificate for domain controller.
//var LDAPDomainCertificate = settings.RegisterStringSetting(
//	settings.ApplicationLevel,
//	LDAPDomainCertificateSettingName,
//	"sets the LDAP server certificate for domain controller",
//	"",
//	settings.WithValidateString(validateDomainCertificate),
//)

var LDAPDomainCACertificate = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	LDAPDomainCACertificateSettingName,
	"sets the custom root CA for verifying domajn certificates when establishing connection with LDAP server",
	"",
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
)

func validateDomainCertificate(values *settings.Values, s string) error {
	block, _ := pem.Decode([]byte(s))
	if block == nil || block.Type != "RSA private key certificate" {
		return errors.Newf("LDAP authentication could not decode pem block from domain certificate")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return errors.Wrap(err, "LDAP authentication domain cert pem block could not parsed")
	}

	if _, err := url.Parse(cert.Subject.CommonName); err != nil {
		return errors.Wrap(err, "LDAP authentication server cert subject CN is not a valid domain name")
	}

	//certUsageDigitalSignature, certUsageKeyEncipherment := false, false
	//for keyUsage := range cert.Extensions {
	//	if keyUsage == int(x509.ExtKeyUsageServerAuth) {
	//		certValidServerAuth = true
	//	}
	//}

	certValidServerAuth := false
	for keyUsage := range cert.ExtKeyUsage {
		if keyUsage == int(x509.ExtKeyUsageServerAuth) {
			certValidServerAuth = true
		}
	}

	if !certValidServerAuth {
		return errors.Newf("LDAP authentication domain certificate not valid for server authentication")
	}

	return nil
}
