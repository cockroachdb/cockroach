// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distinguishedname

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

// ValidateDN validates a distinguished name string to verify that it is
// well-formed and valid for the given user.
func ValidateDN(u username.SQLUsername, dnStr string) error {
	if u.IsRootUser() {
		return errors.Newf("role %q cannot have a SUBJECT", u)
	}
	dn, err := ParseDN(dnStr)
	if err != nil {
		return err
	}
	fieldsList := []string{"CN", "L", "ST", "O", "OU", "C", "STREET", "DC", "UID"}
	for _, rdn := range dn.RDNs {
		for _, attr := range rdn.Attributes {
			if !slices.Contains(fieldsList, attr.Type) {
				return errors.Newf("SUBJECT contains illegal field type %q, should be one of %+q", attr.Type, fieldsList)
			}
		}
	}
	return nil
}

// ParseDN parses a distinguished name string. It must be in RFC4514 or RFC2253
// format.
func ParseDN(dnStr string) (*ldap.DN, error) {
	dn, err := ldap.ParseDN(dnStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse distinguished name %s", dnStr)
	}
	return dn, nil
}

// ParseDNFromCertificate parses the distinguished name for the subject from
// X.509 certificate provided. It retains the sequence of fields as provided in
// the certificate subject and also parses all fields mentioned in RFC4514 which
// ldap/v3 library currently supports.
func ParseDNFromCertificate(cert *x509.Certificate) (*ldap.DN, error) {
	var RDNSeq pkix.RDNSequence
	_, err := asn1.Unmarshal(cert.RawSubject, &RDNSeq)
	if err != nil {
		return nil, err
	}

	// This is required because RDNSeq.String() reverses the order of fields.
	// The x509 library possibly intended to use cert.Subject.ToRDNSequence and
	// RDNSequence.String() in succession which is done in the library function
	// cert.Subject.String(). But since x509 is incapable of handling all fields
	// defined in RFC 4514, we need to directly parse cert.RawSubject here.
	slices.Reverse(RDNSeq)
	subjectDN, err := ParseDN(RDNSeq.String())
	if err != nil {
		return nil, err
	}

	const (
		// Go only parses a subset of the possible fields in a DN (golang/go#25667).
		// We add the remaining ones defined in section 3 of RFC 4514
		// (https://datatracker.ietf.org/doc/html/rfc4514#section-3)
		encodedUserID          = "0.9.2342.19200300.100.1.1"
		encodedDomainComponent = "0.9.2342.19200300.100.1.25"
	)

	for _, dn := range subjectDN.RDNs {
		for _, attr := range dn.Attributes {
			switch attr.Type {
			case encodedUserID:
				attr.Type = "UID"
			case encodedDomainComponent:
				attr.Type = "DC"
			}
		}
	}

	return subjectDN, nil
}
