// Copyright 2017 The Cockroach Authors.
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
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
)

// KeyUsageToString returns the list of key usages described by the bitmask.
// This list may not up-to-date with https://golang.org/pkg/crypto/x509/#KeyUsage
func KeyUsageToString(ku x509.KeyUsage) []string {
	ret := make([]string, 0)
	if ku&x509.KeyUsageDigitalSignature != 0 {
		ret = append(ret, "DigitalSignature")
	}
	if ku&x509.KeyUsageContentCommitment != 0 {
		ret = append(ret, "ContentCommitment")
	}
	if ku&x509.KeyUsageKeyEncipherment != 0 {
		ret = append(ret, "KeyEncipherment")
	}
	if ku&x509.KeyUsageDataEncipherment != 0 {
		ret = append(ret, "DataEncirpherment")
	}
	if ku&x509.KeyUsageKeyAgreement != 0 {
		ret = append(ret, "KeyAgreement")
	}
	if ku&x509.KeyUsageCertSign != 0 {
		ret = append(ret, "CertSign")
	}
	if ku&x509.KeyUsageCRLSign != 0 {
		ret = append(ret, "CRLSign")
	}
	if ku&x509.KeyUsageEncipherOnly != 0 {
		ret = append(ret, "EncipherOnly")
	}
	if ku&x509.KeyUsageDecipherOnly != 0 {
		ret = append(ret, "DecipherOnly")
	}

	return ret
}

// ExtKeyUsageToString converts a x509.ExtKeyUsage to a string, returning "unknown" if
// the list is not up-to-date.
func ExtKeyUsageToString(eku x509.ExtKeyUsage) string {
	switch eku {

	case x509.ExtKeyUsageAny:
		return "Any"
	case x509.ExtKeyUsageServerAuth:
		return "ServerAuth"
	case x509.ExtKeyUsageClientAuth:
		return "ClientAuth"
	case x509.ExtKeyUsageCodeSigning:
		return "CodeSigning"
	case x509.ExtKeyUsageEmailProtection:
		return "EmailProtection"
	case x509.ExtKeyUsageIPSECEndSystem:
		return "IPSECEndSystem"
	case x509.ExtKeyUsageIPSECTunnel:
		return "IPSECTunnel"
	case x509.ExtKeyUsageIPSECUser:
		return "IPSECUser"
	case x509.ExtKeyUsageTimeStamping:
		return "TimeStamping"
	case x509.ExtKeyUsageOCSPSigning:
		return "OCSPSigning"
	case x509.ExtKeyUsageMicrosoftServerGatedCrypto:
		return "MicrosoftServerGatedCrypto"
	case x509.ExtKeyUsageNetscapeServerGatedCrypto:
		return "NetscapeServerGatedCrypto"
	default:
		return "unknown"
	}
}

// GenerateRDNSequenceFromSpecMap takes a list subject DN fields and
// corresponding values. It generates pkix.RDNSequence for these fields. The
// returned  sequence could then used to generate cert.Subject and
// cert.RawSubject for creating a mock crypto/x509 certificate object.
func GenerateRDNSequenceFromSpecMap(
	subjectSpecMap [][]string,
) (RDNSeq pkix.RDNSequence, err error) {
	var (
		oidCountry            = []int{2, 5, 4, 6}
		oidOrganization       = []int{2, 5, 4, 10}
		oidOrganizationalUnit = []int{2, 5, 4, 11}
		oidCommonName         = []int{2, 5, 4, 3}
		oidLocality           = []int{2, 5, 4, 7}
		oidProvince           = []int{2, 5, 4, 8}
		oidStreetAddress      = []int{2, 5, 4, 9}
		oidUID                = []int{0, 9, 2342, 19200300, 100, 1, 1}
		oidDC                 = []int{0, 9, 2342, 19200300, 100, 1, 25}
	)

	for _, fieldAndValue := range subjectSpecMap {
		field := fieldAndValue[0]
		fieldValue := fieldAndValue[1]
		var attrTypeAndValue pkix.AttributeTypeAndValue
		switch field {
		case "CN":
			attrTypeAndValue.Type = oidCommonName
		case "L":
			attrTypeAndValue.Type = oidLocality
		case "ST":
			attrTypeAndValue.Type = oidProvince
		case "O":
			attrTypeAndValue.Type = oidOrganization
		case "OU":
			attrTypeAndValue.Type = oidOrganizationalUnit
		case "C":
			attrTypeAndValue.Type = oidCountry
		case "STREET":
			attrTypeAndValue.Type = oidStreetAddress
		case "DC":
			attrTypeAndValue.Type = oidDC
		case "UID":
			attrTypeAndValue.Type = oidUID
		default:
			return nil, fmt.Errorf("found unknown field value %q in spec map", field)
		}
		attrTypeAndValue.Value = fieldValue
		RDNSeq = append(RDNSeq, pkix.RelativeDistinguishedNameSET{
			attrTypeAndValue,
		})
	}

	return RDNSeq, nil
}
