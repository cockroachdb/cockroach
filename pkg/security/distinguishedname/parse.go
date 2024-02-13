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
	sawCN := false
	for _, rdn := range dn.RDNs {
		for _, attr := range rdn.Attributes {
			if attr.Type == "CN" {
				if sawCN {
					return errors.Newf("SUBJECT must have only one CN attribute")
				}
				sawCN = true
				normalizedCN, err := username.MakeSQLUsernameFromUserInput(attr.Value, username.PurposeValidation)
				if err != nil {
					return err
				}
				if normalizedCN != u {
					return errors.Newf("SUBJECT CN must match %q but got %q", u, attr.Value)
				}
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
