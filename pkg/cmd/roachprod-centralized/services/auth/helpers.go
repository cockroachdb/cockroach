// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"fmt"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
)

// getPrincipalIdentifier returns a non-PII identifier for the principal.
// Used in audit logging.
func getPrincipalIdentifier(principal *pkgauth.Principal) string {
	if principal == nil {
		return "system"
	}
	if principal.UserID != nil {
		return "user:" + principal.UserID.String()
	}
	if principal.ServiceAccountID != nil {
		identifier := "sa:" + principal.ServiceAccountID.String()
		if principal.DelegatedFrom != nil {
			identifier += fmt.Sprintf("(delegatedFrom user:%s)", principal.DelegatedFrom.String())
		}
		return identifier
	}
	return "unknown"
}
