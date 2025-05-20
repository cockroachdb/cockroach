// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisioning

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
)

// ValidateSource validates a provisioning source string set for the
// role option to verify that it is well-formed and contains the auth method
// prefixed IDP URI.
func ValidateSource(sourceStr string) (err error) {
	var idp string
	if _, idp, err = parseAuthMethod(sourceStr); err != nil {
		return err
	}
	if _, err = parseIDP(idp); err != nil {
		return err
	}
	return
}

type Source struct {
	authMethod string
	idp        *url.URL
}

// ParseProvisioningSource parses a provisioning source string. It must contain  the auth
// method prefixed IDP URI.
func ParseProvisioningSource(sourceStr string) (source *Source, err error) {
	var idp string
	if source.authMethod, idp, err = parseAuthMethod(sourceStr); err != nil {
		return nil, err
	}
	if source.idp, err = parseIDP(idp); err != nil {
		return nil, err
	}
	return
}

func parseAuthMethod(sourceStr string) (authMethod string, idp string, err error) {
	if strings.HasPrefix(sourceStr, "ldap:") {
		idp = strings.TrimPrefix(sourceStr, "ldap:")
		authMethod = "ldap"
	} else {
		return "", "", errors.Newf("PROVISIONING_SOURCE %q was not prefixed with valid HBA auth method %q", sourceStr, "ldap")
	}
	return
}

func parseIDP(idp string) (url *url.URL, err error) {
	if url, err = url.Parse(idp); err != nil {
		return nil, errors.Wrapf(err, "provided IDP %q in PROVISIONING_SOURCE is non parseable", idp)
	}
	return
}
