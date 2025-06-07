// Copyright 2025 The Cockroach Authors.
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

// Source is used to map the `PROVISIONING_SOURCE` role option to the
// appropriate authentication method used to perform provisioning and the IDP
// uri which serves as the source of the role. These are made available via
// sql.GetUserSessionInitInfo.
type Source struct {
	authMethod string
	idp        *url.URL
}

// ParseProvisioningSource parses a provisioning source string. It must contain  the auth
// method prefixed IDP URI.
func ParseProvisioningSource(sourceStr string) (source *Source, err error) {
	var idp string
	source = new(Source)
	if source.authMethod, idp, err = parseAuthMethod(sourceStr); err != nil {
		return nil, err
	}
	if source.idp, err = parseIDP(idp); err != nil {
		return nil, err
	}
	return
}

func parseAuthMethod(sourceStr string) (authMethod string, idp string, err error) {
	supportedProvisioningMethods := []string{"ldap"}
	for _, method := range supportedProvisioningMethods {
		prefix := method + ":"
		if strings.HasPrefix(sourceStr, prefix) {
			idp = strings.TrimPrefix(sourceStr, prefix)
			authMethod = method
		}
	}
	return "", "", errors.Newf("PROVISIONING_SOURCE %q was not prefixed with any valid HBA auth methods %q", sourceStr, supportedProvisioningMethods)
}

func parseIDP(idp string) (u *url.URL, err error) {
	if u, err = url.Parse(idp); err != nil {
		return nil, errors.Wrapf(err, "provided IDP %q in PROVISIONING_SOURCE is non parseable", idp)
	}
	return
}

func (source *Source) Size() int {
	return len(source.authMethod) + len(source.idp.String())
}
