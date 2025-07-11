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
func ValidateSource(sourceStr string) error {
	_, idp, err := parseAuthMethod(sourceStr)
	if err != nil {
		return err
	}
	_, err = parseIDP(idp)
	return err
}

// Source is used to map the `PROVISIONSRC` role option to the
// appropriate authentication method used to perform provisioning and the IDP
// uri which serves as the source of the role. These are made available via
// sql.GetUserSessionInitInfo.
type Source struct {
	authMethod string
	idp        *url.URL
}

// ParseProvisioningSource parses a provisioning source string. It must contain  the auth
// method prefixed IDP URI.
func ParseProvisioningSource(sourceStr string) (*Source, error) {
	authMethod, idpStr, err := parseAuthMethod(sourceStr)
	if err != nil {
		return nil, err
	}
	idpURL, err := parseIDP(idpStr)
	if err != nil {
		return nil, err
	}
	return &Source{
		authMethod: authMethod,
		idp:        idpURL,
	}, nil
}

func parseAuthMethod(sourceStr string) (authMethod string, idp string, err error) {
	supportedProvisioningMethods := []string{supportedAuthMethodLDAP}
	for _, method := range supportedProvisioningMethods {
		prefix := method + ":"
		if strings.HasPrefix(sourceStr, prefix) {
			idp = strings.TrimPrefix(sourceStr, prefix)
			authMethod = method
			return
		}
	}
	return "", "", errors.Newf("PROVISIONSRC %q was not prefixed with any valid auth methods %q", sourceStr, supportedProvisioningMethods)
}

func parseIDP(idp string) (u *url.URL, err error) {
	if len(idp) == 0 {
		return nil, errors.Newf("PROVISIONSRC IDP cannot be empty")
	}
	if u, err = url.Parse(idp); err != nil {
		return nil, errors.Wrapf(err, "provided IDP %q in PROVISIONSRC is non parseable", idp)
	}
	if len(u.Port()) != 0 || u.Opaque != "" {
		return nil, errors.Newf("unknown PROVISIONSRC IDP url format in %q", idp)
	}
	return
}

func (source *Source) Size() int {
	return len(source.authMethod) + len(source.idp.String())
}

func (source *Source) String() string {
	return source.authMethod + ":" + source.idp.String()
}
