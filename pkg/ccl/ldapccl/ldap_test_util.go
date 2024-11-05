// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

const (
	emptyParam   = "empty"
	invalidParam = "invalid"
)

type mockLDAPUtil struct {
	conn         *ldap.Conn
	tlsConfig    *tls.Config
	userGroupDNs map[string][]string
	connClosing  bool
}

var _ ILDAPUtil = &mockLDAPUtil{}

var LDAPMocks = func() (*mockLDAPUtil, func(context.Context, ldapConfig) (ILDAPUtil, error)) {
	var mLU = mockLDAPUtil{tlsConfig: &tls.Config{}, userGroupDNs: make(map[string][]string)}
	var newMockLDAPUtil = func(ctx context.Context, conf ldapConfig) (ILDAPUtil, error) {
		return &mLU, nil
	}
	return &mLU, newMockLDAPUtil
}

// MaybeInitLDAPsConn implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) MaybeInitLDAPsConn(ctx context.Context, conf ldapConfig) error {
	if strings.Contains(conf.ldapServer, invalidParam) {
		return errors.Newf(ldapsFailureMessage + ": invalid ldap server provided")
	} else if strings.Contains(conf.ldapPort, invalidParam) {
		return errors.Newf(ldapsFailureMessage + ": invalid ldap port provided")
	}
	if lu.conn != nil && !lu.connClosing {
		return nil
	}
	lu.conn = ldap.NewConn(nil, true)
	return nil
}

// resetLDAPsConn mocks server behavior for sending ECONNRESET in case of
// prolonged connection idleness.
// ref: https://github.com/cockroachdb/cockroach/issues/133777
func (lu *mockLDAPUtil) resetLDAPsConn() {
	lu.connClosing = true
}

// getLDAPsConn returns the current ldap conn set for the ldap util.
func (lu *mockLDAPUtil) getLDAPsConn() *ldap.Conn {
	return lu.conn
}

// Bind implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) Bind(ctx context.Context, userDN string, ldapPwd string) error {
	if strings.Contains(userDN, invalidParam) {
		return errors.Newf(bindFailureMessage + ": invalid username provided")
	} else if strings.Contains(ldapPwd, invalidParam) {
		return errors.Newf(bindFailureMessage + ": invalid password provided")
	}

	return nil
}

// Search implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) Search(
	ctx context.Context, conf ldapConfig, username string,
) (userDN string, err error) {
	if strings.Contains(conf.ldapBaseDN, invalidParam) {
		return "", errors.Newf(searchFailureMessage+": invalid base DN %q provided", conf.ldapBaseDN)
	}
	if strings.Contains(conf.ldapSearchFilter, invalidParam) {
		return "", errors.Newf(searchFailureMessage+": invalid search filter %q provided", conf.ldapSearchFilter)
	}
	if strings.Contains(conf.ldapSearchAttribute, invalidParam) {
		return "", errors.Newf(searchFailureMessage+": invalid search attribute %q provided", conf.ldapSearchAttribute)
	}
	if strings.Contains(username, invalidParam) {
		return "", errors.Newf(searchFailureMessage+": invalid search value %q provided", username)
	}
	commonNames := strings.Split(username, ",")
	switch {
	case len(username) == 0:
		return "", errors.Newf(searchFailureMessage+": user %q does not exist", username)
	case len(commonNames) > 1:
		return "", errors.Newf(searchFailureMessage+": too many matching entries returned for user %q", username)
	}

	return lu.GetLdapDN(commonNames[0]), nil
}

// GetLdapDN returns the LDAP DN for a sql user for testing purposes.
func (lu *mockLDAPUtil) GetLdapDN(user string) string {
	return "cn=" + user
}

// SetGroups overrides the return value of ListGroups for an LDAP userDN for
// testing purposes.
func (lu *mockLDAPUtil) SetGroups(userDN string, groupsDN []string) {
	lu.userGroupDNs[userDN] = groupsDN
}

// ListGroups implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) ListGroups(
	ctx context.Context, conf ldapConfig, userDN string,
) (ldapGroupsDN []string, err error) {
	if strings.Contains(conf.ldapBaseDN, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid base DN %q provided", conf.ldapBaseDN)
	}
	if strings.Contains(conf.ldapSearchFilter, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid search filter %q provided", conf.ldapSearchFilter)
	}
	if strings.Contains(conf.ldapGroupListFilter, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid group list filter %q provided", conf.ldapGroupListFilter)
	}
	if strings.Contains(userDN, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid user DN %q provided", userDN)
	}

	if len(userDN) == 0 {
		return nil, errors.Newf(groupListFailureMessage+": user dn %q does not belong to any groups", userDN)
	}

	return lu.userGroupDNs[userDN], nil
}

func constructHBAEntry(
	t *testing.T,
	hbaEntryBase string,
	hbaConfLDAPDefaultOpts map[string]string,
	hbaConfLDAPOpts map[string]string,
) hba.Entry {
	hbaEntryLDAP := hbaEntryBase
	// add options from default and override default options when provided with one
	for opt, value := range hbaConfLDAPDefaultOpts {
		setValue := value
		if hbaConfLDAPOpts[opt] == emptyParam {
			continue
		} else if hbaConfLDAPOpts[opt] != "" {
			setValue = hbaConfLDAPOpts[opt]
		}
		hbaEntryLDAP += fmt.Sprintf("\"%s=%s\" ", opt, setValue)
	}
	// add non default options
	for additionalOpt, additionalOptValue := range hbaConfLDAPOpts {
		if _, ok := hbaConfLDAPDefaultOpts[additionalOpt]; !ok {
			hbaEntryLDAP += fmt.Sprintf("\"%s=%s\" ", additionalOpt, additionalOptValue)
		}
	}
	hbaConf, err := hba.ParseAndNormalize(hbaEntryLDAP)
	if err != nil {
		t.Fatalf("error parsing hba conf: %v", err)
	}
	if len(hbaConf.Entries) != 1 {
		t.Fatalf("hba conf value invalid: should contain only 1 entry")
	}
	return hbaConf.Entries[0]
}
