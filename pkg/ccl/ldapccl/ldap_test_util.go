// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	conn      *ldap.Conn
	tlsConfig *tls.Config
}

// MaybeInitLDAPsConn implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) MaybeInitLDAPsConn(ctx context.Context, conf ldapConfig) error {
	if strings.Contains(conf.ldapServer, invalidParam) {
		return errors.Newf(ldapsFailureMessage + ": invalid ldap server provided")
	} else if strings.Contains(conf.ldapPort, invalidParam) {
		return errors.Newf(ldapsFailureMessage + ": invalid ldap port provided")
	}
	lu.conn = &ldap.Conn{}
	return nil
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
	if err := lu.Bind(ctx, conf.ldapBindDN, conf.ldapBindPassword); err != nil {
		return "", errors.Wrap(err, searchFailureMessage)
	}
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
	distinguishedNames := strings.Split(username, ",")
	switch {
	case len(username) == 0:
		return "", errors.Newf(searchFailureMessage+": user %q does not exist", username)
	case len(distinguishedNames) > 1:
		return "", errors.Newf(searchFailureMessage+": too many matching entries returned for user %q", username)
	}
	return distinguishedNames[0], nil
}

// ListGroups implements the ILDAPUtil interface.
func (lu *mockLDAPUtil) ListGroups(
	ctx context.Context, conf ldapConfig, userDN string,
) (ldapGroupsDN []string, err error) {
	if err := lu.Bind(ctx, conf.ldapBindDN, conf.ldapBindPassword); err != nil {
		return nil, errors.Wrap(err, groupListFailureMessage)
	}
	if strings.Contains(conf.ldapBaseDN, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid base DN %q provided", conf.ldapBaseDN)
	}
	if strings.Contains(conf.ldapSearchFilter, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid search filter %q provided", conf.ldapSearchFilter)
	}
	if strings.Contains(conf.ldapGroupListFilter, invalidParam) {
		return nil, errors.Newf(groupListFailureMessage+": invalid group list filter %q provided", conf.ldapGroupListFilter)
	}
	if len(userDN) == 0 {
		return nil, errors.Newf(groupListFailureMessage+": user dn %q does not belong to any groups", userDN)
	}

	ldapGroupsDN = strings.Split(userDN, ",")
	return ldapGroupsDN, nil
}

var _ ILDAPUtil = &mockLDAPUtil{}

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
