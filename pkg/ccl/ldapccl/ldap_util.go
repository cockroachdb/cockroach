// Copyright 2022 The Cockroach Authors.
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
	"crypto/x509"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

type ldapUtil struct {
	con       *ldap.Conn
	tlsConfig *tls.Config
}

func (lu *ldapUtil) LDAPSConn(ctx context.Context, conf ldapAuthenticatorConf) error {
	// ldapAddress := "ldap://ldap.example.com:636"
	ldapAddress := conf.ldapServer + ":" + conf.ldapPort
	con, err := ldap.DialTLS("tcp", ldapAddress, lu.tlsConfig)
	if err != nil {
		return err
	}

	lu.con = con

	return nil
}

func (lu *ldapUtil) Bind(ctx context.Context, username string, password string) error {
	err := lu.con.Bind(username, password)
	if err != nil {
		return err
	}
	return nil
}

func (lu *ldapUtil) Search(
	ctx context.Context, conf ldapAuthenticatorConf, username string,
) (distinguishedName string, err error) {
	if err := lu.Bind(ctx, conf.ldapBindDN, conf.ldapBindPassword); err != nil {
		return "", err
	}

	searchRequest := ldap.NewSearchRequest(
		conf.ldapBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&(%s)(%s=%s))", conf.ldapSearchFilter, conf.ldapSearchAttribute, ldap.EscapeFilter(username)),
		[]string{"dn"},
		nil,
	)

	sr, err := lu.con.Search(searchRequest)
	if err != nil {
		return "", err
	}

	if len(sr.Entries) != 1 {
		return "", errors.Newf("user does not exist or too many entries returned")
	}

	return sr.Entries[0].DN, nil
}

type ILDAPUtil interface {
	LDAPSConn(context.Context, ldapAuthenticatorConf) error
	Bind(context.Context, string, string) error
	Search(context.Context, ldapAuthenticatorConf, string) (string, error)
}

var _ ILDAPUtil = &ldapUtil{}

var NewLDAPUtil func(context.Context, ldapAuthenticatorConf) (ILDAPUtil, error) = func(
	ctx context.Context,
	conf ldapAuthenticatorConf,
) (ILDAPUtil, error) {
	util := ldapUtil{}
	if conf.domainCACert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(conf.domainCACert))
		util.tlsConfig.RootCAs = caCertPool
	}

	return &util, nil
}
