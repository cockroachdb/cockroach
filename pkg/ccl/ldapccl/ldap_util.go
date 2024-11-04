// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/go-ldap/ldap/v3"
)

const (
	invalidLDAPConfMessage  = "LDAP configuration invalid"
	ldapsFailureMessage     = "LDAPs connection failed"
	bindFailureMessage      = "LDAP bind failed"
	searchFailureMessage    = "LDAP search failed"
	groupListFailureMessage = "LDAP groups list failed"
)

type ldapUtil struct {
	conn      *ldap.Conn
	tlsConfig *tls.Config
}

// MaybeInitLDAPsConn implements the ILDAPUtil interface.
func (lu *ldapUtil) MaybeInitLDAPsConn(ctx context.Context, conf ldapConfig) (err error) {
	// TODO(souravcrl): (Fix 1) DialTLS is slow if we do it for every authN
	// attempt. We should look into ways for caching connections and avoiding
	// connection timeouts in case LDAP server enforces that for idle connections.
	// We still should be able to validate a large number of authN requests
	// reusing the same connection(s).
	// (Fix 2) Every authN attempt acquires a lock on ldapAuthManager, so
	// only 1 authN attempt is possible at a given time(for entire flow of
	// bind+search+bind). We should have a permanent bind connection to search for
	// entries and short-lived bind attempts for requested sql authNs.
	// (Fix 3) Every CRDB node currently looks into establishing a separate
	// connection with LDAP servers significantly increasing total number of open
	// connections. This should be capped and configurable as to how many
	// connections crdb nodes can take up(either in total or on a per node basis)
	//
	// ldapAddress := "ldap://ldap.example.com:636"
	// If the connection is idle for sometime, we get a ERRCONNRESET error from
	// server, the ldap client sets the connection to closing. We need to dial for
	// a new connection to continue using the client.
	if lu.conn != nil && !lu.conn.IsClosing() {
		return nil
	}
	ldapAddress := conf.ldapServer + ":" + conf.ldapPort
	if lu.conn, err = ldap.DialTLS("tcp", ldapAddress, lu.tlsConfig); err != nil {
		return errors.Wrap(err, ldapsFailureMessage)
	}
	return nil
}

// Bind implements the ILDAPUtil interface.
func (lu *ldapUtil) Bind(ctx context.Context, userDN string, ldapPwd string) (err error) {
	if err = lu.conn.Bind(userDN, ldapPwd); err != nil {
		return errors.Wrap(err, bindFailureMessage)
	}
	return nil
}

// Search implements the ILDAPUtil interface.
func (lu *ldapUtil) Search(
	ctx context.Context, conf ldapConfig, username string,
) (userDN string, err error) {
	// TODO(souravcrl): Currently search could be performed at subtree level but
	// this should be configurable through HBA conf using any of the scopes
	// provided: https://github.com/go-ldap/ldap/blob/master/search.go#L17-L24
	searchRequest := ldap.NewSearchRequest(
		conf.ldapBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&%s(%s=%s))", conf.ldapSearchFilter, conf.ldapSearchAttribute, ldap.EscapeFilter(username)),
		[]string{},
		nil,
	)
	sr, err := lu.conn.Search(searchRequest)
	if err != nil {
		return "", errors.Wrap(err, searchFailureMessage)
	}

	switch {
	case len(sr.Entries) == 0:
		return "", errors.Newf(searchFailureMessage+": user %s does not exist", username)
	case len(sr.Entries) > 1:
		return "", errors.Newf(searchFailureMessage+": too many matching entries returned for user %s", username)
	}

	return sr.Entries[0].DN, nil
}

// ListGroups implements the ILDAPUtil interface.
func (lu *ldapUtil) ListGroups(
	ctx context.Context, conf ldapConfig, userDN string,
) (_ []string, err error) {
	// TODO(souravcrl): Currently list groups can only be performed at subtree
	// level but this should be configurable through HBA conf using any of the
	// scopes provided:
	// https://github.com/go-ldap/ldap/blob/master/search.go#L17-L24
	searchRequest := ldap.NewSearchRequest(
		conf.ldapBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&%s(member=%s))", conf.ldapGroupListFilter, ldap.EscapeFilter(userDN)),
		[]string{},
		nil,
	)
	sr, err := lu.conn.Search(searchRequest)
	if err != nil {
		return nil, errors.Wrap(err, groupListFailureMessage)
	}
	if len(sr.Entries) == 0 {
		return nil, errors.Newf(groupListFailureMessage+": user dn %s does not belong to any groups", userDN)
	}

	ldapGroupsDN := make([]string, len(sr.Entries))
	for idx := range sr.Entries {
		ldapGroupsDN[idx] = sr.Entries[idx].DN
	}
	return ldapGroupsDN, nil
}

// ILDAPUtil is an interface for the `ldapauthccl` library to wrap various LDAP
// functionalities exposed by `go-ldap` library as part of CRDB modules for
// authN and authZ.
type ILDAPUtil interface {
	// MaybeInitLDAPsConn optionally creates a mTLS connection with the LDAP
	// server if it does not already exist taking arguments for domain CA, ldap
	// client key and cert, ldap server & port
	MaybeInitLDAPsConn(ctx context.Context, conf ldapConfig) error
	// Bind performs a bind given a valid DN and LDAP password
	Bind(ctx context.Context, userDN string, ldapPwd string) error
	// Search performs search on LDAP server binding with bindDN and bindpwd
	// expecting search arguments from HBA conf and crdb database connection
	// string and returns the ldap userDN.
	Search(ctx context.Context, conf ldapConfig, username string) (userDN string, err error)
	// ListGroups performs search on AD subtree starting from baseDN filtered by
	// groupListFilter and lists groups which have provided userDN as a member
	ListGroups(ctx context.Context, conf ldapConfig, userDN string) (ldapGroupsDN []string, err error)
}

var _ ILDAPUtil = &ldapUtil{}

// NewLDAPUtil initializes ldapUtil which implements the ILDAPUtil wrapper for
// client interface provided by `go-ldap`. This is needed for testing (to
// intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil which has
// mock implementations for ILDAPUtil interface).
var NewLDAPUtil func(context.Context, ldapConfig) (ILDAPUtil, error) = func(
	ctx context.Context,
	conf ldapConfig,
) (ILDAPUtil, error) {
	util := ldapUtil{tlsConfig: &tls.Config{}}

	if conf.domainCACert != "" {
		util.tlsConfig.RootCAs = x509.NewCertPool()
		if ok := util.tlsConfig.RootCAs.AppendCertsFromPEM([]byte(conf.domainCACert)); !ok {
			return nil, errors.Newf(invalidLDAPConfMessage + ": set domain CA cert for ldap server is not valid")
		}
	}

	if conf.clientTLSCert != "" && conf.clientTLSKey != "" {
		clientCert, err := tls.X509KeyPair([]byte(conf.clientTLSCert), []byte(conf.clientTLSKey))
		if err != nil {
			return nil, errors.Wrap(err, invalidLDAPConfMessage+": error parsing client cert and key pair for mTLS")
		}
		util.tlsConfig.Certificates = []tls.Certificate{clientCert}
	} else if conf.clientTLSCert != "" || conf.clientTLSKey != "" {
		return nil, errors.Newf(invalidLDAPConfMessage + ": both client cert and key pair must be set for mTLS")
	}

	return &util, nil
}
