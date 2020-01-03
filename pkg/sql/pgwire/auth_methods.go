// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"bytes"
	"crypto/tls"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// This file contains the methods that are accepted to perform
// authentication of users during the pgwire connection handshake.
//
// Which method are accepted for which user is selected using
// the HBA config loaded into the cluster setting
// server.host_based_authentication.configuration.
//
// Other methods can be added using RegisterAuthMethod(). This is done
// e.g. in the CCL modules to add support for GSS authentication using
// Kerberos.

func init() {
	// The "password" method requires a clear text password.
	//
	// Care should be taken by administrators to only accept this auth
	// method over secure connections, e.g. those encrypted using SSL.
	RegisterAuthMethod("password", authPassword, nil)

	// The "cert" method requires a valid client certificate for the
	// user attempting to connect.
	//
	// This method is only usable over SSL connections.
	RegisterAuthMethod("cert", authCert, nil)

	// The "cert-password" method requires either a valid client
	// certificate for the connecting user, or, if no cert is provided,
	// a cleartext password.
	RegisterAuthMethod("cert-password", authCertPassword, nil)
}

// AuthMethod defines a method for authentication of a connection.
type AuthMethod func(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error)

func authPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
		return nil, err
	}
	pwdData, err := c.GetPwdData()
	if err != nil {
		return nil, err
	}
	password, err := passwordString(pwdData)
	if err != nil {
		return nil, err
	}
	return security.UserAuthPasswordHook(
		insecure, password, hashedPassword,
	), nil
}

func passwordString(pwdData []byte) (string, error) {
	// Make a string out of the byte array.
	if bytes.IndexByte(pwdData, 0) != len(pwdData)-1 {
		return "", fmt.Errorf("expected 0-terminated byte array")
	}
	return string(pwdData[:len(pwdData)-1]), nil
}

func authCert(
	_ AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.New("no TLS peer certificates, but required for auth")
	}
	// Normalize the username contained in the certificate.
	tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
		tlsState.PeerCertificates[0].Subject.CommonName,
	).Normalize()
	return security.UserAuthCertHook(insecure, &tlsState)
}

func authCertPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		fn = authPassword
	} else {
		fn = authCert
	}
	return fn(c, tlsState, insecure, hashedPassword, execCfg, entry)
}
