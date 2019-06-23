// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"crypto/tls"

	"github.com/pkg/errors"
)

const (
	// NodeUser is used by nodes for intra-cluster traffic.
	NodeUser = "node"
	// RootUser is the default cluster administrator.
	RootUser = "root"
)

// UserAuthHook authenticates a user based on their username and whether their
// connection originates from a client or another node in the cluster.
type UserAuthHook func(string, bool) error

// GetCertificateUser extract the username from a client certificate.
func GetCertificateUser(tlsState *tls.ConnectionState) (string, error) {
	if tlsState == nil {
		return "", errors.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return "", errors.Errorf("no client certificates in request")
	}
	// The go server handshake code verifies the first certificate, using
	// any following certificates as intermediates. See:
	// https://github.com/golang/go/blob/go1.8.1/src/crypto/tls/handshake_server.go#L723:L742
	return tlsState.PeerCertificates[0].Subject.CommonName, nil
}

// UserAuthCertHook builds an authentication hook based on the security
// mode and client certificate.
func UserAuthCertHook(insecureMode bool, tlsState *tls.ConnectionState) (UserAuthHook, error) {
	var certUser string

	if !insecureMode {
		var err error
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(requestedUser string, clientConnection bool) error {
		// TODO(marc): we may eventually need stricter user syntax rules.
		if len(requestedUser) == 0 {
			return errors.New("user is missing")
		}

		if !clientConnection && requestedUser != NodeUser {
			return errors.Errorf("user %s is not allowed", requestedUser)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate user must match the requested user,
		// except if the certificate user is NodeUser, which is allowed to
		// act on behalf of all other users.
		if !(certUser == NodeUser || certUser == requestedUser) {
			return errors.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
		}

		return nil
	}, nil
}

// UserAuthPasswordHook builds an authentication hook based on the security
// mode, password, and its potentially matching hash.
func UserAuthPasswordHook(insecureMode bool, password string, hashedPassword []byte) UserAuthHook {
	return func(requestedUser string, clientConnection bool) error {
		if len(requestedUser) == 0 {
			return errors.New("user is missing")
		}

		if !clientConnection {
			return errors.New("password authentication is only available for client connections")
		}

		if insecureMode {
			return nil
		}

		if requestedUser == RootUser {
			return errors.Errorf("user %s must use certificate authentication instead of password authentication", RootUser)
		}

		// If the requested user has an empty password, disallow authentication.
		if len(password) == 0 || CompareHashAndPassword(hashedPassword, password) != nil {
			return errors.Errorf(ErrPasswordUserAuthFailed, requestedUser)
		}

		return nil
	}
}

// ErrPasswordUserAuthFailed is the error template for failed password auth
// of a user. It should be used when the password is incorrect or the user
// does not exist.
const ErrPasswordUserAuthFailed = "password authentication failed for user %s"
