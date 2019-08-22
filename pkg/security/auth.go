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
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

const (
	// NodeUser is used by nodes for intra-cluster traffic.
	NodeUser = "node"
	// RootUser is the default cluster administrator.
	RootUser = "root"
)

// AuthContext describes the authentication settings for the cluster.
// These are based on command-line flags and do not change during the lifetime of a node.
type AuthContext struct {
	Insecure                        bool
	ClusterName                     string
	EnforceClusterNameInCertificate bool
}

// UserAuthHook authenticates a user based on their username and whether their
// connection originates from a client or another node in the cluster.
type UserAuthHook func(string, bool) error

// GetCertificateUser extracts the username from a client certificate.
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

// CheckCertificateClusterName checks whether the clustername is set in the peer certificate.
func CheckCertificateClusterName(authCtx AuthContext, tlsState *tls.ConnectionState) error {
	if tlsState == nil {
		return errors.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return errors.Errorf("no client certificates in request")
	}

	if !authCtx.EnforceClusterNameInCertificate {
		return nil
	}

	// The cluster name given to the node must match exactly one of the cluster names
	// in the peer certificate Subject.OrganizationUnit fields.
	// If one is set but not the other, this counts as a mismatch.
	certClusterNames := ClusterNamesFromList(tlsState.PeerCertificates[0].Subject.OrganizationalUnit)
	nodeHasClusterName := len(authCtx.ClusterName) > 0
	certHasClusterName := len(certClusterNames) > 0

	// --cluster-name not specified.
	if !nodeHasClusterName {
		if certHasClusterName {
			return fmt.Errorf("client certificate is valid for clusters [%s], but no --cluster-name set on node",
				strings.Join(certClusterNames, ","))
		}
		return nil
	}

	// certificate has no cluster names specified.
	if !certHasClusterName {
		if nodeHasClusterName {
			return fmt.Errorf("cluster name is %q, but client certificate does not specify any clusters",
				authCtx.ClusterName)
		}
		return nil
	}

	// --cluster-name is set and certificate has cluster names.
	for _, certName := range certClusterNames {
		if certName == authCtx.ClusterName {
			return nil
		}
	}

	return fmt.Errorf("cluster name is %q, but client certificate only allows clusters [%s]",
		authCtx.ClusterName, strings.Join(certClusterNames, ","))
}

// AuthenticateRPC verifies that the TLS state from an RPC context is allowed.
// authCtx.Insecure is not checked as this is only called in secure mode.
func AuthenticateRPC(authCtx AuthContext, tlsState *tls.ConnectionState) error {
	certUser, err := GetCertificateUser(tlsState)
	if err != nil {
		return err
	}

	// TODO(benesch): the vast majority of RPCs should be limited to just
	// NodeUser. This is not a security concern, as RootUser has access to
	// read and write all data, merely good hygiene. For example, there is
	// no reason to permit the root user to send raw Raft RPCs.
	if certUser != NodeUser && certUser != RootUser {
		return errors.Errorf("user %s is not allowed to perform this RPC", certUser)
	}

	return CheckCertificateClusterName(authCtx, tlsState)
}

// UserAuthCertHook builds an authentication hook based on the security
// mode and client certificate.
func UserAuthCertHook(authCtx AuthContext, tlsState *tls.ConnectionState) (UserAuthHook, error) {
	var certUser string

	if !authCtx.Insecure {
		var err error
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}

		if err := CheckCertificateClusterName(authCtx, tlsState); err != nil {
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
		if authCtx.Insecure {
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
func UserAuthPasswordHook(
	authCtx AuthContext, password string, hashedPassword []byte,
) UserAuthHook {
	return func(requestedUser string, clientConnection bool) error {
		if len(requestedUser) == 0 {
			return errors.New("user is missing")
		}

		if !clientConnection {
			return errors.New("password authentication is only available for client connections")
		}

		if authCtx.Insecure {
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
