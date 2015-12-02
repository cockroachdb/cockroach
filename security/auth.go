// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"crypto/tls"
	"log"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

const (
	// NodeUser is used by nodes for intra-cluster traffic.
	NodeUser = "node"
	// RootUser is the default cluster administrator.
	RootUser = "root"
)

// LogTLSState logs information about TLS state in the form:
// "<method>: peer certs: [<Subject.CommonName>...], chain: [[<CommonName>...][..]]"
func LogTLSState(method string, tlsState *tls.ConnectionState) {
	if tlsState == nil {
		log.Printf("%s: no TLS\n", method)
		return
	}

	peerCerts := []string{}
	verifiedChain := []string{}
	for _, cert := range tlsState.PeerCertificates {
		peerCerts = append(peerCerts, cert.Subject.CommonName)
	}
	for _, chain := range tlsState.VerifiedChains {
		subjects := []string{}
		for _, cert := range chain {
			subjects = append(subjects, cert.Subject.CommonName)
		}
		verifiedChain = append(verifiedChain, strings.Join(subjects, ","))
	}
	log.Printf("%s: peer certs: %v, chain: %v\n", method, peerCerts, verifiedChain)
}

// GetCertificateUser extract the username from a client certificate.
func GetCertificateUser(tlsState *tls.ConnectionState) (string, error) {
	if tlsState == nil {
		return "", util.Errorf("request is not using TLS")
	}
	if len(tlsState.PeerCertificates) == 0 {
		return "", util.Errorf("no client certificates in request")
	}
	if len(tlsState.VerifiedChains) != len(tlsState.PeerCertificates) {
		// TODO(marc): can this happen? Should we require exactly one?
		return "", util.Errorf("client cerficates not verified")
	}
	return tlsState.PeerCertificates[0].Subject.CommonName, nil
}

// RequestWithUser must be implemented by `roachpb.Request`s which are
// arguments to methods that are not permitted to skip user checks.
type RequestWithUser interface {
	GetUser() string
}

// ProtoAuthHook builds an authentication hook based on the security
// mode and client certificate.
// The proto.Message passed to the hook must implement RequestWithUser.
func ProtoAuthHook(insecureMode bool, tlsState *tls.ConnectionState) (
	func(proto.Message, bool) error, error) {
	userHook, err := UserAuthHook(insecureMode, tlsState)
	if err != nil {
		return nil, err
	}

	return func(request proto.Message, public bool) error {
		// RequestWithUser must be implemented.
		requestWithUser, ok := request.(RequestWithUser)
		if !ok {
			return util.Errorf("unknown request type: %T", request)
		}

		if err := userHook(requestWithUser.GetUser(), public); err != nil {
			return util.Errorf("%s error in request: %s", err, request)
		}
		return nil
	}, nil
}

// UserAuthHook builds an authentication hook based on the security
// mode and client certificate.
func UserAuthHook(insecureMode bool, tlsState *tls.ConnectionState) (
	func(string, bool) error, error) {
	var certUser string

	if !insecureMode {
		var err error
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(requestedUser string, public bool) error {
		// TODO(marc): we may eventually need stricter user syntax rules.
		if len(requestedUser) == 0 {
			return util.Errorf("user is missing")
		}

		if !public && requestedUser != NodeUser {
			return util.Errorf("user %s is not allowed", requestedUser)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate user must match the requested user,
		// except if the certificate user is NodeUser, which is allowed to
		// act on behalf of all other users.
		if !(certUser == NodeUser || certUser == requestedUser) {
			return util.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
		}

		return nil
	}, nil
}
