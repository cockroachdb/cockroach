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
	"fmt"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
)

const (
	// NodeUser is used by nodes for intra-cluster traffic.
	NodeUser = "node"
	// RootUser is the default cluster administrator.
	RootUser = "root"
)

// LogRequestCertificates examines a http request and logs a summary of the TLS config.
func LogRequestCertificates(r *http.Request) {
	LogTLSState(fmt.Sprintf("%s %s", r.Method, r.URL), r.TLS)
}

// LogTLSState logs information about TLS state in the form:
// "<method>: perr certs: [<Subject.CommonName>...], chain: [[<CommonName>...][..]]"
func LogTLSState(method string, tlsState *tls.ConnectionState) {
	if tlsState == nil {
		if log.V(3) {
			log.Infof("%s: no TLS", method)
		}
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
	if log.V(3) {
		log.Infof("%s: peer certs: %v, chain: %v", method, peerCerts, verifiedChain)
	}
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

// AuthenticationHook builds an authentication hook based on the
// security mode and client certificate.
// Must be called at connection time and passed the TLS state.
// Returns a func(proto.Message) error. The passed-in proto must implement
// the GetUser interface.
func AuthenticationHook(insecureMode bool, tlsState *tls.ConnectionState) (
	func(request proto.Message) error, error) {
	var certUser string
	var err error

	if !insecureMode {
		certUser, err = GetCertificateUser(tlsState)
		if err != nil {
			return nil, err
		}
	}

	return func(request proto.Message) error {
		// userRequest is an interface for RPC requests that have a "requested user".
		type userRequest interface {
			// GetUser returns the user from the request.
			GetUser() string
		}

		// UserRequest must be implemented.
		requestWithUser, ok := request.(userRequest)
		if !ok {
			return util.Errorf("unknown request type: %T", request)
		}

		// Extract user and verify.
		// TODO(marc): we may eventually need stricter user syntax rules.
		requestedUser := requestWithUser.GetUser()
		if len(requestedUser) == 0 {
			return util.Errorf("missing User in request: %+v", request)
		}

		// If running in insecure mode, we have nothing to verify it against.
		if insecureMode {
			return nil
		}

		// The client certificate user must either be "node", or match the requested used.
		if certUser == NodeUser || certUser == requestedUser {
			return nil
		}
		return util.Errorf("requested user is %s, but certificate is for %s", requestedUser, certUser)
	}, nil
}
