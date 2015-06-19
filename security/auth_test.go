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

package security_test

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"testing"

	cockroach_proto "github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/gogo/protobuf/proto"
)

// Construct a fake tls.ConnectionState object with one peer certificate
// for each 'commonNames', and one chain of length 'chainLengths[i]' for each 'chainLengths'.
func makeFakeTLSState(commonNames []string, chainLengths []int) *tls.ConnectionState {
	tls := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{},
		VerifiedChains:   [][]*x509.Certificate{},
	}
	for _, name := range commonNames {
		tls.PeerCertificates = append(tls.PeerCertificates, &x509.Certificate{Subject: pkix.Name{CommonName: name}})
	}
	for i, length := range chainLengths {
		chain := []*x509.Certificate{}
		for j := 0; j < length; j++ {
			name := fmt.Sprintf("chain%d:%d", i, j)
			chain = append(chain, &x509.Certificate{Subject: pkix.Name{CommonName: name}})
		}
		tls.VerifiedChains = append(tls.VerifiedChains, chain)
	}
	return tls
}

func TestGetCertificateUser(t *testing.T) {
	// Nil TLS state.
	if _, err := security.GetCertificateUser(nil); err == nil {
		t.Error("unexpected success")
	}

	// No certificates.
	if _, err := security.GetCertificateUser(makeFakeTLSState(nil, nil)); err == nil {
		t.Error("unexpected success")
	}

	// len(certs) != len(chains)
	if _, err := security.GetCertificateUser(makeFakeTLSState([]string{"foo"}, []int{1, 1})); err == nil {
		t.Error("unexpected success")
	}

	// Good request: single certificate.
	if name, err := security.GetCertificateUser(makeFakeTLSState([]string{"foo"}, []int{2})); err != nil {
		t.Error(err)
	} else if name != "foo" {
		t.Errorf("expected name: foo, got: %s", name)
	}

	// Always use the first certificate.
	if name, err := security.GetCertificateUser(makeFakeTLSState([]string{"foo", "bar"}, []int{2, 1})); err != nil {
		t.Error(err)
	} else if name != "foo" {
		t.Errorf("expected name: foo, got: %s", name)
	}
}

// Build a proto request that implements GetUser.
func makeUserRequest(user string) proto.Message {
	get := &cockroach_proto.GetRequest{
		RequestHeader: cockroach_proto.RequestHeader{
			User: user,
		},
	}
	return get
}

func TestAuthenticationHook(t *testing.T) {
	// Proto that does not implement GetUser.
	badRequest := &cockroach_proto.GetResponse{}

	testCases := []struct {
		insecure         bool
		tls              *tls.ConnectionState
		request          proto.Message
		buildHookSuccess bool
		runHookSuccess   bool
	}{
		// Insecure mode, nil request.
		{true, nil, nil, true, false},
		// Insecure mode, bad request.
		{true, nil, badRequest, true, false},
		// Insecure mode, userRequest with empty user.
		{true, nil, makeUserRequest(""), true, false},
		// Insecure mode, userRequest with good user.
		{true, nil, makeUserRequest("foo"), true, true},
		// Secure mode, no TLS state.
		{false, nil, nil, false, false},
		// Secure mode, user mismatch.
		{false, makeFakeTLSState([]string{"foo"}, []int{1}), makeUserRequest("bar"), true, false},
		// Secure mode, user mismatch, but client certificate is for the node user.
		{false, makeFakeTLSState([]string{security.NodeUser}, []int{1}), makeUserRequest("bar"), true, true},
		// Secure mode, matchin users.
		{false, makeFakeTLSState([]string{"foo"}, []int{1}), makeUserRequest("foo"), true, true},
	}

	for tcNum, tc := range testCases {
		hook, err := security.AuthenticationHook(tc.insecure, tc.tls)
		if (err == nil) != tc.buildHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.buildHookSuccess, err)
		}
		if err != nil {
			continue
		}
		err = hook(tc.request)
		if (err == nil) != tc.runHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.runHookSuccess, err)
		}
	}
}
