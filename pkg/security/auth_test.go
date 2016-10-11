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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security_test

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
	defer leaktest.AfterTest(t)()
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

func TestAuthenticationHook(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Proto that does not implement GetUser.
	badRequest := &roachpb.GetResponse{}
	goodRequest := &roachpb.BatchRequest{}

	testCases := []struct {
		insecure           bool
		tls                *tls.ConnectionState
		request            proto.Message
		buildHookSuccess   bool
		publicHookSuccess  bool
		privateHookSuccess bool
	}{
		// Insecure mode, nil request.
		{true, nil, nil, true, false, false},
		// Insecure mode, bad request.
		{true, nil, badRequest, true, false, false},
		// Insecure mode, good request.
		{true, nil, goodRequest, true, true, true},
		// Secure mode, no TLS state.
		{false, nil, nil, false, false, false},
		// Secure mode, bad user.
		{false, makeFakeTLSState([]string{"foo"}, []int{1}), goodRequest, true, false, false},
		// Secure mode, node user.
		{false, makeFakeTLSState([]string{security.NodeUser}, []int{1}), goodRequest, true, true, true},
		// Secure mode, root user.
		{false, makeFakeTLSState([]string{security.RootUser}, []int{1}), goodRequest, true, false, false},
	}

	for tcNum, tc := range testCases {
		hook, err := security.ProtoAuthHook(tc.insecure, tc.tls)
		if (err == nil) != tc.buildHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.buildHookSuccess, err)
		}
		if err != nil {
			continue
		}
		err = hook(tc.request, true /*public*/)
		if (err == nil) != tc.publicHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.publicHookSuccess, err)
		}
		err = hook(tc.request, false /*not public*/)
		if (err == nil) != tc.privateHookSuccess {
			t.Fatalf("#%d: expected success=%t, got err=%v", tcNum, tc.privateHookSuccess, err)
		}
	}
}
