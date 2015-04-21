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
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
)

// This is just the mechanics of certs generation.
func TestGenerateCerts(t *testing.T) {
	certsDir := util.CreateTempDir(t, "certs_test")
	defer util.CleanupDir(certsDir)

	context := server.NewContext()

	context.Certs = ""
	// Try certs generation with empty Certs dir argument.
	err := RunMakeCACert(context)
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}
	err = RunMakeNodeCert(context, []string{"localhost"})
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}

	context.Certs = certsDir
	// Try generating node certs without CA certs present.
	err = RunMakeNodeCert(context, []string{"localhost"})
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Now try in the proper order.
	err = RunMakeCACert(context)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	err = RunMakeNodeCert(context, []string{"localhost"})
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

// This is a fairly high-level test of CA and node certificates.
// We construct SSL server and clients and use the generated certs.
func TestUseCerts(t *testing.T) {
	certsDir := util.CreateTempDir(t, "certs_test")
	defer util.CleanupDir(certsDir)

	context := server.NewContext()
	context.Certs = certsDir
	err := RunMakeCACert(context)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	err = RunMakeNodeCert(context, []string{"127.0.0.1"})
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Load TLS Configs. This is what TestServer and HTTPClient do internally.
	_, err = rpc.LoadTLSConfigFromDir(certsDir)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	_, err = rpc.LoadClientTLSConfigFromDir(certsDir)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Start a test server with the just-generated certs.
	s := &server.TestServer{CertDir: certsDir, Addr: "127.0.0.1:0"}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	// Try a client without certs and without InsecureSkipVerify.
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: nil}}
	req, err := http.NewRequest("GET", "https://"+s.Addr+"/_admin/health", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatalf("Expected SSL error, got success")
	}

	// New client. With certs this time.
	httpClient, err = client.NewHTTPClient(certsDir /* with certs */)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	req, err = http.NewRequest("GET", "https://"+s.Addr+"/_admin/health", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	defer resp.Body.Close()
}
