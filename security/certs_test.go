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
	"net/http"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// This is just the mechanics of certs generation.
func TestGenerateCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Do not mock cert access for this test.
	security.ResetReadFileFn()
	defer ResetTest()

	certsDir := util.CreateTempDir(t, "certs_test")
	defer util.CleanupDir(certsDir)

	// Try certs generation with empty Certs dir argument.
	err := security.RunCreateCACert("", "", 512)
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}
	err = security.RunCreateNodeCert(
		"", "", "", "",
		512, []string{"localhost"})
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Try generating node certs without CA certs present.
	err = security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"localhost"})
	if err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Now try in the proper order.
	err = security.RunCreateCACert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		512)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	err = security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"localhost"})
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

// This is a fairly high-level test of CA and node certificates.
// We construct SSL server and clients and use the generated certs.
func TestUseCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Do not mock cert access for this test.
	security.ResetReadFileFn()
	defer ResetTest()
	certsDir := util.CreateTempDir(t, "certs_test")
	defer util.CleanupDir(certsDir)

	err := security.RunCreateCACert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		512)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	err = security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"127.0.0.1"})
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	err = security.RunCreateClientCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedRootCert),
		filepath.Join(certsDir, security.EmbeddedRootKey),
		512, security.RootUser)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Load TLS Configs. This is what TestServer and HTTPClient do internally.
	_, err = security.LoadServerTLSConfig(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey))
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	_, err = security.LoadClientTLSConfig(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey))
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Start a test server and override certs.
	// We use a real context since we want generated certs.
	testCtx := server.NewContext()
	testCtx.Insecure = false
	testCtx.SSLCA = filepath.Join(certsDir, security.EmbeddedCACert)
	testCtx.SSLCert = filepath.Join(certsDir, security.EmbeddedNodeCert)
	testCtx.SSLCertKey = filepath.Join(certsDir, security.EmbeddedNodeKey)
	testCtx.User = security.NodeUser
	testCtx.Addr = "127.0.0.1:0"
	testCtx.HTTPAddr = "127.0.0.1:0"
	s := &server.TestServer{Ctx: testCtx}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	// Insecure mode.
	clientContext := testutils.NewNodeTestBaseContext()
	clientContext.Insecure = true
	httpClient, err := clientContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("GET", testCtx.HTTPRequestScheme()+"://"+s.HTTPAddr()+"/_admin/v1/health", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatalf("Expected SSL error, got success")
	}

	// Secure mode but no Certs: permissive config.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.Insecure = false
	clientContext.SSLCert = ""
	httpClient, err = clientContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	// Endpoint that does not enforce client auth (see: server/authentication_test.go)
	req, err = http.NewRequest("GET", testCtx.HTTPRequestScheme()+"://"+s.HTTPAddr()+"/_admin/v1/health", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected OK, got: %d", resp.StatusCode)
	}

	// New client. With certs this time.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCA = filepath.Join(certsDir, security.EmbeddedCACert)
	clientContext.SSLCert = filepath.Join(certsDir, security.EmbeddedNodeCert)
	clientContext.SSLCertKey = filepath.Join(certsDir, security.EmbeddedNodeKey)
	httpClient, err = clientContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	req, err = http.NewRequest("GET", testCtx.HTTPRequestScheme()+"://"+s.HTTPAddr()+"/_admin/v1/health", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected OK, got: %d", resp.StatusCode)
	}
}
