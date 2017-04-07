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
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// This is just the mechanics of certs generation.
func TestGenerateCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Do not mock cert access for this test.
	security.ResetAssetLoader()
	defer ResetTest()

	certsDir, err := ioutil.TempDir("", "certs_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(certsDir); err != nil {
			t.Fatal(err)
		}
	}()

	// Try certs generation with empty Certs dir argument.
	if err := security.RunCreateCACert("", "", 512); err == nil {
		t.Fatalf("Expected error, but got none")
	}
	if err := security.RunCreateNodeCert(
		"", "", "", "",
		512, []string{"localhost"},
	); err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Try generating node certs without CA certs present.
	if err := security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"localhost"},
	); err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Now try in the proper order.
	if err := security.RunCreateCACert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		512,
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if err := security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"localhost"},
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

// This is a fairly high-level test of CA and node certificates.
// We construct SSL server and clients and use the generated certs.
func TestUseCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Do not mock cert access for this test.
	security.ResetAssetLoader()
	defer ResetTest()
	certsDir, err := ioutil.TempDir("", "certs_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(certsDir); err != nil {
			t.Fatal(err)
		}
	}()

	if err := security.RunCreateCACert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		512,
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if err := security.RunCreateNodeCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
		512, []string{"127.0.0.1"},
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if err := security.RunCreateClientCert(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedCAKey),
		filepath.Join(certsDir, security.EmbeddedRootCert),
		filepath.Join(certsDir, security.EmbeddedRootKey),
		512, security.RootUser,
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Load TLS Configs. This is what TestServer and HTTPClient do internally.
	if _, err := security.LoadServerTLSConfig(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if _, err := security.LoadClientTLSConfig(
		filepath.Join(certsDir, security.EmbeddedCACert),
		filepath.Join(certsDir, security.EmbeddedNodeCert),
		filepath.Join(certsDir, security.EmbeddedNodeKey),
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	// Start a test server and override certs.
	// We use a real context since we want generated certs.
	params := base.TestServerArgs{
		SSLCertsDir: certsDir,
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	// Insecure mode.
	clientContext := testutils.NewNodeTestBaseContext()
	clientContext.Insecure = true
	httpClient, err := clientContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("GET", s.AdminURL()+"/_status/metrics/local", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("Expected SSL error, got success: %s", body)
	}

	// New client. With certs this time.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	httpClient, err = clientContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	req, err = http.NewRequest("GET", s.AdminURL()+"/_status/metrics/local", nil)
	if err != nil {
		t.Fatalf("could not create request: %v", err)
	}
	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("Expected OK, got %q with body: %s", resp.Status, body)
	}
}
