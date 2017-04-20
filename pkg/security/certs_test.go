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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestGenerateCACert(t *testing.T) {
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

	cm, err := security.NewCertificateManager(certsDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keyPath := filepath.Join(certsDir, "ca.key")

	testCases := []struct {
		certsDir, caKey       string
		allowReuse, overwrite bool
		errStr                string // error string for CreateCAPair, empty for nil.
		numCerts              int    // number of certificates found in ca.crt
	}{
		{"", "ca.key", false, false, "the path to the certs directory is required", 0},
		{certsDir, "", false, false, "the path to the CA key is required", 0},
		// New CA key/cert.
		{certsDir, keyPath, false, false, "", 1},
		// Files exist, but reuse is disabled.
		{certsDir, keyPath, false, false, "exists, but key reuse is disabled", 2},
		// Files exist, but overwrite is off.
		{certsDir, keyPath, true, false, "file exists", 2},
		// Files exist and reuse/overwrite is enabled.
		{certsDir, keyPath, true, true, "", 2},
		// Cert exists and overwrite is enabled.
		{certsDir, keyPath + "2", false, true, "", 3}, // Using a new key still keeps the ca.crt
	}

	for i, tc := range testCases {
		err := security.CreateCAPair(tc.certsDir, tc.caKey, 512,
			time.Hour*48, tc.allowReuse, tc.overwrite)
		if !testutils.IsError(err, tc.errStr) {
			t.Errorf("#%d: expected error %s but got %+v", i, tc.errStr, err)
			continue
		}

		if err != nil {
			continue
		}

		// No failures on CreateCAPair, we expect a valid CA cert.
		err = cm.LoadCertificates()
		if err != nil {
			t.Fatalf("#%d: unexpected failure: %v", i, err)
		}

		ci := cm.CACert()
		if ci == nil {
			t.Fatalf("#%d: no CA cert found", i)
		}

		certs, err := security.PEMToCertificates(ci.FileContents)
		if err != nil {
			t.Fatalf("#%d: unexpected parsing error for %+v: %v", i, ci, err)
		}

		if actual := len(certs); actual != tc.numCerts {
			t.Errorf("#%d: expected %d certificates, found %d", i, tc.numCerts, actual)
		}
	}
}

func TestGenerateNodeCerts(t *testing.T) {
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

	// Try generating node certs without CA certs present.
	if err := security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, false, []string{"localhost"},
	); err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Now try in the proper order.
	if err := security.CreateCAPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey), 512, time.Hour*48, false, false,
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if err := security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, false, []string{"localhost"},
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

func generateAllCerts(certsDir string) error {
	if err := security.CreateCAPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, true, true,
	); err != nil {
		return errors.Errorf("could not generate CA pair: %v", err)
	}

	if err := security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, true, []string{"127.0.0.1"},
	); err != nil {
		return errors.Errorf("could not generate Node pair: %v", err)
	}

	if err := security.CreateClientPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, true, security.RootUser,
	); err != nil {
		return errors.Errorf("could not generate Client pair: %v", err)
	}

	return nil
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

	if err := generateAllCerts(certsDir); err != nil {
		t.Fatal(err)
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
	defer s.Stopper().Stop(context.TODO())

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
