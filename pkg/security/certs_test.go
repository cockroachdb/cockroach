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
	"syscall"
	"testing"
	"time"

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
		certsDir, caKey string
		errStr          string // error string for CreateCAPair, empty for nil.
		numCerts        int    // number of certificates found in ca.crt
	}{
		{"", "ca.key", "the path to the certs directory is required", 0},
		{certsDir, "", "the path to the CA key is required", 0},
		{certsDir, keyPath, "", 1},
		{certsDir, keyPath, "", 2},
		{certsDir, keyPath + "2", "", 3}, // Using a new key still keeps the ca.crt
	}

	for i, tc := range testCases {
		err := security.CreateCAPair(tc.certsDir, tc.caKey, 512, time.Hour*48)
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
		512, time.Hour*48, []string{"localhost"},
	); err == nil {
		t.Fatalf("Expected error, but got none")
	}

	// Now try in the proper order.
	if err := security.CreateCAPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey), 512, time.Hour*48,
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}

	if err := security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, []string{"localhost"},
	); err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

func generateAllCerts(certsDir string) error {
	if err := security.CreateCAPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey), 512, time.Hour*48,
	); err != nil {
		return errors.Errorf("could not generate CA pair: %v", err)
	}

	if err := security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, []string{"127.0.0.1"},
	); err != nil {
		return errors.Errorf("could not generate Node pair: %v", err)
	}

	if err := security.CreateClientPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		512, time.Hour*48, security.RootUser,
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

// TestRotateCerts tests certs rotation in the server.
func TestRotateCerts(t *testing.T) {
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

	// Start a test server with first set of certs.
	params := base.TestServerArgs{
		SSLCertsDir: certsDir,
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	// Client test function.
	clientTest := func(httpClient http.Client) error {
		req, err := http.NewRequest("GET", s.AdminURL()+"/_status/metrics/local", nil)
		if err != nil {
			return errors.Errorf("could not create request: %v", err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return errors.Errorf("http request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			return errors.Errorf("Expected OK, got %q with body: %s", resp.Status, body)
		}
		return nil
	}

	// Test client with the same certs.
	clientContext := testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	firstClient, err := clientContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	if err := clientTest(firstClient); err != nil {
		t.Fatal(err)
	}

	// Delete certs and re-generate them.
	// New clients will fail with CA errors.
	if err := os.RemoveAll(certsDir); err != nil {
		t.Fatal(err)
	}
	if err := generateAllCerts(certsDir); err != nil {
		t.Fatal(err)
	}

	// Setup a second http client. It will load the new certs.
	// We need to use a new context as it keeps the certificate manager around.
	// Fails on crypto errors.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	secondClient, err := clientContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	if err := clientTest(secondClient); !testutils.IsError(err, "unknown authority") {
		t.Fatalf("expected unknown authority error, got: %q", err)
	}

	// We haven't triggered the reload, first client should still work.
	if err := clientTest(firstClient); err != nil {
		t.Fatal(err)
	}

	t.Log("issuing SIGHUP")
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// Try again, the first client should now fail, the second should succeed.
	testutils.SucceedsSoon(t,
		func() error {
			if err := clientTest(firstClient); !testutils.IsError(err, "unknown authority") {
				return errors.Errorf("expected unknown authority, got %v", err)
			}

			if err := clientTest(secondClient); err != nil {
				return err
			}
			return nil
		})

	// Now regenerate certs, but keep the CA cert around.
	// We still need to delete the key.
	// New clients will fail with bad certificate (CA not yet loaded).
	if err := os.Remove(filepath.Join(certsDir, security.EmbeddedCAKey)); err != nil {
		t.Fatal(err)
	}
	if err := generateAllCerts(certsDir); err != nil {
		t.Fatal(err)
	}

	// Setup a third http client. It will load the new certs.
	// We need to use a new context as it keeps the certificate manager around.
	// Fails on crypto errors.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	thirdClient, err := clientContext.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	// client3 fails on bad certificate, because the node does not have the new CA.
	if err := clientTest(thirdClient); !testutils.IsError(err, "tls: bad certificate") {
		t.Fatalf("expected bad certificate error, got: %q", err)
	}

	// We haven't triggered the reload, second client should still work.
	if err := clientTest(secondClient); err != nil {
		t.Fatal(err)
	}

	t.Log("issuing SIGHUP")
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// client2 fails on bad CA for the node certs, client3 succeeds.
	testutils.SucceedsSoon(t,
		func() error {
			if err := clientTest(secondClient); !testutils.IsError(err, "unknown authority") {
				return errors.Errorf("expected unknown authority, got %v", err)
			}
			if err := clientTest(thirdClient); err != nil {
				return errors.Errorf("third client failed: %v", err)
			}
			return nil
		})
}
