// Copyright 2017 The Cockroach Authors.
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

// +build !windows

package security_test

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestRotateCerts tests certs rotation in the server.
// TODO(marc): enable this test on windows once we support a non-signal method
// of triggering a certificate refresh.
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
	// Web session authentication is disabled in order to avoid the need to
	// authenticate the individual clients being instantiated (session auth has
	// no effect on what is being tested here).
	params := base.TestServerArgs{
		SSLCertsDir:                     certsDir,
		DisableWebSessionAuthentication: true,
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

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
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
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
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
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
