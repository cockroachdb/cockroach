// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package security_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// TestRotateCerts tests certs rotation in the server.
// TODO(marc): enable this test on windows once we support a non-signal method
// of triggering a certificate refresh.
func TestRotateCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

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

	if err := generateBaseCerts(certsDir); err != nil {
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
	defer s.Stopper().Stop(context.Background())

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

	// Create a client by calling sql.Open which loads the certificates but do not use it yet.
	createTestClient := func() *gosql.DB {
		pgUrl := makeSecurePGUrl(s.ServingSQLAddr(), security.RootUser, certsDir, security.EmbeddedCACert, security.EmbeddedRootCert, security.EmbeddedRootKey)
		goDB, err := gosql.Open("postgres", pgUrl)
		if err != nil {
			t.Fatal(err)
		}
		return goDB
	}

	// Some errors codes.
	const kBadAuthority = "certificate signed by unknown authority"
	const kBadCertificate = "tls: bad certificate"

	// Test client with the same certs.
	clientContext := testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	firstSCtx := rpc.MakeSecurityContext(clientContext, security.CommandTLSSettings{}, roachpb.SystemTenantID)
	firstClient, err := firstSCtx.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	if err := clientTest(firstClient); err != nil {
		t.Fatal(err)
	}

	firstSQLClient := createTestClient()
	defer firstSQLClient.Close()

	if _, err := firstSQLClient.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// Delete certs and re-generate them.
	// New clients will fail with CA errors.
	if err := os.RemoveAll(certsDir); err != nil {
		t.Fatal(err)
	}
	if err := generateBaseCerts(certsDir); err != nil {
		t.Fatal(err)
	}

	// Setup a second http client. It will load the new certs.
	// We need to use a new context as it keeps the certificate manager around.
	// Fails on crypto errors.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir

	secondSCtx := rpc.MakeSecurityContext(clientContext, security.CommandTLSSettings{}, roachpb.SystemTenantID)
	secondClient, err := secondSCtx.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	if err := clientTest(secondClient); !testutils.IsError(err, kBadAuthority) {
		t.Fatalf("expected error %q, got: %q", kBadAuthority, err)
	}

	secondSQLClient := createTestClient()
	defer secondSQLClient.Close()

	if _, err := secondSQLClient.Exec("SELECT 1"); !testutils.IsError(err, kBadAuthority) {
		t.Fatalf("expected error %q, got: %q", kBadAuthority, err)
	}

	// We haven't triggered the reload, first client should still work.
	if err := clientTest(firstClient); err != nil {
		t.Fatal(err)
	}

	if _, err := firstSQLClient.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	beforeReload := timeutil.Now()
	t.Log("issuing SIGHUP")
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// Try again, the first HTTP client should now fail, the second should succeed.
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

	// Check that the structured event was logged.
	// We use SucceedsSoon here because there may be a delay between
	// the moment SIGHUP is processed and certs are reloaded, and
	// the moment the structured logging event is actually
	// written to the log file.
	testutils.SucceedsSoon(t, func() error {
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(beforeReload.UnixNano(),
			math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
		if err != nil {
			t.Fatal(err)
		}
		foundEntry := false
		for _, e := range entries {
			if !strings.Contains(e.Message, "certs_reload") {
				continue
			}
			foundEntry = true
			// TODO(knz): Remove this when crdb-v2 becomes the new format.
			e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
			// crdb-v2 starts json with an equal sign.
			e.Message = strings.TrimPrefix(e.Message, "=")
			jsonPayload := []byte(e.Message)
			var ev eventpb.CertsReload
			if err := json.Unmarshal(jsonPayload, &ev); err != nil {
				t.Errorf("unmarshalling %q: %v", e.Message, err)
			}
			if ev.Success != true || ev.ErrorMessage != "" {
				t.Errorf("incorrect event: expected success with no error, got %+v", ev)
			}
		}
		if !foundEntry {
			return errors.New("structured entry for certs_reload not found in log")
		}
		return nil
	})

	// Nothing changed in the first SQL client: the connection is already established.
	if _, err := firstSQLClient.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// However, the second SQL client now succeeds.
	if _, err := secondSQLClient.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	// Now regenerate certs, but keep the CA cert around.
	// We still need to delete the key.
	// New clients with certs will fail with bad certificate (CA not yet loaded).
	if err := os.Remove(filepath.Join(certsDir, security.EmbeddedCAKey)); err != nil {
		t.Fatal(err)
	}
	if err := generateBaseCerts(certsDir); err != nil {
		t.Fatal(err)
	}

	// Setup a third http client. It will load the new certs.
	// We need to use a new context as it keeps the certificate manager around.
	// This is HTTP and succeeds because we do not ask for or verify client certificates.
	clientContext = testutils.NewNodeTestBaseContext()
	clientContext.SSLCertsDir = certsDir
	thirdSCtx := rpc.MakeSecurityContext(clientContext, security.CommandTLSSettings{}, roachpb.SystemTenantID)
	thirdClient, err := thirdSCtx.GetHTTPClient()
	if err != nil {
		t.Fatalf("could not create http client: %v", err)
	}

	if err := clientTest(thirdClient); err != nil {
		t.Fatal(err)
	}

	// However, a SQL client uses client certificates. The node does not have the new CA yet.
	thirdSQLClient := createTestClient()
	defer thirdSQLClient.Close()

	if _, err := thirdSQLClient.Exec("SELECT 1"); !testutils.IsError(err, kBadCertificate) {
		t.Fatalf("expected error %q, got: %q", kBadCertificate, err)
	}

	// We haven't triggered the reload, second client should still work.
	if err := clientTest(secondClient); err != nil {
		t.Fatal(err)
	}

	t.Log("issuing SIGHUP")
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// Wait until client3 succeeds (both http and sql).
	testutils.SucceedsSoon(t,
		func() error {
			if err := clientTest(thirdClient); err != nil {
				return errors.Errorf("third HTTP client failed: %v", err)
			}
			if _, err := thirdSQLClient.Exec("SELECT 1"); err != nil {
				return errors.Errorf("third SQL client failed: %v", err)
			}
			return nil
		})
}

var cmLogRe = regexp.MustCompile(`event_log\.go`)
