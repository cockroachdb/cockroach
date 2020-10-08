// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func makeTenantCerts(t *testing.T, tenant uint64) (certsDir string, cleanup func()) {
	certsDir, cleanup = tempDir(t)

	// Make certs for the tenant CA (= auth broker). In production, these would be
	// given to a dedicated service.
	tenantCAKey := filepath.Join(certsDir, "tenant-ca-name-irrelevant.key")
	require.NoError(t, security.CreateTenantClientCAPair(
		certsDir,
		tenantCAKey,
		2048,
		100000*time.Hour, // basically long-lived
		false,            // allowKeyReuse
		false,            // overwrite
	))

	// That dedicated service can make client certs for a tenant as follows:
	tenantCerts, err := security.CreateTenantClientPair(
		certsDir, tenantCAKey, testKeySize, 48*time.Hour, tenant,
	)
	require.NoError(t, err)
	// We write the certs to disk, though in production this would not necessarily
	// happen (it may be enough to just have them in-mem, we will see).
	require.NoError(t, security.WriteTenantClientPair(certsDir, tenantCerts, false /* overwrite */))

	// The server also needs to show certs trusted by the client. These are the
	// node certs.
	serverCAKeyPath := filepath.Join(certsDir, "name-does-not-matter-too.key")
	require.NoError(t, security.CreateCAPair(
		certsDir, serverCAKeyPath, testKeySize, 1000*time.Hour, false, false,
	))
	require.NoError(t, security.CreateNodePair(
		certsDir, serverCAKeyPath, testKeySize, 500*time.Hour, false, []string{"127.0.0.1"}))
	return certsDir, cleanup
}

// TestTenantCertificates creates a tenant CA and from it client certificates
// for a tenant. It then sets up a smoke test that verifies that the tenant
// can use its client certificates to connect to a https server that trusts
// the tenant CA.
//
// This foreshadows upcoming work on multi-tenancy, see:
// https://github.com/cockroachdb/cockroach/issues/49105
// https://github.com/cockroachdb/cockroach/issues/47898
func TestTenantCertificates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("embedded-certs", func(t *testing.T) {
		testTenantCertificatesInner(t, true /* embedded */)
	})
	t.Run("new-certs", func(t *testing.T) {
		testTenantCertificatesInner(t, false /* embedded */)
	})
}

func testTenantCertificatesInner(t *testing.T, embedded bool) {
	defer leaktest.AfterTest(t)()

	var certsDir string
	var tenant uint64
	if !embedded {
		// Don't mock assets in this test, we're creating our own one-off certs.
		security.ResetAssetLoader()
		defer ResetTest()
		tenant = uint64(rand.Int63())
		var cleanup func()
		certsDir, cleanup = makeTenantCerts(t, tenant)
		defer cleanup()
	} else {
		certsDir = security.EmbeddedCertsDir
		tenant = security.EmbeddedTenantIDs()[0]
	}

	// Now set up the config a server would use. The client will trust it based on
	// the server CA and server node certs, and it will validate incoming
	// connections based on the tenant CA.

	cm, err := security.NewCertificateManager(certsDir, security.CommandTLSSettings{})
	require.NoError(t, err)
	serverTLSConfig, err := cm.GetServerTLSConfig()
	require.NoError(t, err)

	// Make a new CertificateManager for the tenant. We could've used this one
	// for the server as well, but this way it's closer to reality.
	cm, err = security.NewCertificateManager(certsDir, security.CommandTLSSettings{}, security.ForTenant(tenant))
	require.NoError(t, err)

	// The client in turn trusts the server CA and presents its tenant certs to the
	// server (which will validate them using the tenant CA).
	clientTLSConfig, err := cm.GetTenantClientTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, clientTLSConfig)

	// Set up a HTTPS server using server TLS config, set up a http client using the
	// client TLS config, make a request.

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	httpServer := http.Server{
		Addr:      ln.Addr().String(),
		TLSConfig: serverTLSConfig,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			fmt.Fprint(w, "hello, tenant ", req.TLS.PeerCertificates[0].Subject.CommonName)
		}),
	}
	defer func() { _ = httpServer.Close() }()
	go func() {
		_ = httpServer.ServeTLS(ln, "", "")
	}()

	httpClient := http.Client{Transport: &http.Transport{
		TLSClientConfig: clientTLSConfig,
	}}
	defer httpClient.CloseIdleConnections()

	resp, err := httpClient.Get("https://" + ln.Addr().String())
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("hello, tenant %d", tenant), string(b))
}
