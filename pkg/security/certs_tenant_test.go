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
)

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

	// Don't mock assets in this test, we're creating our own one-off certs.
	security.ResetAssetLoader()
	defer ResetTest()

	certsDir, cleanup := tempDir(t)
	defer cleanup()

	// Make certs for the tenant CA (= auth broker). In production, these would be
	// given to a dedicated service.
	tenantCAKey := filepath.Join(certsDir, "tenant-ca-name-irrelevant.key")
	require.NoError(t, security.CreateTenantCAPair(
		certsDir,
		tenantCAKey,
		2048,
		100000*time.Hour, // basically long-lived
		false,            // allowKeyReuse
		false,            // overwrite
	))

	// That dedicated service can make client certs for a tenant as follows:
	const tenant = "gromphadorhina-portentosa"
	tenantCerts, err := security.CreateTenantClientPair(
		certsDir, tenantCAKey, testKeySize, 48*time.Hour, tenant, []string{"127.0.0.1"},
	)
	require.NoError(t, err)
	// We write the certs to disk, though in production this would not necessarily
	// happen (it may be enough to just have them in-mem, we will see).
	require.NoError(t, security.WriteTenantClientPair(certsDir, tenantCerts, false /* overwrite */))

	// To make this example work we also need certs that the server can use. We
	// need something here that the tenant will trust. We just make node certs
	// out of convenience. In production, these would be auxiliary certs.
	dummyCAKeyPath := filepath.Join(certsDir, "name-does-not-matter-too.key")
	require.NoError(t, security.CreateCAPair(
		certsDir, dummyCAKeyPath, testKeySize, 1000*time.Hour, false, false,
	))
	require.NoError(t, security.CreateNodePair(
		certsDir, dummyCAKeyPath, testKeySize, 500*time.Hour, false, []string{"127.0.0.1"}))

	dummyCACertPath := filepath.Join(certsDir, security.CACertFilename())

	// Now set up the config a server would use. The client will trust it based on
	// the dummy CA and dummy node certs, and it will validate incoming
	// connections based on the tenant CA.
	serverTLSConfig, err := security.LoadServerTLSConfig(
		dummyCACertPath,
		filepath.Join(certsDir, security.TenantCACertFilename()),
		filepath.Join(certsDir, security.NodeCertFilename()),
		filepath.Join(certsDir, security.NodeKeyFilename()),
	)
	require.NoError(t, err)

	// The client in turn trusts the dummy CA and presents its tenant certs to the
	// server (which will validate them using the tenant CA).
	clientTLSConfig, err := security.LoadClientTLSConfig(
		dummyCACertPath,
		filepath.Join(certsDir, security.TenantClientCertFilename(tenant)),
		filepath.Join(certsDir, security.TenantClientKeyFilename(tenant)),
	)
	require.NoError(t, err)

	// Set up a HTTPS server using server TLS config, set up a http client using the
	// client TLS config, make a request.

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer ln.Close()

	httpServer := http.Server{
		Addr:      ln.Addr().String(),
		TLSConfig: serverTLSConfig,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			fmt.Fprint(w, "hello, tenant ", req.TLS.PeerCertificates[0].Subject.CommonName)
		}),
	}
	defer httpServer.Close()
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
	require.Equal(t, "hello, tenant "+tenant, string(b))
}
