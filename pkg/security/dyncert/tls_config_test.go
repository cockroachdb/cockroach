// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDynamicTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	withOU := func(ous ...string) func(*x509.Certificate) {
		return func(c *x509.Certificate) {
			c.Subject.OrganizationalUnit = ous
		}
	}

	// Create two CAs and certs for testing.
	ca1 := generateTestCA(t, "Test CA")
	ca2 := generateTestCA(t, "Test CA")
	goodCert1 := generateTestLeafWith(t, ca1, "node", withOU("Tenants"))
	goodCert2 := generateTestLeafWith(t, ca2, "node", withOU("Tenants"))
	wrongCNCert := generateTestLeafWith(t, ca1, "wrong-cn", withOU("Tenants"))
	wrongOUCert := generateTestLeafWith(t, ca1, "node", withOU("WrongOU"))
	multiOUCert := generateTestLeafWith(t, ca1, "node", withOU("Admin", "Tenants", "Support"))
	missingOUCert := generateTestLeaf(t, ca1, "node") // No OU set
	invalidCert := NewCert([]byte("invalid"), []byte("invalid"))

	// Create intermediate CA and leaf for chain testing.
	intermediateCA := generateTestLeafWith(t, ca1, "Intermediate CA", func(c *x509.Certificate) {
		c.IsCA = true
		c.BasicConstraintsValid = true
		c.KeyUsage = x509.KeyUsageCertSign
	})
	chainLeaf := generateTestLeafWith(t, intermediateCA, "node", withOU("Tenants"))
	// Build chain cert: leaf + intermediate (server presents full chain).
	chainLeafPEM, chainLeafKeyPEM := chainLeaf.Get()
	intermediatePEM, _ := intermediateCA.Get()
	chainCert := NewCert(append(chainLeafPEM, intermediatePEM...), chainLeafKeyPEM)

	// Combined CA pool containing both ca1 and ca2.
	ca1PEM, _ := ca1.Get()
	ca2PEM, _ := ca2.Get()
	combinedCA := NewCert(append(ca1PEM, ca2PEM...), nil)

	tests := []struct {
		name         string
		serverPeerCA *Cert // CA server uses to verify client
		serverCert   *Cert
		clientPeerCA *Cert // CA client uses to verify server
		clientCert   *Cert
		// Optional: specify full TLS configs to restart server/client with new configs.
		serverTLSConfig *tls.Config
		clientTLSConfig *tls.Config
		wantError       string
	}{{
		name:         "matching certs succeed",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: goodCert1,
	}, {
		name:         "server wrong CN rejected",
		serverPeerCA: ca1, serverCert: wrongCNCert,
		clientPeerCA: ca1, clientCert: goodCert1,
		wantError: `CN="wrong-cn"`,
	}, {
		name:         "server wrong OU rejected",
		serverPeerCA: ca1, serverCert: wrongOUCert,
		clientPeerCA: ca1, clientCert: goodCert1,
		wantError: "OU=[WrongOU]",
	}, {
		name:         "server different CA rejected",
		serverPeerCA: ca1, serverCert: goodCert2,
		clientPeerCA: ca1, clientCert: goodCert1,
		wantError: "unknown authority",
	}, {
		name:         "client wrong CN rejected",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: wrongCNCert,
		wantError: "bad certificate",
	}, {
		name:         "client wrong OU rejected",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: wrongOUCert,
		wantError: "bad certificate",
	}, {
		name:         "client different CA rejected",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: goodCert2,
		wantError: "unknown certificate authority",
	}, {
		name:         "rotated certs succeed",
		serverPeerCA: ca2, serverCert: goodCert2,
		clientPeerCA: ca2, clientCert: goodCert2,
	}, {
		name:         "server missing OU rejected",
		serverPeerCA: ca1, serverCert: missingOUCert,
		clientPeerCA: ca1, clientCert: goodCert1,
		wantError: "OU=[]",
	}, {
		name:         "client missing OU rejected",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: missingOUCert,
		wantError: "bad certificate",
	}, {
		name:         "invalid client cert rejected",
		serverPeerCA: ca1, serverCert: goodCert1,
		clientPeerCA: ca1, clientCert: invalidCert,
		wantError: "tls:",
	}, {
		name:         "invalid server cert rejected",
		serverPeerCA: ca1, serverCert: invalidCert,
		clientPeerCA: ca1, clientCert: goodCert1,
		wantError: "tls:",
	}, {
		name:            "server without CN/OU check accepts any client",
		serverTLSConfig: NewTLSConfig(missingOUCert, ca1),
		clientTLSConfig: NewTLSConfig(wrongCNCert, ca1, WithPeerCN("node")),
	}, {
		name:            "CN-only check accepts missing OU",
		serverTLSConfig: NewTLSConfig(missingOUCert, ca1, WithPeerCN("node")),
		clientTLSConfig: NewTLSConfig(missingOUCert, ca1, WithPeerCN("node")),
	}, {
		name:            "CN-only check rejects wrong CN",
		serverTLSConfig: NewTLSConfig(missingOUCert, ca1, WithPeerCN("node")),
		clientTLSConfig: NewTLSConfig(wrongCNCert, ca1, WithPeerCN("node")),
		wantError:       "bad certificate",
	}, {
		name:            "server without peerCA accepts any client",
		serverTLSConfig: NewTLSConfig(goodCert1, nil),
		clientTLSConfig: NewTLSConfig(nil, ca1),
	}, {
		name:            "server without peerCA with CN check on server cert",
		serverTLSConfig: NewTLSConfig(goodCert1, nil),
		clientTLSConfig: NewTLSConfig(nil, ca1, WithPeerCN("node")),
	}, {
		name:            "server without peerCA rejects wrong CN on server cert",
		serverTLSConfig: NewTLSConfig(wrongCNCert, nil),
		clientTLSConfig: NewTLSConfig(nil, ca1, WithPeerCN("node")),
		wantError:       `CN="wrong-cn"`,
	}, {
		name:            "client without peerCA fails against test certs",
		serverTLSConfig: NewTLSConfig(goodCert1, nil),
		clientTLSConfig: NewTLSConfig(nil, nil),
		wantError:       "failed to verify certificate",
	}, {
		name:            "InsecureSkipVerify skips chain verification",
		serverTLSConfig: NewTLSConfig(goodCert2, nil),
		clientTLSConfig: NewTLSConfig(nil, nil, WithInsecureSkipVerify()),
	}, {
		name:            "InsecureSkipVerify skips CN/OU checks",
		serverTLSConfig: NewTLSConfig(wrongCNCert, nil),
		clientTLSConfig: NewTLSConfig(nil, nil, WithInsecureSkipVerify(), WithPeerCN("node"), WithPeerOU("Tenants")),
	}, {
		name:            "combined CA pool accepts certs from either CA",
		serverTLSConfig: NewTLSConfig(goodCert1, combinedCA, WithPeerCN("node"), WithPeerOU("Tenants")),
		clientTLSConfig: NewTLSConfig(goodCert2, combinedCA, WithPeerCN("node"), WithPeerOU("Tenants")),
	}, {
		name:            "OU-only check accepts any CN",
		serverTLSConfig: NewTLSConfig(goodCert1, ca1, WithPeerOU("Tenants")),
		clientTLSConfig: NewTLSConfig(wrongCNCert, ca1, WithPeerOU("Tenants")),
	}, {
		name:            "OU-only check rejects wrong OU",
		serverTLSConfig: NewTLSConfig(goodCert1, ca1, WithPeerOU("Tenants")),
		clientTLSConfig: NewTLSConfig(wrongOUCert, ca1, WithPeerOU("Tenants")),
		wantError:       "bad certificate",
	}, {
		name:            "multi-value OU matches if any value matches",
		serverTLSConfig: NewTLSConfig(goodCert1, ca1, WithPeerOU("Tenants")),
		clientTLSConfig: NewTLSConfig(multiOUCert, ca1, WithPeerOU("Tenants")),
	}, {
		name:            "certificate chain with intermediate verifies against root",
		serverTLSConfig: NewTLSConfig(chainCert, nil),
		clientTLSConfig: NewTLSConfig(nil, ca1, WithPeerCN("node"), WithPeerOU("Tenants")),
	}}

	// Shared Cert objects for both peers.
	good1CertPEM, good1KeyPEM := goodCert1.Get()
	serverPeerCACert := NewCert(ca1PEM, nil) // CA the server uses to verify clients
	clientPeerCACert := NewCert(ca1PEM, nil) // CA the client uses to verify server
	serverCert := NewCert(good1CertPEM, good1KeyPEM)
	clientCert := NewCert(good1CertPEM, good1KeyPEM)

	// Default TLS configs with CN and OU verification.
	defaultServerConfig := NewTLSConfig(serverCert, serverPeerCACert, WithPeerCN("node"), WithPeerOU("Tenants"))
	defaultClientConfig := NewTLSConfig(clientCert, clientPeerCACert, WithPeerCN("node"), WithPeerOU("Tenants"))

	// Helper to create a test server with TLS config.
	newServer := func(cfg *tls.Config) *httptest.Server {
		s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "ok")
		}))
		s.TLS = cfg
		s.StartTLS()
		return s
	}

	// Helper to create a client with TLS config.
	newClient := func(cfg *tls.Config) *http.Client {
		return &http.Client{
			Transport: &http.Transport{TLSClientConfig: cfg},
		}
	}

	// Create initial server and client.
	server := newServer(defaultServerConfig)
	client := newClient(defaultClientConfig)

	// Cleanup: close final server/client at end of test.
	// Use closure to capture current value of server/client variables.
	defer func() {
		server.Close()
		client.CloseIdleConnections()
	}()

	// Track current configs to detect when restart is needed.
	currentServerConfig := defaultServerConfig
	currentClientConfig := defaultClientConfig

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Restart server if custom TLS config is specified.
			if tt.serverTLSConfig != nil && tt.serverTLSConfig != currentServerConfig {
				server.Close()
				server = newServer(tt.serverTLSConfig)
				currentServerConfig = tt.serverTLSConfig
			}

			// Recreate client if custom TLS config is specified.
			if tt.clientTLSConfig != nil && tt.clientTLSConfig != currentClientConfig {
				client.CloseIdleConnections()
				client = newClient(tt.clientTLSConfig)
				currentClientConfig = tt.clientTLSConfig
			}

			// Set certs for this test case (only used with default TLS configs).
			if tt.serverPeerCA != nil {
				serverPeerCAPEM, _ := tt.serverPeerCA.Get()
				serverPeerCACert.Set(serverPeerCAPEM, nil)
			}
			if tt.clientPeerCA != nil {
				clientPeerCAPEM, _ := tt.clientPeerCA.Get()
				clientPeerCACert.Set(clientPeerCAPEM, nil)
			}
			if tt.serverCert != nil {
				serverCertPEM, serverKeyPEM := tt.serverCert.Get()
				serverCert.Set(serverCertPEM, serverKeyPEM)
			}
			if tt.clientCert != nil {
				clientCertPEM, clientKeyPEM := tt.clientCert.Get()
				clientCert.Set(clientCertPEM, clientKeyPEM)
			}

			// Make request.
			resp, err := client.Get(server.URL)
			if err == nil {
				_ = resp.Body.Close()
			}

			// Check result.
			if tt.wantError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantError)
			}
		})
	}
}

// TestDynamicTLS_Caching tests internal caching behavior (pointer equality)
// not observable via HTTP end-to-end tests.
func TestDynamicTLS_Caching(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("GetConfigForClient returns error on invalid peerCA", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		cert := generateTestLeaf(t, ca, "test")
		certPEM, keyPEM := cert.Get()
		localCert := NewCert(certPEM, keyPEM)
		invalidCA := NewCert([]byte("invalid"), nil)

		config := NewTLSConfig(localCert, invalidCA)

		_, err := config.GetConfigForClient(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parsing CA certificate")
	})

	t.Run("GetClientCertificate caches result", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		cert := generateTestLeaf(t, ca, "test")
		certPEM, keyPEM := cert.Get()
		localCert := NewCert(certPEM, keyPEM)

		config := NewTLSConfig(localCert, nil)

		cert1, err := config.GetClientCertificate(nil)
		require.NoError(t, err)
		cert2, err := config.GetClientCertificate(nil)
		require.NoError(t, err)
		require.Same(t, cert1, cert2)
	})

	t.Run("GetConfigForClient caches result", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		cert := generateTestLeaf(t, ca, "test")
		caCertPEM, _ := ca.Get()
		certPEM, keyPEM := cert.Get()
		localCert := NewCert(certPEM, keyPEM)
		peerCA := NewCert(caCertPEM, nil)

		config := NewTLSConfig(localCert, peerCA)

		cfg1, err := config.GetConfigForClient(nil)
		require.NoError(t, err)
		cfg2, err := config.GetConfigForClient(nil)
		require.NoError(t, err)
		require.Same(t, cfg1, cfg2)
	})

	t.Run("GetConfigForClient refreshes on local cert change", func(t *testing.T) {
		ca := generateTestCA(t, "Test CA")
		cert1 := generateTestLeaf(t, ca, "test")
		caCertPEM, _ := ca.Get()
		cert1PEM, key1PEM := cert1.Get()
		localCert := NewCert(cert1PEM, key1PEM)
		peerCA := NewCert(caCertPEM, nil)

		config := NewTLSConfig(localCert, peerCA)

		initialCfg, err := config.GetConfigForClient(nil)
		require.NoError(t, err)

		cert2 := generateTestLeaf(t, ca, "test")
		cert2PEM, key2PEM := cert2.Get()
		localCert.Set(cert2PEM, key2PEM)

		newCfg, err := config.GetConfigForClient(nil)
		require.NoError(t, err)
		require.NotSame(t, initialCfg, newCfg)
	})

	t.Run("GetConfigForClient refreshes on CA change", func(t *testing.T) {
		ca1 := generateTestCA(t, "Test CA")
		ca2 := generateTestCA(t, "Test CA")
		cert1 := generateTestLeaf(t, ca1, "test")
		ca1CertPEM, _ := ca1.Get()
		ca2CertPEM, _ := ca2.Get()
		cert1PEM, key1PEM := cert1.Get()
		localCert := NewCert(cert1PEM, key1PEM)
		peerCA := NewCert(ca1CertPEM, nil)

		config := NewTLSConfig(localCert, peerCA)

		initialCfg, err := config.GetConfigForClient(nil)
		require.NoError(t, err)

		cert2 := generateTestLeaf(t, ca2, "test")
		cert2PEM, key2PEM := cert2.Get()
		peerCA.Set(ca2CertPEM, nil)
		localCert.Set(cert2PEM, key2PEM)

		newCfg, err := config.GetConfigForClient(nil)
		require.NoError(t, err)
		require.NotSame(t, initialCfg, newCfg)
	})
}
