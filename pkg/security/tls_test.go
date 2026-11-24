// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"context"
	"crypto/fips140"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestLoadTLSConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager(certnames.EmbeddedCertsDir, security.CommandTLSSettings{})
	require.NoError(t, err)
	// We check that the UI Server tls.Config doesn't include the old cipher suites by default.
	config, err := cm.GetUIServerTLSConfig()
	require.NoError(t, err)
	clientConfig, err := config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)
	require.NotNil(t, clientConfig)
	require.Equal(t, security.RecommendedCipherSuites(), clientConfig.CipherSuites)

	// Next we check that the Server tls.Config is correctly generated.
	config, err = cm.GetServerTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)

	clientConfig, err = config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, security.RecommendedCipherSuites(), clientConfig.CipherSuites)
	if len(clientConfig.Certificates) != 1 {
		t.Fatalf("clientConfig.Certificates should have 1 cert; found %d", len(clientConfig.Certificates))
	}
	cert := clientConfig.Certificates[0]
	asn1Data := cert.Certificate[0] // TODO Check len()

	x509Cert, err := x509.ParseCertificate(asn1Data)
	if err != nil {
		t.Fatalf("Couldn't parse test cert: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", clientConfig.RootCAs); err != nil {
		t.Errorf("Couldn't verify test cert against server CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "localhost", clientConfig.ClientCAs); err != nil {
		t.Errorf("Couldn't verify test cert against client CA: %v", err)
	}

	if err = verifyX509Cert(x509Cert, "google.com", clientConfig.RootCAs); err == nil {
		t.Errorf("Verified test cert for wrong hostname")
	}
}

func TestLoadTLSConfigWithOldCipherSuites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reset := envutil.TestSetEnv(t, security.OldCipherSuitesEnabledEnv, "true")
	defer reset()

	cm, err := security.NewCertificateManager(certnames.EmbeddedCertsDir, security.CommandTLSSettings{})
	require.NoError(t, err)

	recommendedAndOldCipherSuites := append(
		security.RecommendedCipherSuites(),
		security.OldCipherSuites()...,
	)

	// We check that the UI Server tls.Config now includes the old cipher suites after the existing cipher suites.
	config, err := cm.GetUIServerTLSConfig()
	require.NoError(t, err)
	clientConfig, err := config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, recommendedAndOldCipherSuites, clientConfig.CipherSuites)

	// We check that the Server tls.Config now includes the old cipher suites after the existing cipher suites.
	config, err = cm.GetServerTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, config.GetConfigForClient)
	clientConfig, err = config.GetConfigForClient(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	require.NotNil(t, clientConfig)
	require.Equal(t, recommendedAndOldCipherSuites, clientConfig.CipherSuites)
}

func verifyX509Cert(cert *x509.Certificate, dnsName string, roots *x509.CertPool) error {
	verifyOptions := x509.VerifyOptions{
		DNSName: dnsName,
		Roots:   roots,
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
	}
	_, err := cert.Verify(verifyOptions)
	return err
}

func TestTLSCipherRestrict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name      string
		ciphers   []string
		wantErr   bool
		httpsErr  []string
		sqlErr    string
		rpcErr    string
		cipherErr string
	}{
		{name: "no cipher set", ciphers: []string{}, wantErr: false},
		{name: "valid ciphers", ciphers: []string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"},
			wantErr: false},
		{name: "invalid ciphers", ciphers: []string{"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"}, wantErr: true,
			httpsErr:  []string{"\": EOF", "connect: connection refused", "read: connection reset by peer", "http: server closed idle connection"},
			sqlErr:    "^failed to connect to `host=127\\.0\\.0\\.1 user=root database=`: server error \\(ERROR: cannot use SSL\\/TLS with the requested ciphers: presented cipher [^ ]+ not in allowed cipher suite list \\(SQLSTATE 08004\\)\\)$",
			rpcErr:    "initial connection heartbeat failed:",
			cipherErr: "^presented cipher [^ ]+ not in allowed cipher suite list$"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// since the listener does not return rpc/sql/http connection errors, we
			// need to have a separate hook to obtain and validate it.
			type cipherErrContainer struct {
				syncutil.Mutex
				err net.Error
			}

			cipherErrC := &cipherErrContainer{}
			cipherRestrictFn := security.TLSCipherRestrict
			defer testutils.TestingHook(&security.TLSCipherRestrict, func(conn net.Conn) (err net.Error) {
				err = cipherRestrictFn(conn)
				cipherErrC.Lock()
				cipherErrC.err = err
				cipherErrC.Unlock()
				return err
			})()
			ctx := context.Background()

			s := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			// set the custom test ciphers
			err := security.SetTLSCipherSuitesConfigured(tt.ciphers)
			require.NoError(t, err)

			// Reset at end of subtest, to avoid global state pollution.
			defer func() {
				_ = security.SetTLSCipherSuitesConfigured([]string{})
			}()

			// setup for db console tests
			httpClient, transport, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
			require.NoError(t, err)
			const httpTimeout = "Client.Timeout exceeded while awaiting headers"
			transport.TLSClientConfig.MinVersion = tls.VersionTLS13
			defer httpClient.CloseIdleConnections()

			urlsToTest := []string{"/_status/vars", "/index.html", "/"}
			adminURLHTTPS := s.AdminURL().String()
			adminURLHTTP := strings.Replace(adminURLHTTPS, "https", "http", 1)

			// Reset the error container before each test
			cipherErrC.Lock()
			cipherErrC.err = nil
			cipherErrC.Unlock()

			// test db console tls access for cipher restriction.
			for _, u := range urlsToTest {
				for _, client := range []http.Client{httpClient} {
					var wg sync.WaitGroup
					wg.Add(1)
					var body []byte
					go func() {
						defer wg.Done()
						var resp *http.Response
						resp, err = client.Get(adminURLHTTP + u)
						if resp != nil && resp.Body != nil {
							defer resp.Body.Close()
							body, err = io.ReadAll(resp.Body)
						}
					}()
					wg.Wait()
					if (err == nil) == tt.wantErr {
						if !(err != nil && strings.Contains(err.Error(), httpTimeout)) {
							t.Fatalf("expected wantError=%t, got err=%v, resp=%v", tt.wantErr, err, string(body))
						}
					}
					if tt.wantErr {
						var errMatch bool
						for idx := range tt.httpsErr {
							errMatch = errMatch || strings.Contains(err.Error(), tt.httpsErr[idx])
						}
						if !errMatch {
							t.Fatalf("the provided error %s does not match any of the expected errors: %v", err.Error(), strings.Join(tt.httpsErr, ", "))
						}
						cipherErrC.Lock()
						errVal := cipherErrC.err
						cipherErrC.Unlock()
						require.NotNil(t, errVal)
						require.Regexp(t, tt.cipherErr, errVal.Error())
					}
				}
			}

			// test pgx connection for root user with cert auth
			pgURL, cleanup := s.PGUrl(t, serverutils.User(username.RootUser), serverutils.ClientCerts(true))
			defer cleanup()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err = pgx.Connect(ctx, pgURL.String())
			}()
			wg.Wait()

			if (err == nil) == tt.wantErr {
				t.Fatalf("expected wantError=%t, got err=%v", tt.wantErr, err)
			}
			if err != nil {
				cipherErrC.Lock()
				errVal := cipherErrC.err
				cipherErrC.Unlock()
				require.Regexp(t, tt.cipherErr, errVal.Error())
				require.Regexp(t, tt.sqlErr, err.Error())
			}

			// test rpc connection for root user.
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err = s.RPCClientConnE(username.RootUserName())
			}()
			wg.Wait()
			if (err == nil) == tt.wantErr {
				t.Fatalf("expected wantError=%t, got err=%v", tt.wantErr, err)
			}
			if err != nil {
				cipherErrC.Lock()
				errVal := cipherErrC.err
				cipherErrC.Unlock()
				require.Regexp(t, tt.cipherErr, errVal.Error())
				require.Contains(t, err.Error(), tt.rpcErr)
			}
		})
	}
}

// TestPostQuantumTLSIntegration tests complete integration of post-quantum
// TLS in CockroachDB server configuration across all connection types.
// PQC is supported by Go 1.24+ via the X25519MLKEM768 curve.
// There is no need to make explicit TLS changes to support PQC as
// it's natively supported by golang.
// The test runs with both DRPC enabled and disabled to ensure PQC key
// exchange works regardless of the RPC transport.
func TestPostQuantumTLSIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if fips140.Enabled() {
		skip.IgnoreLint(t, "ML-KEM (X25519MLKEM768) is not FIPS-approved in go 1.25")
	}

	for _, drpcOption := range []base.DefaultTestDRPCOption{
		base.TestDRPCDisabled,
		base.TestDRPCEnabled,
	} {
		t.Run(fmt.Sprintf("DRPC_%s", drpcOption), func(t *testing.T) {
			testPostQuantumTLSIntegration(t, drpcOption)
		})
	}
}

func testPostQuantumTLSIntegration(t *testing.T, drpcOption base.DefaultTestDRPCOption) {
	type connectionInfo struct {
		connType string
		state    tls.ConnectionState
	}

	// Capture TLS connection details via the TLSCipherRestrict hook.
	// We reuse this existing hook rather than adding a new dedicated inspection hook
	// to keep the implementation simple. This is primarily necessary for RPC connections,
	// as HTTP and SQL connections already expose TLS state through their respective APIs
	// (resp.TLS and pgConn.Conn()). Note: this hook is being used here for TLS curve
	// validation (X25519MLKEM768), not for cipher suite enforcement.
	var connections []connectionInfo
	var connMutex syncutil.Mutex
	tlsInspectionHook := security.TLSCipherRestrict
	defer testutils.TestingHook(&security.TLSCipherRestrict, func(conn net.Conn) (err net.Error) {
		if tlsConn, ok := conn.(*tls.Conn); ok {
			if !tlsConn.ConnectionState().HandshakeComplete {
				_ = tlsConn.Handshake()
			}
			state := tlsConn.ConnectionState()

			connMutex.Lock()
			connections = append(connections, connectionInfo{
				connType: "captured",
				state:    state,
			})
			connMutex.Unlock()
		}
		return tlsInspectionHook(conn)
	})()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultDRPCOption: drpcOption,
	})
	defer s.Stopper().Stop(ctx)

	// Test HTTP/Admin UI connections
	t.Run("HTTP_Connection", func(t *testing.T) {
		httpClient, _, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		var connState tls.ConnectionState
		resp, err := httpClient.Get(s.AdminURL().String() + "/_status/vars")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		if resp.TLS != nil {
			connState = *resp.TLS
		}

		validateConnection(t, connState, "HTTP")
	})

	// Test PostgreSQL connections
	t.Run("SQL_Connection", func(t *testing.T) {
		pgURL, cleanup := s.PGUrl(t, serverutils.User(username.RootUser), serverutils.ClientCerts(true))
		defer cleanup()

		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		var result int
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 1, result)

		var connState tls.ConnectionState
		if pgConn := conn.PgConn(); pgConn != nil {
			if tlsConn, ok := pgConn.Conn().(*tls.Conn); ok {
				connState = tlsConn.ConnectionState()
			}
		}

		validateConnection(t, connState, "SQL")
	})

	// Test RPC connections. Snapshot captured connections before making the
	// RPC call so we only inspect connections established by our call, not
	// background heartbeats or internal RPCs.
	t.Run("RPC_Connection", func(t *testing.T) {
		connMutex.Lock()
		baselineCount := len(connections)
		connMutex.Unlock()

		_, err := s.RPCClientConnE(username.RootUserName())
		require.NoError(t, err)

		connMutex.Lock()
		newConns := connections[baselineCount:]
		connMutex.Unlock()

		require.NotEmpty(t, newConns, "Should have captured new RPC connection states")
		validateConnection(t, newConns[0].state, "RPC")
	})
}

// validateConnection checks if the TLS connection matches expected PQC configuration
func validateConnection(t *testing.T, state tls.ConnectionState, connType string) {
	require.True(t, state.HandshakeComplete, "%s: TLS handshake should be complete", connType)

	require.GreaterOrEqual(t, state.Version, uint16(tls.VersionTLS13),
		"%s: Expected TLS 1.3 or higher when PQC enabled", connType)

	// Verify cipher suite is appropriate for TLS version
	validTLS13Ciphers := []uint16{
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_CHACHA20_POLY1305_SHA256,
	}
	require.Contains(t, validTLS13Ciphers, state.CipherSuite,
		"%s: Should use valid TLS 1.3 cipher suite", connType)

	require.Equal(t, state.CurveID, tls.X25519MLKEM768)
}

// TestMLKEMCompatibility tests compatibility between ML-KEM and classical clients
func TestMLKEMCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if fips140.Enabled() {
		skip.IgnoreLint(t, "ML-KEM (X25519MLKEM768) is not FIPS-approved in go 1.25")
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("ClassicalClientToMLKEMServer", func(t *testing.T) {
		// Test that classical clients can still connect to ML-KEM enabled server
		httpClient, transport, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		// Force classical key exchange only
		transport.TLSClientConfig.CurvePreferences = []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
			tls.CurveP384,
		}
		transport.TLSClientConfig.MinVersion = tls.VersionTLS13

		resp, err := httpClient.Get(s.AdminURL().String() + "/_status/vars")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NotNil(t, resp.TLS)
		require.Equal(t, tls.X25519, resp.TLS.CurveID,
			"Classical client should negotiate X25519, not ML-KEM")
	})

	t.Run("MLKEMClientToMLKEMServer", func(t *testing.T) {
		// Test that ML-KEM clients can connect to ML-KEM enabled server
		httpClient, transport, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		// Prefer ML-KEM with classical fallback
		transport.TLSClientConfig.CurvePreferences = []tls.CurveID{
			tls.X25519MLKEM768,
		}
		transport.TLSClientConfig.MinVersion = tls.VersionTLS13

		resp, err := httpClient.Get(s.AdminURL().String() + "/_status/vars")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NotNil(t, resp.TLS)
		require.Equal(t, tls.X25519MLKEM768, resp.TLS.CurveID,
			"ML-KEM client should negotiate X25519MLKEM768")
	})
}

// TestServerCurveNegotiation tests that a server constrained to specific TLS
// curves correctly negotiates those curves with clients. This validates that
// CurvePreferencesOverride works and that curve negotiation behaves as expected
// when the server is pinned (e.g. in a FIPS-like configuration).
func TestServerCurveNegotiation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name            string
		serverCurves    []tls.CurveID
		clientCurves    []tls.CurveID
		expectedCurveID tls.CurveID
	}{
		{
			name:            "P256ServerWithMLKEMClient",
			serverCurves:    []tls.CurveID{tls.CurveP256},
			clientCurves:    []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256},
			expectedCurveID: tls.CurveP256,
		},
		{
			name:            "P256ServerWithP256Client",
			serverCurves:    []tls.CurveID{tls.CurveP256},
			clientCurves:    []tls.CurveID{tls.CurveP256, tls.CurveP384},
			expectedCurveID: tls.CurveP256,
		},
		{
			name:            "X25519ServerWithMLKEMClient",
			serverCurves:    []tls.CurveID{tls.X25519},
			clientCurves:    []tls.CurveID{tls.X25519MLKEM768, tls.X25519},
			expectedCurveID: tls.X25519,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pin the server's curve preferences before starting the server so
			// the cached TLS config picks up the override.
			defer testutils.TestingHook(
				&security.CurvePreferencesOverride, tt.serverCurves,
			)()

			ctx := context.Background()
			s := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			httpClient, transport, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
			require.NoError(t, err)
			defer httpClient.CloseIdleConnections()

			transport.TLSClientConfig.CurvePreferences = tt.clientCurves
			transport.TLSClientConfig.MinVersion = tls.VersionTLS13

			resp, err := httpClient.Get(s.AdminURL().String() + "/_status/vars")
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NotNil(t, resp.TLS)
			require.Equal(t, tt.expectedCurveID, resp.TLS.CurveID)
		})
	}

	// Test that a connection fails when the client and server have no
	// overlapping curves.
	t.Run("NoCommonCurves", func(t *testing.T) {
		defer testutils.TestingHook(
			&security.CurvePreferencesOverride,
			[]tls.CurveID{tls.CurveP256},
		)()

		ctx := context.Background()
		s := serverutils.StartServerOnly(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		httpClient, transport, err := s.ApplicationLayer().GetUnauthenticatedHTTPClientWithTransport()
		require.NoError(t, err)
		defer httpClient.CloseIdleConnections()

		transport.TLSClientConfig.CurvePreferences = []tls.CurveID{
			tls.X25519MLKEM768,
			tls.X25519,
		}
		transport.TLSClientConfig.MinVersion = tls.VersionTLS13

		_, err = httpClient.Get(s.AdminURL().String() + "/_status/vars")
		require.ErrorContains(t, err, "tls: handshake failure",
			"expected TLS handshake failure when client and server have no common curves")
	})
}
