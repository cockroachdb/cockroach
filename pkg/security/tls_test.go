// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

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
	skip.UnderStress(t, "http server accessing previous test's restriction fn")
	skip.UnderRace(t)

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

	// Start with a clean cipher configuration state
	err := security.SetTLSCipherSuitesConfigured([]string{})
	require.NoError(t, err)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	defer require.NoError(t, security.SetTLSCipherSuitesConfigured([]string{}))

	// setup for db console tests
	httpClient, err := s.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)
	httpClient.Timeout = 2 * time.Second
	defer httpClient.CloseIdleConnections()

	secureClient, err := s.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	secureClient.Timeout = 2 * time.Second
	defer secureClient.CloseIdleConnections()

	urlsToTest := []string{"/_status/vars", "/index.html", "/"}
	adminURLHTTPS := s.AdminURL().String()
	adminURLHTTP := strings.Replace(adminURLHTTPS, "https", "http", 1)

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
		{name: "valid ciphers", ciphers: []string{"TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256"},
			wantErr: false},
		{name: "invalid ciphers", ciphers: []string{"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"}, wantErr: true,
			httpsErr:  []string{"\": EOF", "connect: connection refused", "read: connection reset by peer", "http: server closed idle connection"},
			sqlErr:    "failed to connect to `host=127.0.0.1 user=root database=`: failed to receive message (unexpected EOF)",
			rpcErr:    "initial connection heartbeat failed: grpc:",
			cipherErr: "^presented cipher [^ ]+ not in allowed cipher suite list$"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First ensure we're starting with no restrictions
			err := security.SetTLSCipherSuitesConfigured([]string{})
			require.NoError(t, err)

			// Reset the error container before each test
			cipherErrC.Lock()
			cipherErrC.err = nil
			cipherErrC.Unlock()

			// Now set the custom test ciphers
			err = security.SetTLSCipherSuitesConfigured(tt.ciphers)
			require.NoError(t, err)
			// unset the ciphers after test
			defer func() { _ = security.SetTLSCipherSuitesConfigured([]string{}) }()

			// test db console tls access for cipher restriction.
			for _, u := range urlsToTest {
				for _, client := range []http.Client{httpClient, secureClient} {
					resp, err := client.Get(adminURLHTTP + u)
					if (err == nil) == tt.wantErr {
						var body []byte
						if resp != nil && resp.Body != nil {
							defer resp.Body.Close()
							body, err = io.ReadAll(resp.Body)
						}
						t.Fatalf("expected wantError=%t, got err=%v, resp=%v", tt.wantErr, err, string(body))
					}
					if tt.wantErr {
						cipherErrC.Lock()
						errVal := cipherErrC.err
						cipherErrC.Unlock()
						require.Regexp(t, tt.cipherErr, errVal.Error())
						var errMatch bool
						for idx := range tt.httpsErr {
							errMatch = errMatch || strings.Contains(err.Error(), tt.httpsErr[idx])
						}
						if !errMatch {
							t.Fatalf("the provided error %s does not match any of the expected errors: %v", err.Error(), strings.Join(tt.httpsErr, ", "))
						}
					}
				}
			}

			// test pgx connection for root user with cert auth
			pgURL, cleanup := s.PGUrl(t, serverutils.User(username.RootUser), serverutils.ClientCerts(true))
			defer cleanup()
			rootConn, err := pgx.Connect(ctx, pgURL.String())
			if (err == nil) == tt.wantErr {
				t.Fatalf("expected wantError=%t, got err=%v", tt.wantErr, err)
			}
			if err != nil {
				cipherErrC.Lock()
				errVal := cipherErrC.err
				cipherErrC.Unlock()
				require.Regexp(t, tt.cipherErr, errVal.Error())
				require.Equal(t, tt.sqlErr, err.Error())
			} else {
				require.NoError(t, rootConn.Close(ctx))
			}

			// test rpc connection for root user.
			conn, err := s.RPCClientConnE(username.RootUserName())
			if (err == nil) == tt.wantErr {
				t.Fatalf("expected wantError=%t, got err=%v", tt.wantErr, err)
			}
			if err != nil {
				cipherErrC.Lock()
				errVal := cipherErrC.err
				cipherErrC.Unlock()
				require.Regexp(t, tt.cipherErr, errVal.Error())
				require.Contains(t, err.Error(), tt.rpcErr)
			} else {
				require.NoError(t, conn.Close()) // nolint:grpcconnclose
			}
		})
	}
}
