// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestManager(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	dir := t.TempDir()

	// Create two certificates with separate paths.
	certPath1 := filepath.Join(dir, "cert1.pem")
	keyPath1 := filepath.Join(dir, "key1.pem")
	certPath2 := filepath.Join(dir, "cert2.pem")
	keyPath2 := filepath.Join(dir, "key2.pem")

	// Generate and write initial certificates.
	ca1 := generateTestCA(t, "CA v1")
	certPEM1, keyPEM1 := ca1.Get()
	require.NoError(t, os.WriteFile(certPath1, certPEM1, 0600))
	require.NoError(t, os.WriteFile(keyPath1, keyPEM1, 0600))

	ca2 := generateTestCA(t, "CA v2")
	certPEM2, keyPEM2 := ca2.Get()
	require.NoError(t, os.WriteFile(certPath2, certPEM2, 0600))
	require.NoError(t, os.WriteFile(keyPath2, keyPEM2, 0600))

	m, err := NewManager(ctx, stopper)
	require.NoError(t, err)

	t.Run("Register and Get multiple certs", func(t *testing.T) {
		require.NoError(t, m.Register("first", certPath1, keyPath1))
		require.NoError(t, m.Register("second", certPath2, keyPath2))

		cert1 := m.Get("first")
		require.NotNil(t, cert1)
		parsed1, key1, err := cert1.AsLeafCertificate()
		require.NoError(t, err)
		require.Equal(t, "CA v1", parsed1.Subject.CommonName)
		require.NotNil(t, key1)

		cert2 := m.Get("second")
		require.NotNil(t, cert2)
		parsed2, key2, err := cert2.AsLeafCertificate()
		require.NoError(t, err)
		require.Equal(t, "CA v2", parsed2.Subject.CommonName)
		require.NotNil(t, key2)
	})

	t.Run("Get unknown returns nil", func(t *testing.T) {
		require.Nil(t, m.Get("unknown"))
	})

	t.Run("GetPaths returns registered paths", func(t *testing.T) {
		gotCertPath, gotKeyPath := m.GetPaths("first")
		require.Equal(t, certPath1, gotCertPath)
		require.Equal(t, keyPath1, gotKeyPath)

		gotCertPath2, gotKeyPath2 := m.GetPaths("second")
		require.Equal(t, certPath2, gotCertPath2)
		require.Equal(t, keyPath2, gotKeyPath2)
	})

	t.Run("GetPaths unknown returns empty strings", func(t *testing.T) {
		gotCertPath, gotKeyPath := m.GetPaths("unknown")
		require.Empty(t, gotCertPath)
		require.Empty(t, gotKeyPath)
	})

	t.Run("Reload picks up new certs", func(t *testing.T) {
		// Write new certificates to both paths.
		ca3 := generateTestCA(t, "CA v3")
		certPEM3, keyPEM3 := ca3.Get()
		require.NoError(t, os.WriteFile(certPath1, certPEM3, 0600))
		require.NoError(t, os.WriteFile(keyPath1, keyPEM3, 0600))

		ca4 := generateTestCA(t, "CA v4")
		certPEM4, keyPEM4 := ca4.Get()
		require.NoError(t, os.WriteFile(certPath2, certPEM4, 0600))
		require.NoError(t, os.WriteFile(keyPath2, keyPEM4, 0600))

		require.NoError(t, m.Reload(t.Context()))

		parsed1, _, _ := m.Get("first").AsLeafCertificate()
		require.Equal(t, "CA v3", parsed1.Subject.CommonName)

		parsed2, _, _ := m.Get("second").AsLeafCertificate()
		require.Equal(t, "CA v4", parsed2.Subject.CommonName)
	})

	t.Run("Register with missing file fails", func(t *testing.T) {
		err := m.Register("missing", "/nonexistent/cert.pem", "/nonexistent/key.pem")
		require.Error(t, err)
	})

	t.Run("Reload with missing file fails but preserves old cert", func(t *testing.T) {
		// Create a new manager with a valid cert.
		dir2 := t.TempDir()
		certPath := filepath.Join(dir2, "cert.pem")
		keyPath := filepath.Join(dir2, "key.pem")

		ca := generateTestCA(t, "Original CA")
		certPEM, keyPEM := ca.Get()
		require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
		require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))

		stopper2 := stop.NewStopper()
		defer stopper2.Stop(ctx)
		m2, err := NewManager(ctx, stopper2)
		require.NoError(t, err)
		require.NoError(t, m2.Register("test", certPath, keyPath))

		// Verify initial cert loaded correctly.
		cert := m2.Get("test")
		parsed, _, err := cert.AsLeafCertificate()
		require.NoError(t, err)
		require.Equal(t, "Original CA", parsed.Subject.CommonName)

		// Delete the cert file to cause Reload to fail.
		require.NoError(t, os.Remove(certPath))

		// Reload should fail.
		err = m2.Reload(t.Context())
		require.Error(t, err)
		require.Contains(t, err.Error(), "1 certs (out of 1) failed to reload")

		// Old cert should still be accessible.
		parsed, _, err = cert.AsLeafCertificate()
		require.NoError(t, err)
		require.Equal(t, "Original CA", parsed.Subject.CommonName)
	})

	t.Run("SIGHUP reloads certs", func(t *testing.T) {
		// Create a new manager with two registered certs.
		dir2 := t.TempDir()
		certPathA := filepath.Join(dir2, "certA.pem")
		keyPathA := filepath.Join(dir2, "keyA.pem")
		certPathB := filepath.Join(dir2, "certB.pem")
		keyPathB := filepath.Join(dir2, "keyB.pem")

		caA := generateTestCA(t, "CA A1")
		certPEMA, keyPEMA := caA.Get()
		require.NoError(t, os.WriteFile(certPathA, certPEMA, 0600))
		require.NoError(t, os.WriteFile(keyPathA, keyPEMA, 0600))

		caB := generateTestCA(t, "CA B1")
		certPEMB, keyPEMB := caB.Get()
		require.NoError(t, os.WriteFile(certPathB, certPEMB, 0600))
		require.NoError(t, os.WriteFile(keyPathB, keyPEMB, 0600))

		stopper3 := stop.NewStopper()
		defer stopper3.Stop(ctx)
		m2, err := NewManager(ctx, stopper3)
		require.NoError(t, err)
		require.NoError(t, m2.Register("certA", certPathA, keyPathA))
		require.NoError(t, m2.Register("certB", certPathB, keyPathB))

		// Verify initial certs loaded correctly.
		certA := m2.Get("certA")
		parsedA, _, _ := certA.AsLeafCertificate()
		require.Equal(t, "CA A1", parsedA.Subject.CommonName)

		certB := m2.Get("certB")
		parsedB, _, _ := certB.AsLeafCertificate()
		require.Equal(t, "CA B1", parsedB.Subject.CommonName)

		// Write new certs and send SIGHUP.
		caA2 := generateTestCA(t, "CA A2")
		certPEMA2, keyPEMA2 := caA2.Get()
		require.NoError(t, os.WriteFile(certPathA, certPEMA2, 0600))
		require.NoError(t, os.WriteFile(keyPathA, keyPEMA2, 0600))

		caB2 := generateTestCA(t, "CA B2")
		certPEMB2, keyPEMB2 := caB2.Get()
		require.NoError(t, os.WriteFile(certPathB, certPEMB2, 0600))
		require.NoError(t, os.WriteFile(keyPathB, keyPEMB2, 0600))

		require.NoError(t, unix.Kill(unix.Getpid(), unix.SIGHUP))

		// Wait for reload to complete - both certs should be updated.
		require.Eventually(t, func() bool {
			parsedA, _, _ := certA.AsLeafCertificate()
			parsedB, _, _ := certB.AsLeafCertificate()
			return parsedA.Subject.CommonName == "CA A2" && parsedB.Subject.CommonName == "CA B2"
		}, time.Second, 10*time.Millisecond)
	})
}
