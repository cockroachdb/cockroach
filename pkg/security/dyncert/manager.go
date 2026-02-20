// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dyncert

import (
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

// registration tracks a managed cert and its file paths.
type registration struct {
	cert     *Cert
	certPath string
	keyPath  string
}

// Manager loads certificates from disk and reloads them on SIGHUP.
//
// This enables certificate rotation without process restarts. The typical flow:
//  1. Create a Manager with NewManager(ctx, stopper)
//  2. Register certificates with their file paths
//  3. Use Get() to retrieve Cert objects for TLS configuration
//  4. When certificates need rotation, update the files and send SIGHUP
//
// The Manager updates the underlying Cert objects in place, so any code
// holding references to them automatically sees the new certificates.
// The Manager's background goroutine will stop when the stopper quiesces.
type Manager struct {
	mu struct {
		syncutil.Mutex
		registrations map[string]registration
	}
}

// NewManager creates a new certificate manager tied to the given stopper.
// The Manager immediately starts listening for SIGHUP signals to reload
// certificates. The background goroutine stops when the stopper quiesces.
func NewManager(ctx context.Context, stopper *stop.Stopper) (*Manager, error) {
	m := &Manager{}
	m.mu.registrations = make(map[string]registration)

	// Register for SIGHUP before spawning the goroutine so signals sent
	// immediately after NewManager returns are not lost.
	sigCh := sysutil.RefreshSignaledChan()

	if err := stopper.RunAsyncTask(ctx, "dyncert-manager", func(ctx context.Context) {
		for {
			select {
			case sig := <-sigCh:
				log.Ops.Infof(ctx, "received signal %q, triggering certificate reload", sig)
				if err := m.Reload(ctx); err != nil {
					log.Ops.Warningf(ctx, "cert reload failed on SIGHUP: %v", err)
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	}); err != nil {
		return nil, err
	}

	return m, nil
}

// Register loads a certificate from disk and tracks it for reloading.
//
// The name is an arbitrary identifier used with Get() to retrieve the Cert.
// If keyPath is empty, only the certificate is loaded (typical for CA
// certificates that are only used to verify peers, not authenticate).
func (m *Manager) Register(name, certPath, keyPath string) error {
	cert := &Cert{}
	if err := m.loadCert(cert, certPath, keyPath); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.registrations[name] = registration{
		cert:     cert,
		certPath: certPath,
		keyPath:  keyPath,
	}

	return nil
}

// Get returns the certificate registered with the given name.
// Returns nil if no certificate with that name exists.
func (m *Manager) Get(name string) *Cert {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reg, ok := m.mu.registrations[name]; ok {
		return reg.cert
	}
	return nil
}

// GetPaths returns the cert and key file paths for the registered certificate.
// Returns empty strings if no certificate with that name exists.
func (m *Manager) GetPaths(name string) (certPath, keyPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reg, ok := m.mu.registrations[name]; ok {
		return reg.certPath, reg.keyPath
	}
	return "", ""
}

// loadCert reads certificate and key from disk and updates the Cert.
// If keyPath is empty, only the certificate is loaded (for CA certs).
func (m *Manager) loadCert(cert *Cert, certPath, keyPath string) error {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return errors.Wrapf(err, "read cert %s", certPath)
	}
	var keyPEM []byte
	if keyPath != "" {
		keyPEM, err = os.ReadFile(keyPath)
		if err != nil {
			return errors.Wrapf(err, "read key %s", keyPath)
		}
	}
	cert.Set(certPEM, keyPEM)
	return nil
}

// Reload reloads all registered certificates from disk.
// Returns the first error encountered, but attempts to reload all certificates.
func (m *Manager) Reload(ctx context.Context) error {
	regs := func() map[string]registration {
		m.mu.Lock()
		defer m.mu.Unlock()
		regs := make(map[string]registration, len(m.mu.registrations))
		for name, reg := range m.mu.registrations {
			regs[name] = reg
		}
		return regs
	}()

	errCount := 0
	for _, reg := range regs {
		if err := m.loadCert(reg.cert, reg.certPath, reg.keyPath); err != nil {
			log.Ops.Warningf(ctx, "could not reload cert: %v", err)
			errCount++
		}
	}

	if errCount > 0 {
		err := errors.Errorf("%d certs (out of %d) failed to reload", errCount, len(regs))
		log.StructuredEvent(ctx, severity.INFO, &eventpb.CertsReload{
			Success:      false,
			ErrorMessage: err.Error(),
		})
		return err
	}
	log.StructuredEvent(ctx, severity.INFO, &eventpb.CertsReload{Success: true})

	return nil
}
