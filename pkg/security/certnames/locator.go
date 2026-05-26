// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package certnames

import (
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/security/username"
)

// A Locator provides locations to certificates.
type Locator struct {
	certsDir string
}

// MakeLocator initializes a Locator.
func MakeLocator(certsDir string) Locator {
	return Locator{certsDir: certsDir}
}

// CertsDir retrieves the configured certificate directory.
func (cl Locator) CertsDir() string {
	return cl.certsDir
}

// CACertPath returns the expected file path for the CA certificate.
func (cl Locator) CACertPath() string {
	return filepath.Join(cl.certsDir, CACertFilename())
}

// TenantCACertPath returns the expected file path for the Tenant client CA
// certificate.
func (cl Locator) TenantCACertPath() string {
	return filepath.Join(cl.certsDir, TenantClientCACertFilename())
}

// ClientCACertPath returns the expected file path for the CA certificate
// used to verify client certificates.
func (cl Locator) ClientCACertPath() string {
	return filepath.Join(cl.certsDir, ClientCACertFilename())
}

// UICACertPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl Locator) UICACertPath() string {
	return filepath.Join(cl.certsDir, UICACertFilename())
}

// NodeCertPath returns the expected file path for the node certificate.
func (cl Locator) NodeCertPath() string {
	return filepath.Join(cl.certsDir, NodeCertFilename())
}

// NodeKeyPath returns the expected file path for the node key.
func (cl Locator) NodeKeyPath() string {
	return filepath.Join(cl.certsDir, NodeKeyFilename())
}

// UICertPath returns the expected file path for the UI certificate.
func (cl Locator) UICertPath() string {
	return filepath.Join(cl.certsDir, UIServerCertFilename())
}

// UIKeyPath returns the expected file path for the UI key.
func (cl Locator) UIKeyPath() string {
	return filepath.Join(cl.certsDir, UIServerKeyFilename())
}

// TenantCertPath returns the expected file path for the user's certificate.
func (cl Locator) TenantCertPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantCertFilename(tenantIdentifier))
}

// TenantKeyPath returns the expected file path for the tenant's key.
func (cl Locator) TenantKeyPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantKeyFilename(tenantIdentifier))
}

// TenantSigningCertPath returns the expected file path for the node certificate.
func (cl Locator) TenantSigningCertPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantSigningCertFilename(tenantIdentifier))
}

// TenantSigningKeyPath returns the expected file path for the node key.
func (cl Locator) TenantSigningKeyPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantSigningKeyFilename(tenantIdentifier))
}

// ClientCertPath returns the expected file path for the user's certificate.
func (cl Locator) ClientCertPath(user username.SQLUsername) string {
	return filepath.Join(cl.certsDir, ClientCertFilename(user))
}

// ClientKeyPath returns the expected file path for the user's key.
func (cl Locator) ClientKeyPath(user username.SQLUsername) string {
	return filepath.Join(cl.certsDir, ClientKeyFilename(user))
}
