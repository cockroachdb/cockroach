// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certnames

import (
	"os"
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

// FullPath takes a CertInfo and returns the full path for it.
func (cl Locator) FullPath(fileName string) string {
	return filepath.Join(cl.certsDir, fileName)
}

// EnsureCertsDirectory ensures that the certs directory exists by
// creating it if does not exist yet.
func (cl Locator) EnsureCertsDirectory() error {
	return os.MkdirAll(cl.certsDir, 0700)
}

// CAKeyPath returns the expected file path for the CA certificate.
func (cl Locator) CAKeyPath() string {
	return filepath.Join(cl.certsDir, CAKeyFilename())
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

// ClientCAKeyPath returns the expected file path for the CA key
// used to sign client certificates.
func (cl Locator) ClientCAKeyPath() string {
	return filepath.Join(cl.certsDir, ClientCAKeyFilename())
}

// ClientNodeCertPath returns the expected file path for the certificate used
// by other nodes to verify outgoing RPCs from this node.
func (cl Locator) ClientNodeCertPath() string {
	return filepath.Join(cl.certsDir, ClientCertFilename(username.NodeUserName()))
}

// ClientNodeKeyPath returns the expected file path for the key used
// to sign outgoing RPCs.
func (cl Locator) ClientNodeKeyPath() string {
	return filepath.Join(cl.certsDir, ClientKeyFilename(username.NodeUserName()))
}

// UICACertPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl Locator) UICACertPath() string {
	return filepath.Join(cl.certsDir, UICACertFilename())
}

// UICAKeyPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl Locator) UICAKeyPath() string {
	return filepath.Join(cl.certsDir, UICAKeyFilename())
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

// SQLServiceCertPath returns the expected file path for the
// SQL service certificate
func (cl Locator) SQLServiceCertPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCertFilename())
}

// SQLServiceKeyPath returns the expected file path for the SQL service key
func (cl Locator) SQLServiceKeyPath() string {
	return filepath.Join(cl.certsDir, SQLServiceKeyFilename())
}

// SQLServiceCACertPath returns the expected file path for the
// SQL CA certificate
func (cl Locator) SQLServiceCACertPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCACertFilename())
}

// SQLServiceCAKeyPath returns the expected file path for the SQL CA key
func (cl Locator) SQLServiceCAKeyPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCAKeyFilename())
}

// RPCServiceCertPath returns the expected file path for the
// RPC service certificate
func (cl Locator) RPCServiceCertPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCertFilename())
}

// RPCServiceKeyPath returns the expected file path for the RPC service key
func (cl Locator) RPCServiceKeyPath() string {
	return filepath.Join(cl.certsDir, RPCServiceKeyFilename())
}

// RPCServiceCACertPath returns the expected file path for the
// RPC service certificate
func (cl Locator) RPCServiceCACertPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCACertFilename())
}

// RPCServiceCAKeyPath returns the expected file path for the RPC service key
func (cl Locator) RPCServiceCAKeyPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCAKeyFilename())
}
