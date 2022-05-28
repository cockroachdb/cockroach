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

// A CertsLocator provides locations to certificates.
type CertsLocator struct {
	certsDir string
}

// MakeCertsLocator initializes a CertsLocator.
func MakeCertsLocator(certsDir string) CertsLocator {
	return CertsLocator{certsDir: certsDir}
}

// CertsDir retrieves the configured certificate directory.
func (cl CertsLocator) CertsDir() string {
	return cl.certsDir
}

// CACertPath returns the expected file path for the CA certificate.
func (cl CertsLocator) CACertPath() string {
	return filepath.Join(cl.certsDir, CACertFilename())
}

// FullPath takes a CertInfo and returns the full path for it.
func (cl CertsLocator) FullPath(fileName string) string {
	return filepath.Join(cl.certsDir, fileName)
}

// EnsureCertsDirectory ensures that the certs directory exists by
// creating it if does not exist yet.
func (cl CertsLocator) EnsureCertsDirectory() error {
	return os.MkdirAll(cl.certsDir, 0700)
}

// CAKeyPath returns the expected file path for the CA certificate.
func (cl CertsLocator) CAKeyPath() string {
	return filepath.Join(cl.certsDir, CAKeyFilename())
}

// TenantCACertPath returns the expected file path for the Tenant client CA
// certificate.
func (cl CertsLocator) TenantCACertPath() string {
	return filepath.Join(cl.certsDir, TenantClientCACertFilename())
}

// ClientCACertPath returns the expected file path for the CA certificate
// used to verify client certificates.
func (cl CertsLocator) ClientCACertPath() string {
	return filepath.Join(cl.certsDir, ClientCACertFilename())
}

// ClientCAKeyPath returns the expected file path for the CA key
// used to sign client certificates.
func (cl CertsLocator) ClientCAKeyPath() string {
	return filepath.Join(cl.certsDir, ClientCAKeyFilename())
}

// ClientNodeCertPath returns the expected file path for the certificate used
// by other nodes to verify outgoing RPCs from this node.
func (cl CertsLocator) ClientNodeCertPath() string {
	return filepath.Join(cl.certsDir, ClientCertFilename(username.NodeUserName()))
}

// ClientNodeKeyPath returns the expected file path for the key used
// to sign outgoing RPCs.
func (cl CertsLocator) ClientNodeKeyPath() string {
	return filepath.Join(cl.certsDir, ClientKeyFilename(username.NodeUserName()))
}

// UICACertPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl CertsLocator) UICACertPath() string {
	return filepath.Join(cl.certsDir, UICACertFilename())
}

// UICAKeyPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl CertsLocator) UICAKeyPath() string {
	return filepath.Join(cl.certsDir, UICAKeyFilename())
}

// NodeCertPath returns the expected file path for the node certificate.
func (cl CertsLocator) NodeCertPath() string {
	return filepath.Join(cl.certsDir, NodeCertFilename())
}

// NodeKeyPath returns the expected file path for the node key.
func (cl CertsLocator) NodeKeyPath() string {
	return filepath.Join(cl.certsDir, NodeKeyFilename())
}

// UICertPath returns the expected file path for the UI certificate.
func (cl CertsLocator) UICertPath() string {
	return filepath.Join(cl.certsDir, UIServerCertFilename())
}

// UIKeyPath returns the expected file path for the UI key.
func (cl CertsLocator) UIKeyPath() string {
	return filepath.Join(cl.certsDir, UIServerKeyFilename())
}

// TenantCertPath returns the expected file path for the user's certificate.
func (cl CertsLocator) TenantCertPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantCertFilename(tenantIdentifier))
}

// TenantKeyPath returns the expected file path for the tenant's key.
func (cl CertsLocator) TenantKeyPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantKeyFilename(tenantIdentifier))
}

// TenantSigningCertPath returns the expected file path for the node certificate.
func (cl CertsLocator) TenantSigningCertPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantSigningCertFilename(tenantIdentifier))
}

// TenantSigningKeyPath returns the expected file path for the node key.
func (cl CertsLocator) TenantSigningKeyPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantSigningKeyFilename(tenantIdentifier))
}

// ClientCertPath returns the expected file path for the user's certificate.
func (cl CertsLocator) ClientCertPath(user username.SQLUsername) string {
	return filepath.Join(cl.certsDir, ClientCertFilename(user))
}

// ClientKeyPath returns the expected file path for the user's key.
func (cl CertsLocator) ClientKeyPath(user username.SQLUsername) string {
	return filepath.Join(cl.certsDir, ClientKeyFilename(user))
}

// SQLServiceCertPath returns the expected file path for the
// SQL service certificate
func (cl CertsLocator) SQLServiceCertPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCertFilename())
}

// SQLServiceKeyPath returns the expected file path for the SQL service key
func (cl CertsLocator) SQLServiceKeyPath() string {
	return filepath.Join(cl.certsDir, SQLServiceKeyFilename())
}

// SQLServiceCACertPath returns the expected file path for the
// SQL CA certificate
func (cl CertsLocator) SQLServiceCACertPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCACertFilename())
}

// SQLServiceCAKeyPath returns the expected file path for the SQL CA key
func (cl CertsLocator) SQLServiceCAKeyPath() string {
	return filepath.Join(cl.certsDir, SQLServiceCAKeyFilename())
}

// RPCServiceCertPath returns the expected file path for the
// RPC service certificate
func (cl CertsLocator) RPCServiceCertPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCertFilename())
}

// RPCServiceKeyPath returns the expected file path for the RPC service key
func (cl CertsLocator) RPCServiceKeyPath() string {
	return filepath.Join(cl.certsDir, RPCServiceKeyFilename())
}

// RPCServiceCACertPath returns the expected file path for the
// RPC service certificate
func (cl CertsLocator) RPCServiceCACertPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCACertFilename())
}

// RPCServiceCAKeyPath returns the expected file path for the RPC service key
func (cl CertsLocator) RPCServiceCAKeyPath() string {
	return filepath.Join(cl.certsDir, RPCServiceCAKeyFilename())
}
