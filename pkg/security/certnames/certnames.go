// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package certnames

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
)

const (
	certExtension = `.crt`
	keyExtension  = `.key`
)

// IsCertificateFilename returns true if the file name looks like a certificate file.
func IsCertificateFilename(filename string) bool {
	return strings.HasSuffix(filename, certExtension)
}

// KeyForCert returns the expected key file name for the given cert file name.
// The caller is responsible for calling IsCertFile beforehand.
func KeyForCert(certFile string) string {
	return strings.TrimSuffix(certFile, certExtension) + keyExtension
}

// CACertFilename returns the expected file name for the CA certificate.
func CACertFilename() string { return "ca" + certExtension }

// CAKeyFilename returns the expected file name for the CA certificate.
func CAKeyFilename() string { return "ca" + keyExtension }

// TenantClientCACertFilename returns the expected file name for the Tenant CA
// certificate.
func TenantClientCACertFilename() string {
	return "ca-client-tenant" + certExtension
}

// ClientCACertFilename returns the expected file name for the client CA certificate.
func ClientCACertFilename() string { return "ca-client" + certExtension }

// ClientCAKeyFilename returns the expected file name for the client CA key.
func ClientCAKeyFilename() string { return "ca-client" + keyExtension }

// UICACertFilename returns the expected file name for the HTTP CA certificate.
func UICACertFilename() string { return "ca-ui" + certExtension }

// UICAKeyFilename returns the expected file name for the HTTP CA key.
func UICAKeyFilename() string { return "ca-ui" + keyExtension }

// ClientCertFilename returns the expected file name for the user's client certificate.
func ClientCertFilename(user username.SQLUsername) string {
	return "client." + user.Normalized() + certExtension
}

// ClientKeyFilename returns the expected file name for the user's client cert key.
func ClientKeyFilename(user username.SQLUsername) string {
	return "client." + user.Normalized() + keyExtension
}

// UIServerCertFilename returns the expected file name for the HTTP CA certificate.
func UIServerCertFilename() string { return "ui" + certExtension }

// UIServerKeyFilename returns the expected file name for the HTTP CA key.
func UIServerKeyFilename() string { return "ui" + keyExtension }

// NodeCertFilename returns the expected file name for the node server certificate.
func NodeCertFilename() string {
	return "node" + certExtension
}

// NodeKeyFilename returns the expected file name for the node server key.
func NodeKeyFilename() string {
	return "node" + keyExtension
}

// TenantCertFilename returns the expected file name for the user's tenant client certificate.
func TenantCertFilename(tenantIdentifier string) string {
	return "client-tenant." + tenantIdentifier + certExtension
}

// TenantKeyFilename returns the expected file name for the user's tenant client key.
func TenantKeyFilename(tenantIdentifier string) string {
	return "client-tenant." + tenantIdentifier + keyExtension
}

// TenantSigningCertFilename returns the expected file name for the
// tenant signing certificate. (Used for session migration tokens.)
func TenantSigningCertFilename(tenantIdentifier string) string {
	return "tenant-signing." + tenantIdentifier + certExtension
}

// TenantSigningKeyFilename returns the expected file name for the tenant signing key.
func TenantSigningKeyFilename(tenantIdentifier string) string {
	return "tenant-signing." + tenantIdentifier + keyExtension
}

// SQLServiceCertFilename returns the expected file name for the SQL service
// certificate.
func SQLServiceCertFilename() string {
	return "service.sql" + certExtension
}

// SQLServiceKeyFilename returns the expected file name for the SQL service
// certificate.
func SQLServiceKeyFilename() string {
	return "service.sql" + keyExtension
}

// SQLServiceCACertFilename returns the expected file name for the SQL CA
// certificate.
func SQLServiceCACertFilename() string {
	return "service.ca.sql" + certExtension
}

// SQLServiceCAKeyFilename returns the expected file name for the SQL CA
// certificate.
func SQLServiceCAKeyFilename() string {
	return "service.ca.sql" + keyExtension
}

// RPCServiceCertFilename returns the expected file name for the RPC service
// certificate.
func RPCServiceCertFilename() string {
	return "service.rpc" + certExtension
}

// RPCServiceKeyFilename returns the expected file name for the RPC service
// certificate.
func RPCServiceKeyFilename() string {
	return "service.rpc" + keyExtension
}

// RPCServiceCACertFilename returns the expected file name for the RPC service
// certificate.
func RPCServiceCACertFilename() string {
	return "service.ca.rpc" + certExtension
}

// RPCServiceCAKeyFilename returns the expected file name for the RPC service
// certificate.
func RPCServiceCAKeyFilename() string {
	return "service.ca.rpc" + keyExtension
}
