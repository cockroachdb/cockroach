// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"context"
	"crypto/x509"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

func init() {
	if runtime.GOOS == "windows" {
		// File modes on windows default to 0666 for r/w files:
		// https://golang.org/src/os/types_windows.go?#L31
		// This would fail any attempt to load keys, so we need to disable permission checks.
		skipPermissionChecks = true
	} else {
		skipPermissionChecks = envutil.EnvOrDefaultBool("COCKROACH_SKIP_KEY_PERMISSION_CHECK", false)
	}
}

var skipPermissionChecks bool

// PemUsage indicates the purpose of a given certificate.
type PemUsage uint32

const (
	_ PemUsage = iota
	// CAPem describes the main CA certificate.
	CAPem
	// TenantCAPem describes the CA certificate used to broker authN/Z for SQL
	// tenants wishing to access the KV layer.
	TenantCAPem
	// ClientCAPem describes the CA certificate used to verify client certificates.
	ClientCAPem
	// UICAPem describes the CA certificate used to verify the Admin UI server certificate.
	UICAPem
	// NodePem describes the server certificate for the node, possibly a combined server/client
	// certificate for user Node if a separate 'client.node.crt' is not present.
	NodePem
	// UIPem describes the server certificate for the admin UI.
	UIPem
	// ClientPem describes a client certificate.
	ClientPem
	// TenantPem describes a SQL tenant client certificate.
	TenantPem
	// TenantSigningPem describes a SQL tenant signing certificate.
	TenantSigningPem

	// Maximum allowable permissions.
	maxKeyPermissions os.FileMode = 0700

	// Maximum allowable permissions if file is owned by root.
	maxGroupKeyPermissions os.FileMode = 0740

	// Certificate directory permissions.
	defaultCertsDirPerm = 0700
)

func isCA(usage PemUsage) bool {
	return usage == CAPem || usage == ClientCAPem || usage == TenantCAPem || usage == UICAPem
}

func (p PemUsage) String() string {
	switch p {
	case CAPem:
		return "CA"
	case ClientCAPem:
		return "Client CA"
	case TenantCAPem:
		return "Tenant Client CA"
	case UICAPem:
		return "UI CA"
	case NodePem:
		return "Node"
	case UIPem:
		return "UI"
	case ClientPem:
		return "Client"
	case TenantPem:
		return "Tenant Client"
	case TenantSigningPem:
		return "Tenant Signing"
	default:
		return "unknown"
	}
}

// CertInfo describe a certificate file and optional key file.
// To obtain the full path, Filename and KeyFilename must be joined
// with the certs directory.
// The key may not be present if this is a CA certificate.
// If Err != nil, the CertInfo must NOT be used.
type CertInfo struct {
	// FileUsage describes the use of this certificate.
	FileUsage PemUsage

	// Filename is the base filename of the certificate.
	Filename string
	// FileContents is the raw cert file data.
	FileContents []byte

	// KeyFilename is the base filename of the key, blank if not found (CA certs only).
	KeyFilename string
	// KeyFileContents is the raw key file data.
	KeyFileContents []byte

	// Name is the blob in the middle of the filename. eg: username for client certs.
	Name string

	// Parsed certificates. This is used by debugging/printing/monitoring only,
	// TLS config objects are passed raw certificate file contents.
	// CA certs may contain (and use) more than one certificate.
	// Client/Server certs may contain more than one, but only the first certificate will be used.
	ParsedCertificates []*x509.Certificate

	// Expiration time is the latest "Not After" date across all parsed certificates.
	ExpirationTime time.Time

	// Error is any error encountered when loading the certificate/key pair.
	// For example: bad permissions on the key will be stored here.
	Error error
}

// CertInfoFromFilename takes a filename and attempts to determine the
// certificate usage (ca, node, etc..).
func CertInfoFromFilename(filename string) (*CertInfo, error) {
	parts := strings.Split(filename, `.`)
	numParts := len(parts)

	if numParts < 2 {
		return nil, errors.New("not enough parts found")
	}

	var fileUsage PemUsage
	var name string
	prefix := parts[0]
	switch parts[0] {
	case `ca`:
		fileUsage = CAPem
		if numParts != 2 {
			return nil, errors.Errorf("CA certificate filename should match %s", certnames.CACertFilename())
		}
	case `ca-client`:
		fileUsage = ClientCAPem
		if numParts != 2 {
			return nil, errors.Errorf("client CA certificate filename should match %s", certnames.ClientCACertFilename())
		}
	case `ca-client-tenant`:
		fileUsage = TenantCAPem
		if numParts != 2 {
			return nil, errors.Errorf("tenant CA certificate filename should match %s", certnames.TenantClientCACertFilename())
		}
	case `ca-ui`:
		fileUsage = UICAPem
		if numParts != 2 {
			return nil, errors.Errorf("UI CA certificate filename should match %s", certnames.UICACertFilename())
		}
	case `node`:
		fileUsage = NodePem
		if numParts != 2 {
			return nil, errors.Errorf("node certificate filename should match %s", certnames.NodeCertFilename())
		}
	case `ui`:
		fileUsage = UIPem
		if numParts != 2 {
			return nil, errors.Errorf("UI certificate filename should match %s", certnames.UIServerCertFilename())
		}
	case `client`:
		fileUsage = ClientPem
		// Strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("client certificate filename should match %s",
				certnames.ClientCertFilename(username.MakeSQLUsernameFromPreNormalizedString("<user>")))
		}
	case `client-tenant`:
		fileUsage = TenantPem
		// Strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("tenant certificate filename should match %s",
				certnames.TenantCertFilename("<tenantid>"))
		}
	case `tenant-signing`:
		fileUsage = TenantSigningPem
		// Strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("tenant signing certificate filename should match %s",
				certnames.TenantSigningCertFilename("<tenantid>"))
		}
	default:
		return nil, errors.Errorf("unknown prefix %q", prefix)
	}

	return &CertInfo{
		FileUsage: fileUsage,
		Filename:  filename,
		Name:      name,
	}, nil
}

// CertificateLoader searches for certificates and keys in the certs directory.
type CertificateLoader struct {
	certsDir             string
	skipPermissionChecks bool
	certificates         []*CertInfo
}

// Certificates returns the loaded certificates.
func (cl *CertificateLoader) Certificates() []*CertInfo {
	return cl.certificates
}

// NewCertificateLoader creates a new instance of the certificate loader.
func NewCertificateLoader(certsDir string) *CertificateLoader {
	return &CertificateLoader{
		certsDir:             certsDir,
		skipPermissionChecks: skipPermissionChecks,
		certificates:         make([]*CertInfo, 0),
	}
}

// MaybeCreateCertsDir creates the certificate directory if it does not
// exist. Returns an error if we could not stat or create the directory.
func (cl *CertificateLoader) MaybeCreateCertsDir() error {
	dirInfo, err := os.Stat(cl.certsDir)
	if err == nil {
		if !dirInfo.IsDir() {
			return errors.Errorf("certs directory %s exists but is not a directory", cl.certsDir)
		}
		return nil
	}

	if !oserror.IsNotExist(err) {
		return makeErrorf(err, "could not stat certs directory %s", cl.certsDir)
	}

	if err := os.Mkdir(cl.certsDir, defaultCertsDirPerm); err != nil {
		return makeErrorf(err, "could not create certs directory %s", cl.certsDir)
	}
	return nil
}

// TestDisablePermissionChecks turns off permissions checks.
// Used by tests only.
func (cl *CertificateLoader) TestDisablePermissionChecks() {
	cl.skipPermissionChecks = true
}

// Load examines all .crt files in the certs directory, determines their
// usage, and looks for their keys.
// It populates the certificates field.
func (cl *CertificateLoader) Load() error {
	fileInfos, err := securityassets.GetLoader().ReadDir(cl.certsDir)
	if err != nil {
		if oserror.IsNotExist(err) {
			// Directory does not exist.
			if log.V(3) {
				log.Infof(context.Background(), "missing certs directory %s", cl.certsDir)
			}
			return nil
		}
		return err
	}

	if log.V(3) {
		log.Infof(context.Background(), "scanning certs directory %s", cl.certsDir)
	}

	// Walk the directory contents.
	for _, info := range fileInfos {
		filename := info.Name()
		fullPath := filepath.Join(cl.certsDir, filename)

		if info.IsDir() {
			// Skip subdirectories.
			if log.V(3) {
				log.Infof(context.Background(), "skipping sub-directory %s", fullPath)
			}
			continue
		}

		if !certnames.IsCertificateFilename(filename) {
			if log.V(3) {
				log.Infof(context.Background(), "skipping non-certificate file %s", filename)
			}
			continue
		}

		// Build the info struct from the filename.
		ci, err := CertInfoFromFilename(filename)
		if err != nil {
			log.Warningf(context.Background(), "bad filename %s: %v", fullPath, err)
			continue
		}

		// Read the cert file contents.
		fullCertPath := filepath.Join(cl.certsDir, filename)
		certPEMBlock, err := securityassets.GetLoader().ReadFile(fullCertPath)
		if err != nil {
			log.Warningf(context.Background(), "could not read certificate file %s: %v", fullPath, err)
		}
		ci.FileContents = certPEMBlock

		// Parse certificate, then look for the private key.
		// Errors are persisted for better visibility later.
		if err := parseCertificate(ci); err != nil {
			log.Warningf(context.Background(), "could not parse certificate for %s: %v", fullPath, err)
			ci.Error = err
		} else if err := cl.findKey(ci); err != nil {
			log.Warningf(context.Background(), "error finding key for %s: %v", fullPath, err)
			ci.Error = err
		} else if log.V(3) {
			log.Infof(context.Background(), "found certificate %s", ci.Filename)
		}

		cl.certificates = append(cl.certificates, ci)
	}

	return nil
}

// findKey takes a CertInfo and looks for the corresponding key file.
// If found, sets the 'keyFilename' and returns nil, returns error otherwise.
// Does not load CA keys.
func (cl *CertificateLoader) findKey(ci *CertInfo) error {
	if isCA(ci.FileUsage) {
		return nil
	}

	keyFilename := certnames.KeyForCert(ci.Filename)
	fullKeyPath := filepath.Join(cl.certsDir, keyFilename)

	// Stat the file. This follows symlinks.
	info, err := securityassets.GetLoader().Stat(fullKeyPath)
	if err != nil {
		return errors.Wrapf(err, "could not stat key file %s", fullKeyPath)
	}

	// Only regular files are supported (after following symlinks).
	fileMode := info.Mode()
	if !fileMode.IsRegular() {
		return errors.Errorf("key file %s is not a regular file", fullKeyPath)
	}

	if !cl.skipPermissionChecks {
		aclInfo := sysutil.GetFileACLInfo(info)
		if err = checkFilePermissions(os.Getgid(), fullKeyPath, aclInfo); err != nil {
			return err
		}
	}

	// Read key file.
	keyPEMBlock, err := securityassets.GetLoader().ReadFile(fullKeyPath)
	if err != nil {
		return errors.Wrapf(err, "could not read key file %s", fullKeyPath)
	}

	ci.KeyFilename = keyFilename
	ci.KeyFileContents = keyPEMBlock
	return nil
}

// parseCertificate attempts to parse the cert file contents into x509 certificate objects.
// The Error field must be nil
func parseCertificate(ci *CertInfo) error {
	if ci.Error != nil {
		return makeErrorf(ci.Error, "parseCertificate called on bad CertInfo object: %s", ci.Filename)
	}

	if len(ci.FileContents) == 0 {
		return errors.Errorf("empty certificate file: %s", ci.Filename)
	}

	// PEM-decode the file.
	derCerts, err := PEMToCertificates(ci.FileContents)
	if err != nil {
		return makeErrorf(err, "failed to parse certificate file %s as PEM", ci.Filename)
	}

	// Make sure we get at least one certificate.
	if len(derCerts) == 0 {
		return errors.Errorf("no certificates found in %s", ci.Filename)
	}

	certs := make([]*x509.Certificate, len(derCerts))
	var expires time.Time
	for i, c := range derCerts {
		x509Cert, err := x509.ParseCertificate(c.Bytes)
		if err != nil {
			return makeErrorf(err, "failed to parse certificate %d in file %s", i, ci.Filename)
		}

		if i == 0 {
			// Expiration from the first certificate.
			expires = x509Cert.NotAfter
		}
		certs[i] = x509Cert
	}

	ci.ParsedCertificates = certs
	ci.ExpirationTime = expires
	return nil
}
