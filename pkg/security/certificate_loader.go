// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// AssetLoader describes the functions necessary to read certificate and key files.
type AssetLoader struct {
	ReadDir  func(dirname string) ([]os.FileInfo, error)
	ReadFile func(filename string) ([]byte, error)
	Stat     func(name string) (os.FileInfo, error)
}

// defaultAssetLoader uses real filesystem calls.
var defaultAssetLoader = AssetLoader{
	ReadDir:  ioutil.ReadDir,
	ReadFile: ioutil.ReadFile,
	Stat:     os.Stat,
}

// assetLoaderImpl is used to list/read/stat security assets.
var assetLoaderImpl = defaultAssetLoader

// GetAssetLoader returns the active asset loader.
func GetAssetLoader() AssetLoader {
	return assetLoaderImpl
}

// SetAssetLoader overrides the asset loader with the passed-in one.
func SetAssetLoader(al AssetLoader) {
	assetLoaderImpl = al
}

// ResetAssetLoader restores the asset loader to the default value.
func ResetAssetLoader() {
	assetLoaderImpl = defaultAssetLoader
}

// PemUsage indicates the purpose of a given certificate.
type PemUsage uint32

const (
	_ PemUsage = iota
	// CAPem describes the main CA certificate.
	CAPem
	// TenantClientCAPem describes the CA certificate used to broker authN/Z for SQL
	// tenants wishing to access the KV layer.
	TenantClientCAPem
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
	// TenantClientPem describes a SQL tenant client certificate.
	TenantClientPem

	// Maximum allowable permissions.
	maxKeyPermissions os.FileMode = 0700
	// Filename extenstions.
	certExtension = `.crt`
	keyExtension  = `.key`
	// Certificate directory permissions.
	defaultCertsDirPerm = 0700
)

func isCA(usage PemUsage) bool {
	return usage == CAPem || usage == ClientCAPem || usage == TenantClientCAPem || usage == UICAPem
}

func (p PemUsage) String() string {
	switch p {
	case CAPem:
		return "CA"
	case ClientCAPem:
		return "Client CA"
	case TenantClientCAPem:
		return "Tenant Client CA"
	case UICAPem:
		return "UI CA"
	case NodePem:
		return "Node"
	case UIPem:
		return "UI"
	case ClientPem:
		return "Client"
	case TenantClientPem:
		return "Tenant Client"
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

func exceedsPermissions(objectMode, allowedMode os.FileMode) bool {
	mask := os.FileMode(0777) ^ allowedMode
	return mask&objectMode != 0
}

func isCertificateFile(filename string) bool {
	return strings.HasSuffix(filename, certExtension)
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
			return nil, errors.Errorf("CA certificate filename should match ca%s", certExtension)
		}
	case `ca-client`:
		fileUsage = ClientCAPem
		if numParts != 2 {
			return nil, errors.Errorf("client CA certificate filename should match ca-client%s", certExtension)
		}
	case `ca-client-tenant`:
		fileUsage = TenantClientCAPem
		if numParts != 2 {
			return nil, errors.Errorf("tenant CA certificate filename should match ca%s", certExtension)
		}
	case `ca-ui`:
		fileUsage = UICAPem
		if numParts != 2 {
			return nil, errors.Errorf("UI CA certificate filename should match ca-ui%s", certExtension)
		}
	case `node`:
		fileUsage = NodePem
		if numParts != 2 {
			return nil, errors.Errorf("node certificate filename should match node%s", certExtension)
		}
	case `ui`:
		fileUsage = UIPem
		if numParts != 2 {
			return nil, errors.Errorf("UI certificate filename should match ui%s", certExtension)
		}
	case `client`:
		fileUsage = ClientPem
		// Strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("client certificate filename should match client.<user>%s", certExtension)
		}
	case `client-tenant`:
		fileUsage = TenantClientPem
		// Strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("tenant certificate filename should match client-tenant.<tenantid>%s", certExtension)
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
	fileInfos, err := assetLoaderImpl.ReadDir(cl.certsDir)
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

		if !isCertificateFile(filename) {
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
		certPEMBlock, err := assetLoaderImpl.ReadFile(fullCertPath)
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

	keyFilename := strings.TrimSuffix(ci.Filename, certExtension) + keyExtension
	fullKeyPath := filepath.Join(cl.certsDir, keyFilename)

	// Stat the file. This follows symlinks.
	info, err := assetLoaderImpl.Stat(fullKeyPath)
	if err != nil {
		return errors.Errorf("could not stat key file %s: %v", fullKeyPath, err)
	}

	// Only regular files are supported (after following symlinks).
	fileMode := info.Mode()
	if !fileMode.IsRegular() {
		return errors.Errorf("key file %s is not a regular file", fullKeyPath)
	}

	if !cl.skipPermissionChecks {
		// Check permissions bits.
		filePerm := fileMode.Perm()
		if exceedsPermissions(filePerm, maxKeyPermissions) {
			return errors.Errorf("key file %s has permissions %s, exceeds %s",
				fullKeyPath, filePerm, maxKeyPermissions)
		}
	}

	// Read key file.
	keyPEMBlock, err := assetLoaderImpl.ReadFile(fullKeyPath)
	if err != nil {
		return errors.Errorf("could not read key file %s: %v", fullKeyPath, err)
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
			// Only check details of the first certificate.
			if err := validateCockroachCertificate(ci, x509Cert); err != nil {
				return makeErrorf(err, "failed to validate certificate %d in file %s", i, ci.Filename)
			}

			// Expiration from the first certificate.
			expires = x509Cert.NotAfter
		}
		certs[i] = x509Cert
	}

	ci.ParsedCertificates = certs
	ci.ExpirationTime = expires
	return nil
}

// validateDualPurposeNodeCert takes a CertInfo and a parsed certificate and checks the
// values of certain fields.
// This should only be called on the NodePem CertInfo when there is no specific
// client certificate for the 'node' user.
// Fields required for a valid server certificate are already checked.
func validateDualPurposeNodeCert(ci *CertInfo) error {
	if ci == nil {
		return errors.Errorf("no node certificate found")
	}

	if ci.Error != nil {
		return ci.Error
	}

	// The first certificate is used in client auth.
	cert := ci.ParsedCertificates[0]
	principals := getCertificatePrincipals(cert)
	if !Contains(principals, NodeUser) {
		return errors.Errorf("client/server node certificate has principals %q, expected %q",
			principals, NodeUser)
	}

	return nil
}

// validateCockroachCertificate takes a CertInfo and a parsed certificate and checks the
// values of certain fields.
func validateCockroachCertificate(ci *CertInfo, cert *x509.Certificate) error {

	switch ci.FileUsage {
	case NodePem:
		// Common Name is checked only if there is no client certificate for 'node'.
		// This is done in validateDualPurposeNodeCert.
	case ClientPem:
		// Check that CommonName matches the username extracted from the filename.
		principals := getCertificatePrincipals(cert)
		if !Contains(principals, ci.Name) {
			return errors.Errorf("client certificate has principals %q, expected %q",
				principals, ci.Name)
		}
	}
	return nil
}
