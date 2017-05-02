// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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

// SetAssetLoader overrides the asset loader with the passed-in one.
func SetAssetLoader(al AssetLoader) {
	assetLoaderImpl = al
}

// ResetAssetLoader restores the asset loader to the default value.
func ResetAssetLoader() {
	assetLoaderImpl = defaultAssetLoader
}

type pemUsage uint32

const (
	_ pemUsage = iota
	// CAPem describes a CA certificate.
	CAPem
	// NodePem describes a combined server/client certificate for user Node.
	NodePem
	// ClientPem describes a client certificate.
	ClientPem

	// Maximum allowable permissions.
	maxKeyPermissions os.FileMode = 0700
	// Filename extenstions.
	certExtension = `.crt`
	keyExtension  = `.key`
	// Certificate directory permissions.
	defaultCertsDirPerm = 0700
)

func (p pemUsage) String() string {
	switch p {
	case CAPem:
		return "Certificate Authority"
	case NodePem:
		return "Node"
	case ClientPem:
		return "Client"
	default:
		return "unknown"
	}
}

// CertInfo describe a certificate file and optional key file.
// To obtain the full path, Filename and KeyFilename must be joined
// with the certs directory.
// The key may not be present if this is a CA certificate.
type CertInfo struct {
	// FileUsage describes the use of this certificate.
	FileUsage pemUsage

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
}

func exceedsPermissions(objectMode, allowedMode os.FileMode) bool {
	mask := os.FileMode(0777) ^ allowedMode
	return mask&objectMode != 0
}

func isCertificateFile(filename string) bool {
	return strings.HasSuffix(filename, certExtension)
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

	if !os.IsNotExist(err) {
		return errors.Wrapf(err, "could not stat certs directory %s", cl.certsDir)
	}

	if err := os.Mkdir(cl.certsDir, defaultCertsDirPerm); err != nil {
		return errors.Wrapf(err, "could not create certs directory %s", cl.certsDir)
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
		if os.IsNotExist(err) {
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
		ci, err := cl.certInfoFromFilename(filename)
		if err != nil {
			log.Warningf(context.Background(), "bad filename %s: %v", fullPath, err)
			continue
		}

		// Look for the associated key.
		if err := cl.findKey(ci); err != nil {
			log.Warningf(context.Background(), "error finding key for %s: %v", fullPath, err)
			continue
		}

		if log.V(3) {
			log.Infof(context.Background(), "found certificate %s", ci.Filename)
		}
		cl.certificates = append(cl.certificates, ci)
	}

	return nil
}

// certInfoFromFilename takes a filename and attempts to determine the
// certificate usage (ca, node, etc..).
func (cl *CertificateLoader) certInfoFromFilename(filename string) (*CertInfo, error) {
	parts := strings.Split(filename, `.`)
	numParts := len(parts)

	if numParts < 2 {
		return nil, errors.New("not enough parts found")
	}

	var pu pemUsage
	var name string
	prefix := parts[0]
	switch parts[0] {
	case `ca`:
		pu = CAPem
		if numParts != 2 {
			return nil, errors.Errorf("CA certificate filename should match ca%s", certExtension)
		}
	case `node`:
		pu = NodePem
		if numParts != 2 {
			return nil, errors.Errorf("node certificate filename should match node%s", certExtension)
		}
	case `client`:
		pu = ClientPem
		// strip prefix and suffix and re-join middle parts.
		name = strings.Join(parts[1:numParts-1], `.`)
		if len(name) == 0 {
			return nil, errors.Errorf("client certificate filename should match client.<user>%s", certExtension)
		}
	default:
		return nil, errors.Errorf("unknown prefix %q", prefix)
	}

	// Read cert file contents.
	fullCertPath := filepath.Join(cl.certsDir, filename)
	certPEMBlock, err := assetLoaderImpl.ReadFile(fullCertPath)
	if err != nil {
		return nil, errors.Errorf("could not read certificate file: %v", err)
	}

	return &CertInfo{
		FileUsage:    pu,
		Filename:     filename,
		FileContents: certPEMBlock,
		Name:         name,
	}, nil
}

// findKey takes a CertInfo and looks for the corresponding key file.
// If found, sets the 'keyFilename' and returns nil, returns error otherwise.
// Does not load CA keys.
func (cl *CertificateLoader) findKey(ci *CertInfo) error {
	if ci.FileUsage == CAPem {
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
