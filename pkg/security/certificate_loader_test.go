// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func TestCertNomenclature(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We're just testing nomenclature parsing, all files exist and contain a valid PEM block.

	testCases := []struct {
		filename      string
		expectedError string
		usage         security.PemUsage
		name          string
	}{
		// Test valid names.
		{"ca.crt", "", security.CAPem, ""},
		{"ca-client.crt", "", security.ClientCAPem, ""},
		{"ca-ui.crt", "", security.UICAPem, ""},
		{"node.crt", "", security.NodePem, ""},
		{"ui.crt", "", security.UIPem, ""},
		{"client.root.crt", "", security.ClientPem, "root"},
		{"client.foo-bar.crt", "", security.ClientPem, "foo-bar"},
		{"client....foo.bar.baz.how.many.dots.do.you.need...really....crt", "", security.ClientPem, "...foo.bar.baz.how.many.dots.do.you.need...really..."},

		// Bad names. This function is only called on filenames ending with '.crt'.
		{"crt", "not enough parts found", 0, ""},
		{".crt", "unknown prefix", 0, ""},
		{"ca2.crt", "unknown prefix \"ca2\"", 0, ""},
		{"ca-client.foo.crt", "client CA certificate filename should match ca-client.crt", 0, ""},
		{"ca-ui.foo.crt", "UI CA certificate filename should match ca-ui.crt", 0, ""},
		{"ca.client.crt", "CA certificate filename should match ca.crt", 0, ""},
		{"ui2.crt", "unknown prefix \"ui2\"", 0, ""},
		{"ui.blah.crt", "UI certificate filename should match ui.crt", 0, ""},
		{"node2.crt", "unknown prefix \"node2\"", 0, ""},
		{"node.foo.crt", "node certificate filename should match node.crt", 0, ""},
		{"client2.crt", "unknown prefix \"client2\"", 0, ""},
		{"client.crt", "client certificate filename should match client.<user>.crt", 0, ""},
		{"root.crt", "unknown prefix \"root\"", 0, ""},
	}

	for i, tc := range testCases {
		ci, err := security.CertInfoFromFilename(tc.filename)
		if !testutils.IsError(err, tc.expectedError) {
			t.Errorf("#%d: expected error %v, got %v", i, tc.expectedError, err)
			continue
		}
		if err != nil {
			continue
		}
		if ci.FileUsage != tc.usage {
			t.Errorf("#%d: expected file usage %v, got %v", i, tc.usage, ci.FileUsage)
		}
		if ci.Name != tc.name {
			t.Errorf("#%d: expected name %v, got %v", i, tc.name, ci.Name)
		}
	}
}

func TestLoadEmbeddedCerts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cl := security.NewCertificateLoader(security.EmbeddedCertsDir)
	if err := cl.Load(); err != nil {
		t.Error(err)
	}

	assets, err := securitytest.AssetReadDir(security.EmbeddedCertsDir)
	if err != nil {
		t.Fatal(err)
	}

	// Check that we have "found pairs * 2 = num assets".
	certs := cl.Certificates()
	if act, exp := len(certs), len(assets); act*2 != exp {
		t.Errorf("found %d keypairs, but have %d embedded files", act, exp)
	}

	// Check that all non-CA pairs include a key.
	for _, c := range certs {
		if c.FileUsage == security.CAPem || c.FileUsage == security.TenantClientCAPem {
			if len(c.KeyFilename) != 0 {
				t.Errorf("CA key was loaded for CertInfo %+v", c)
			}
		} else if len(c.KeyFilename) == 0 {
			t.Errorf("no key found as part of CertInfo %+v", c)
		}
	}
}

func countLoadedCertificates(certsDir string) (int, error) {
	cl := security.NewCertificateLoader(certsDir)
	if err := cl.Load(); err != nil {
		return 0, err
	}
	return len(cl.Certificates()), nil
}

// Generate a x509 cert with specific fields.
func makeTestCert(
	t *testing.T, commonName string, keyUsage x509.KeyUsage, extUsages []x509.ExtKeyUsage,
) (*x509.Certificate, []byte) {
	// Make smallest rsa key possible: not saved.
	key, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		t.Fatalf("error on GenerateKey for CN=%s: %v", commonName, err)
	}

	// Specify the smallest possible set of fields.
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore: timeutil.Now().Add(-time.Hour),
		NotAfter:  timeutil.Now().Add(time.Hour),
		KeyUsage:  keyUsage,
	}

	template.ExtKeyUsage = extUsages

	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatalf("error on CreateCertificate for CN=%s: %v", commonName, err)
	}

	// parse it back.
	parsedCert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatalf("error on ParseCertificate for CN=%s: %v", commonName, err)
	}

	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}
	return parsedCert, pem.EncodeToMemory(certBlock)
}

func TestNamingScheme(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fullKeyUsage := x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	// Build a few certificates. These are barebones since we only need to check our custom validation,
	// not chain verification.
	parsedCACert, caCert := makeTestCert(t, "", 0, nil)

	parsedGoodNodeCert, goodNodeCert := makeTestCert(t, "node", fullKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
	_, badUserNodeCert := makeTestCert(t, "notnode", fullKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})

	parsedGoodRootCert, goodRootCert := makeTestCert(t, "root", fullKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	_, notRootCert := makeTestCert(t, "notroot", fullKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	// Do not use embedded certs.
	security.ResetAssetLoader()
	defer ResetTest()

	// Some test cases are skipped on windows due to non-UGO permissions.
	isWindows := runtime.GOOS == "windows"

	// Test non-existent directory.
	// If the directory exists, we still expect no failures, unless it happens to contain
	// valid filenames, so we don't need to try too hard to generate a unique name.
	if count, err := countLoadedCertificates("my_non_existent_directory-only_for_tests"); err != nil {
		t.Error(err)
	} else if exp := 0; exp != count {
		t.Errorf("found %d certificates, expected %d", count, exp)
	}

	// Create directory.
	certsDir, err := ioutil.TempDir("", "certs_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(certsDir); err != nil {
			t.Fatal(err)
		}
	}()

	type testFile struct {
		name     string
		mode     os.FileMode
		contents []byte
	}

	testData := []struct {
		// Files to write (name and mode).
		files []testFile
		// Certinfos found. Ordered by cert base filename.
		certs []security.CertInfo
		// Set to true to skip permissions checks.
		skipChecks bool
		// Skip test case on windows as permissions are always ignored.
		skipWindows bool
	}{
		{
			// Empty directory.
			files: []testFile{},
			certs: []security.CertInfo{},
		},
		{
			// Test bad names, including ca/node certs with blobs in the middle, wrong separator.
			// We only need to test certs, if they're not loaded, neither will keys.
			files: []testFile{
				{"ca.foo.crt", 0777, []byte{}},
				{"cr..crt", 0777, []byte{}},
				{"node.foo.crt", 0777, []byte{}},
				{"node..crt", 0777, []byte{}},
				{"client.crt", 0777, []byte{}},
				{"client..crt", 0777, []byte{}},
			},
			certs: []security.CertInfo{},
		},
		{
			// Test proper names, but no key files, only the CA cert should be loaded without error.
			files: []testFile{
				{"ca.crt", 0777, caCert},
				{"node.crt", 0777, goodNodeCert},
				{"client.root.crt", 0777, goodRootCert},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: caCert},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", Name: "root",
					Error: errors.New(".* no such file or directory")},
				{FileUsage: security.NodePem, Filename: "node.crt",
					Error: errors.New(".* no such file or directory")},
			},
		},
		{
			// Key files, but wrong permissions.
			// We don't load CA keys here, so permissions for them don't matter.
			files: []testFile{
				{"ca.crt", 0777, caCert},
				{"ca.key", 0777, []byte{}},
				{"node.crt", 0777, goodNodeCert},
				{"node.key", 0704, []byte{}},
				{"client.root.crt", 0777, goodRootCert},
				{"client.root.key", 0740, []byte{}},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: caCert},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", Name: "root",
					Error: errors.New(".* exceeds -rwx------")},
				{FileUsage: security.NodePem, Filename: "node.crt",
					Error: errors.New(".* exceeds -rwx------")},
			},
			skipWindows: true,
		},
		{
			// Bad CommonName: this is checked later in the CertificateManager.
			files: []testFile{
				{"node.crt", 0777, badUserNodeCert},
				{"node.key", 0700, []byte("node.key")},
				{"client.root.crt", 0777, notRootCert},
				{"client.root.key", 0700, []byte{}},
			},
			certs: []security.CertInfo{
				{FileUsage: security.ClientPem, Filename: "client.root.crt", Name: "root",
					Error: errors.New(`client certificate has principals \["notroot"\], expected "root"`)},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: badUserNodeCert, KeyFileContents: []byte("node.key")},
			},
		},
		{
			// Everything loads.
			files: []testFile{
				{"ca.crt", 0777, caCert},
				{"ca.key", 0700, []byte("ca.key")},
				{"node.crt", 0777, goodNodeCert},
				{"node.key", 0700, []byte("node.key")},
				{"client.root.crt", 0777, goodRootCert},
				{"client.root.key", 0700, []byte("client.root.key")},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: caCert},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", KeyFilename: "client.root.key",
					Name: "root", FileContents: goodRootCert, KeyFileContents: []byte("client.root.key")},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: goodNodeCert, KeyFileContents: []byte("node.key")},
			},
		},
		{
			// Certificates contain the CA: everything loads.
			files: []testFile{
				{"ca.crt", 0777, caCert},
				{"ca.key", 0700, []byte("ca.key")},
				{"node.crt", 0777, append(goodNodeCert, caCert...)},
				{"node.key", 0700, []byte("node.key")},
				{"client.root.crt", 0777, append(goodRootCert, caCert...)},
				{"client.root.key", 0700, []byte("client.root.key")},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: caCert},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", KeyFilename: "client.root.key",
					Name: "root", FileContents: append(goodRootCert, caCert...), KeyFileContents: []byte("client.root.key"),
					ParsedCertificates: []*x509.Certificate{parsedGoodRootCert, parsedCACert}},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: append(goodNodeCert, caCert...), KeyFileContents: []byte("node.key"),
					ParsedCertificates: []*x509.Certificate{parsedGoodNodeCert, parsedCACert}},
			},
		},
		{
			// Bad key permissions, but skip permissions checks.
			files: []testFile{
				{"ca.crt", 0777, caCert},
				{"ca.key", 0777, []byte("ca.key")},
				{"node.crt", 0777, goodNodeCert},
				{"node.key", 0777, []byte("node.key")},
				{"client.root.crt", 0777, goodRootCert},
				{"client.root.key", 0777, []byte("client.root.key")},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: caCert},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", KeyFilename: "client.root.key",
					Name: "root", FileContents: goodRootCert, KeyFileContents: []byte("client.root.key")},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: goodNodeCert, KeyFileContents: []byte("node.key")},
			},
			skipChecks: true,
		},
	}

	for testNum, data := range testData {
		if data.skipWindows && isWindows {
			continue
		}

		// Write all files.
		for _, f := range data.files {
			n := f.name
			if err := ioutil.WriteFile(filepath.Join(certsDir, n), f.contents, f.mode); err != nil {
				t.Fatalf("#%d: could not write file %s: %v", testNum, n, err)
			}
		}

		// Load certs.
		cl := security.NewCertificateLoader(certsDir)
		if data.skipChecks {
			cl.TestDisablePermissionChecks()
		}
		if err := cl.Load(); err != nil {
			t.Errorf("#%d: unexpected error: %v", testNum, err)
		}

		// Check count of certificates.
		if expected, actual := len(data.certs), len(cl.Certificates()); expected != actual {
			t.Errorf("#%d: expected %d certificates, found %d", testNum, expected, actual)
		}

		// Check individual certificates.
		for i, actual := range cl.Certificates() {
			expected := data.certs[i]

			if expected.Error == nil {
				if actual.Error != nil {
					t.Errorf("#%d: expected success, got error: %+v", testNum, actual.Error)
					continue
				}
			} else {
				if !testutils.IsError(actual.Error, expected.Error.Error()) {
					t.Errorf("#%d: mismatched error, expected: %+v, got %+v", testNum, expected.Error, actual.Error)
				}
				continue
			}

			// Compare some fields.
			if actual.FileUsage != expected.FileUsage ||
				actual.Filename != expected.Filename ||
				actual.KeyFilename != expected.KeyFilename ||
				actual.Name != expected.Name {
				t.Errorf("#%d: mismatching CertInfo, expected: %+v, got %+v", testNum, expected, actual)
				continue
			}
			if actual.Filename != "" {
				if !bytes.Equal(actual.FileContents, expected.FileContents) {
					t.Errorf("#%d: bad file contents: expected %s, got %s", testNum, expected.FileContents, actual.FileContents)
					continue
				}
				if expected.ParsedCertificates != nil {
					// ParsedCertificates was specified in the expected test output, check against it.
					if a, e := len(actual.ParsedCertificates), len(expected.ParsedCertificates); a != e {
						t.Errorf("#%d: expected %d certificates, found: %d", testNum, e, a)
						continue
					}
					for certIndex := range actual.ParsedCertificates {
						if a, e := actual.ParsedCertificates[certIndex], expected.ParsedCertificates[certIndex]; !a.Equal(e) {
							t.Errorf("#%d: certificate %d does not match: got %v, expected %v", testNum, certIndex, a, e)
							continue
						}
					}
				} else {
					// No ParsedCertificates specified, we expect just 1.
					if a, e := len(actual.ParsedCertificates), 1; a != e {
						t.Errorf("#%d: expected %d certificates, found: %d", testNum, e, a)
						continue
					}
				}
			}
			if actual.KeyFilename != "" && !bytes.Equal(actual.KeyFileContents, expected.KeyFileContents) {
				t.Errorf("#%d: bad file contents: expected %s, got %s", testNum, expected.KeyFileContents, actual.KeyFileContents)
				continue
			}
		}

		// Wipe all files.
		for _, f := range data.files {
			n := f.name
			if err := os.Remove(filepath.Join(certsDir, n)); err != nil {
				t.Fatalf("#%d: could not delete file %s: %v", testNum, n, err)
			}
		}
	}
}
