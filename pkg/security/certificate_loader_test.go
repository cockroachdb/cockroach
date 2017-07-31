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

package security_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

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
		if c.FileUsage == security.CAPem {
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
		return 0, nil
	}
	return len(cl.Certificates()), nil
}

// Generate a valid x509 CA certificate. Returns the x509 certificate and the
// PEM-encoded contents (suitable to write to a .crt file).
func makeTestCACert() (*x509.Certificate, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := security.GenerateCA(key, time.Hour*48)
	if err != nil {
		return nil, nil, err
	}

	x509Cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, err
	}

	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}
	return x509Cert, pem.EncodeToMemory(certBlock), nil
}

func TestNamingScheme(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Make a valid x509 certificate. We don't use it here, but we do need something that
	// parses as a valid pem-encoded x509 certificate.
	x509Cert, certContents, err := makeTestCACert()
	if err != nil {
		t.Fatal(err)
	}

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
				{"ca.crt", 0777, certContents},
				{"node.crt", 0777, certContents},
				{"client.root.crt", 0777, certContents},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: certContents},
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
				{"ca.crt", 0777, certContents},
				{"ca.key", 0777, []byte{}},
				{"node.crt", 0777, certContents},
				{"node.key", 0704, []byte{}},
				{"client.root.crt", 0777, certContents},
				{"client.root.key", 0740, []byte{}},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: certContents},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", Name: "root",
					Error: errors.New(".* exceeds -rwx------")},
				{FileUsage: security.NodePem, Filename: "node.crt",
					Error: errors.New(".* exceeds -rwx------")},
			},
			skipWindows: true,
		},
		{
			// Bad cert files.
			files: []testFile{
				{"ca.crt", 0777, []byte{}},
				{"ca.key", 0777, []byte{}},
				{"node.crt", 0777, []byte("foo")},
				{"node.key", 0700, []byte{}},
				{"client.root.crt", 0777, append(certContents, []byte("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----")...)},
				{"client.root.key", 0700, []byte{}},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt",
					Error: errors.New("empty certificate file: ca.crt")},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", Name: "root",
					Error: errors.New("failed to parse certificate at position 1 in file client.root.crt")},
				{FileUsage: security.NodePem, Filename: "node.crt",
					Error: errors.New("no certificates found in node.crt")},
			},
		},
		{
			// Everything loads.
			files: []testFile{
				{"ca.crt", 0777, certContents},
				{"ca.key", 0700, []byte("ca.key")},
				{"node.crt", 0777, certContents},
				{"node.key", 0700, []byte("node.key")},
				{"client.root.crt", 0777, certContents},
				{"client.root.key", 0700, []byte("client.root.key")},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: certContents},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", KeyFilename: "client.root.key",
					Name: "root", FileContents: certContents, KeyFileContents: []byte("client.root.key")},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: certContents, KeyFileContents: []byte("node.key")},
			},
		},
		{
			// Bad key permissions, but skip permissions checks.
			files: []testFile{
				{"ca.crt", 0777, certContents},
				{"ca.key", 0777, []byte("ca.key")},
				{"node.crt", 0777, certContents},
				{"node.key", 0777, []byte("node.key")},
				{"client.root.crt", 0777, certContents},
				{"client.root.key", 0777, []byte("client.root.key")},
			},
			certs: []security.CertInfo{
				{FileUsage: security.CAPem, Filename: "ca.crt", FileContents: certContents},
				{FileUsage: security.ClientPem, Filename: "client.root.crt", KeyFilename: "client.root.key",
					Name: "root", FileContents: certContents, KeyFileContents: []byte("client.root.key")},
				{FileUsage: security.NodePem, Filename: "node.crt", KeyFilename: "node.key",
					FileContents: certContents, KeyFileContents: []byte("node.key")},
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
					t.Errorf("#%d: mismatched error, expected: %+v, got %+v", testNum, expected, actual)
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
				if a, e := len(actual.ParsedCertificates), 1; a != e {
					t.Errorf("#%d: expected %d certificates, found: %d", testNum, e, a)
					continue
				}
				if a, e := actual.ParsedCertificates[0], x509Cert; !bytes.Equal(a.Raw, e.Raw) {
					t.Errorf("#%d: mismatched certificate: %+v vs %+v", testNum, a, e)
					continue
				}
				if a, e := actual.ExpirationTime, x509Cert.NotAfter; a != e {
					t.Errorf("#%d: mismatched expiration: %s vs %s", testNum, a, e)
					continue
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
