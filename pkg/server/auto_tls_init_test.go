// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestInitializeFromConfig is a placeholder for actual testing functions.
func TestInitializeFromConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}

	certBundle := CertificateBundle{}
	cfg := base.Config{
		SSLCertsDir: tempDir,
	}

	err = certBundle.InitializeFromConfig(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %q", err)
	}

	// Verify certs written to disk match certs in bundles.
	bundleFromDisk, err := loadAllCertsFromDisk(cfg)
	if err != nil {
		t.Fatalf("failed loading certs from disk, got: %q", err)
	}

	// Compare each set of certs and keys to those loaded from disk.
	compareBundleCaCerts(t, bundleFromDisk, certBundle, true)
	compareBundleServiceCerts(t, bundleFromDisk, certBundle, true)

	// Remove temp directory now that we are done with it.
	err = os.RemoveAll(tempDir)
	if err != nil {
		t.Fatalf("failed to remove test temp dir: %s", err)
	}

}

func loadAllCertsFromDisk(cfg base.Config) (CertificateBundle, error) {
	cl := security.MakeCertsLocator(cfg.SSLCertsDir)
	bundleFromDisk, err := collectLocalCABundle(cfg)
	if err != nil {
		return bundleFromDisk, err
	}
	bundleFromDisk.InterNode.loadOrCreateServiceCertificates(
		cl.NodeCertPath(), cl.NodeKeyPath(), "", "", 0, 0, "", []string{},
	)
	// TODO(aaron-crl): Figure out how to handle client auth case.
	//bundleFromDisk.UserAuth.loadOrCreateServiceCertificates(
	//	cl.ClientCertPath(), cl.ClientKeyPath(), "", "", 0, "", []string{},
	//)
	bundleFromDisk.SQLService.loadOrCreateServiceCertificates(
		cl.SQLServiceCertPath(), cl.SQLServiceKeyPath(), "", "", 0, 0, "", []string{},
	)
	bundleFromDisk.RPCService.loadOrCreateServiceCertificates(
		cl.RPCServiceCertPath(), cl.RPCServiceKeyPath(), "", "", 0, 0, "", []string{},
	)
	bundleFromDisk.AdminUIService.loadOrCreateServiceCertificates(
		cl.UICertPath(), cl.UIKeyPath(), "", "", 0, 0, "", []string{},
	)

	return bundleFromDisk, nil
}

func certCompareHelper(t *testing.T, desireEqual bool) func([]byte, []byte, string) {
	if desireEqual {
		return func(b1 []byte, b2 []byte, certName string) {
			if bytes.Compare(b1, b2) != 0 {
				t.Fatalf("bytes for %s not equal", certName)
			}
		}
	}
	return func(b1 []byte, b2 []byte, certName string) {
		if bytes.Compare(b1, b2) == 0 && b1 != nil {
			t.Fatalf("bytes for %s were equal", certName)
		}
	}
}

func compareBundleCaCerts(
	t *testing.T, cb1 CertificateBundle, cb2 CertificateBundle, desireEqual bool,
) {
	cmp := certCompareHelper(t, desireEqual)
	// Compare InterNode CA cert and key.
	cmp(
		cb1.InterNode.CACertificate,
		cb2.InterNode.CACertificate, "InterNodeCA cert")
	cmp(
		cb1.InterNode.CAKey,
		cb2.InterNode.CAKey, "InterNodeCA key")

	// Compare UserAuth CA cert and key.
	cmp(
		cb1.UserAuth.CACertificate,
		cb2.UserAuth.CACertificate, "UserAuth CA cert")
	cmp(
		cb1.UserAuth.CAKey,
		cb2.UserAuth.CAKey, "UserAuth CA key")

	// Compare SQL CA cert and key.
	cmp(
		cb1.SQLService.CACertificate,
		cb2.SQLService.CACertificate, "SQLService CA cert")
	cmp(
		cb1.SQLService.CAKey,
		cb2.SQLService.CAKey, "SQLService CA key")

	// Compare RPC CA cert and key.
	cmp(
		cb1.RPCService.CACertificate,
		cb2.RPCService.CACertificate, "RPCService CA cert")
	cmp(
		cb1.RPCService.CAKey,
		cb2.RPCService.CAKey, "RPCService CA key")

	// Compare UI CA cert and key.
	cmp(
		cb1.AdminUIService.CACertificate,
		cb2.AdminUIService.CACertificate, "AdminUIService CA cert")
	cmp(
		cb1.AdminUIService.CAKey,
		cb2.AdminUIService.CAKey, "AdminUIService CA key")

}

func compareBundleServiceCerts(
	t *testing.T, cb1 CertificateBundle, cb2 CertificateBundle, desireEqual bool,
) {
	cmp := certCompareHelper(t, desireEqual)

	cmp(
		cb1.InterNode.HostCertificate,
		cb2.InterNode.HostCertificate, "InterNode Host cert")
	cmp(
		cb1.InterNode.HostKey,
		cb2.InterNode.HostKey, "InterNode Host key")

	cmp(
		cb1.SQLService.HostCertificate,
		cb2.SQLService.HostCertificate, "SQLService Host cert")
	cmp(
		cb1.SQLService.HostKey,
		cb2.SQLService.HostKey, "SQLService Host key")

	cmp(
		cb1.RPCService.HostCertificate,
		cb2.RPCService.HostCertificate, "RPCService Host cert")
	cmp(
		cb1.RPCService.HostKey,
		cb2.RPCService.HostKey, "RPCService Host key")

	cmp(
		cb1.AdminUIService.HostCertificate,
		cb2.AdminUIService.HostCertificate, "AdminUIService Host cert")
	cmp(
		cb1.AdminUIService.HostKey,
		cb2.AdminUIService.HostKey, "AdminUIService Host key")
}

// TestDummyInitializeNodeFromBundle is a placeholder for actual testing functions.
// TODO(aaron-crl): [tests] write unit tests.
func TestDummyInitializeNodeFromBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	certBundle := CertificateBundle{}
	cfg := base.Config{
		SSLCertsDir: tempDir,
	}

	err = certBundle.InitializeNodeFromBundle(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}
}

// TestDummyCertLoader is a placeholder for actual testing functions.
// TODO(aaron-crl): [tests] write unit tests.
func TestDummyCertLoader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	scb := ServiceCertificateBundle{}
	_ = scb.loadServiceCertAndKey("", "")
	_ = scb.loadCACertAndKey("", "")
	_ = scb.rotateServiceCert("", "", time.Minute, "", []string{""})
}

// TestNodeCertRotation tests that the rotation function will overwrite the
// expected certificates and fail if they are not there.
// TODO(aaron-crl): correct this
func TestRotationOnUnintializedNode(t *testing.T) {
	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := base.Config{
		SSLCertsDir: tempDir,
	}

	// Check the empty case.
	// Check to see that the only file in dir is the EOF.
	dir, err := os.Open(cfg.SSLCertsDir)
	if err != nil {
		t.Fatalf("failed to open cfg.SSLCertsDir: %q", cfg.SSLCertsDir)
	}
	_, err = dir.Readdir(1)
	if err != io.EOF {
		// Directory is not empty to start with, this is an error.
		t.Fatal("files added to cfg.SSLCertsDir when they shouldn't have been")
	}
	dir.Close()

	err = rotateGeneratedCerts(cfg)
	if err != nil {
		t.Fatalf("expected nil error generating no certs, got: %q", err)
	}

}

func TestRotationOnIntializedNode(t *testing.T) {
	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := base.Config{
		SSLCertsDir: tempDir,
	}

	// Test in the fully provisioned case.
	certBundle := CertificateBundle{}
	err = certBundle.InitializeFromConfig(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %q", err)
	}

	err = rotateGeneratedCerts(cfg)
	if err != nil {
		t.Fatalf("rotation failed; expected err=nil, got: %q", err)
	}

	// Verify that any existing certs have changed on disk for services
	diskBundle, err := loadAllCertsFromDisk(cfg)
	if err != nil {
		t.Fatalf("failed loading certs from disk, got: %q", err)
	}
	compareBundleServiceCerts(t, certBundle, diskBundle, false)
}

func TestRotationOnPartialIntializedNode(t *testing.T) {
	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := base.Config{
		SSLCertsDir: tempDir,
	}
	// Test in the partially provisioned case (remove the Client and UI CAs).
	certBundle := CertificateBundle{}
	err = certBundle.InitializeFromConfig(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %q", err)
	}

	cl := security.MakeCertsLocator(cfg.SSLCertsDir)
	if err = os.Remove(cl.ClientCACertPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}
	if err = os.Remove(cl.ClientCAKeyPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}
	if err = os.Remove(cl.UICACertPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}
	if err = os.Remove(cl.UICAKeyPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}

	// This should rotate all service certs except client and UI.
	err = rotateGeneratedCerts(cfg)
	if err != nil {
		t.Fatalf("rotation failed; expected err=nil, got: %q", err)
	}

	// Verify that client and UI service host certs are unchanged.
	diskBundle, err := loadAllCertsFromDisk(cfg)
	if err != nil {
		t.Fatalf("failed loading certs from disk, got: %q", err)
	}
	cmp := certCompareHelper(t, true)
	cmp(
		certBundle.UserAuth.HostCertificate,
		diskBundle.UserAuth.HostCertificate, "UserAuth host cert")
	cmp(
		certBundle.UserAuth.HostKey,
		diskBundle.UserAuth.HostKey, "UserAuth host key")
	cmp(
		certBundle.AdminUIService.HostCertificate,
		diskBundle.AdminUIService.HostCertificate, "AdminUIService host cert")
	cmp(
		certBundle.AdminUIService.HostKey,
		diskBundle.AdminUIService.HostKey, "AdminUIService host key")
}

// TestRotationOnBrokenIntializedNode in the partially provisioned case (remove the Client and UI CAs).
func TestRotationOnBrokenIntializedNode(t *testing.T) {
	// Create a temp dir for all certificate tests.
	tempDir, err := ioutil.TempDir("", "auto_tls_init_test")
	if err != nil {
		t.Fatalf("failed to create test temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := base.Config{
		SSLCertsDir: tempDir,
	}
	cl := security.MakeCertsLocator(cfg.SSLCertsDir)
	certBundle := CertificateBundle{}
	err = certBundle.InitializeFromConfig(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %q", err)
	}
	// Test in the case where a leaf certificate has been removed but a CA is
	// still present with key. This should fail. Removing SQL certificate.
	if err = os.Remove(cl.SQLServiceCertPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}
	if err = os.Remove(cl.SQLServiceKeyPath()); err != nil {
		t.Fatalf("failed to remove test cert: %q", err)
	}

	err = rotateGeneratedCerts(cfg)
	if err == nil {
		t.Fatalf("rotation succeeded but should have failed with missing leaf certs for SQLService")
	}
}
