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
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestDummyInitializeFromConfig is a placeholder for actual testing functions.
// TODO(aaron-crl): [tests] write unit tests.
func TestDummyInitializeFromConfig(t *testing.T) {
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
		t.Fatalf("expected err=nil, got: %s", err)
	}

	// Remove temp directory now that we are done with it.
	err = os.RemoveAll(tempDir)
	if err != nil {
		t.Fatalf("failed to remove test temp dir: %s", err)
	}

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

	certBundle := CertificateBundle{}
	cfg := base.Config{
		SSLCertsDir: tempDir,
	}

	err = certBundle.InitializeNodeFromBundle(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}

	// Remove temp directory now that we are done with it.
	err = os.RemoveAll(tempDir)
	if err != nil {
		t.Fatalf("failed to remove test temp dir: %s", err)
	}
}

// TestDummyCertLoader is a placeholder for actual testing functions.
// TODO(aaron-crl): [tests] write unit tests.
func TestDummyCertLoader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	scb := ServiceCertificateBundle{}
	_ = scb.loadServiceCertAndKey("", "")
	_ = scb.loadCACertAndKey("", "")

	cb := CertificateBundle{}
	cb.InterNode.copyOnlyCAs(&scb)
	_ = cb.ToPeerInitBundle()
}
