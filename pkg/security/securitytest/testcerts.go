// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package securitytest

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
)

// CreateTestCerts populates the test certificates in the given directory.
func CreateTestCerts(certsDir string) (cleanup func() error) {
	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	// Disable embedded certs, or the security library will try to load
	// our real files as embedded assets.
	securityassets.ResetLoader()

	assets := []string{
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedCACert),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedCAKey),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeCert),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeKey),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedRootCert),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedRootKey),
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedTenantCACert),
	}

	for _, a := range assets {
		_, err := RestrictedCopy(a, certsDir, filepath.Base(a))
		if err != nil {
			panic(err)
		}
	}

	return func() error {
		securityassets.SetLoader(EmbeddedAssets)
		return os.RemoveAll(certsDir)
	}
}
