// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
)

const certsDir = ".localcluster.certs"

var absCertsDir string

// keyLen is the length (in bits) of the generated TLS certs.
//
// This needs to be at least 2048 since the newer versions of openssl
// (used by some tests) produce an error 'ee key too small' for
// smaller values.
const keyLen = 2048

// AbsCertsDir returns the absolute path to the certificate directory.
func AbsCertsDir() string {
	return absCertsDir
}

// GenerateCerts generates CA and client certificates and private keys to be
// used with a cluster. It returns a function that will clean up the generated
// files.
func GenerateCerts(ctx context.Context) func() {
	var err error
	// docker-compose tests change their working directory,
	// so they need to know the absolute path to the certificate directory.
	absCertsDir, err = filepath.Abs(certsDir)
	if err != nil {
		panic(err)
	}
	maybePanic(os.RemoveAll(certsDir))

	maybePanic(security.CreateCAPair(
		certsDir, filepath.Join(certsDir, certnames.EmbeddedCAKey),
		keyLen, 96*time.Hour, false, false))

	// Root user.
	// Scope root user to system tenant and tenant ID 5 which is what we use by default for acceptance
	// tests.
	userScopes := []roachpb.TenantID{roachpb.SystemTenantID, roachpb.MustMakeTenantID(5)}
	maybePanic(security.CreateClientPair(
		certsDir, filepath.Join(certsDir, certnames.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, username.RootUserName(), userScopes,
		nil /* tenantNames */, true /* generate pk8 key */))

	// Test user.
	// Scope test user to system tenant and tenant ID 5 which is what we use by default for acceptance
	// tests.
	maybePanic(security.CreateClientPair(
		certsDir, filepath.Join(certsDir, certnames.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, username.TestUserName(), userScopes,
		nil /* tenantNames */, true /* generate pk8 key */))

	// Certs for starting a cockroach server. Key size is from cli/cert.go:defaultKeySize.
	maybePanic(security.CreateNodePair(
		certsDir, filepath.Join(certsDir, certnames.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, []string{"localhost", "cockroach"}))

	// Store a copy of the client certificate and private key in a PKCS#12
	// bundle, which is the only format understood by Npgsql (.NET).
	{
		execCmd("openssl", "pkcs12", "-export", "-password", "pass:",
			"-in", filepath.Join(certsDir, "client.root.crt"),
			"-inkey", filepath.Join(certsDir, "client.root.key"),
			"-out", filepath.Join(certsDir, "client.root.pk12"))
	}

	return func() { _ = os.RemoveAll(certsDir) }
}

// GenerateCerts is only called in a file protected by a build tag. Suppress the
// unused linter's warning.
var _ = GenerateCerts

func execCmd(args ...string) {
	cmd := exec.Command(args[0], args[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("error: %s: %s\nout: %s\n", args, err, out))
	}
}
