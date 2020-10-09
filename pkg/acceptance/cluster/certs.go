// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
)

const certsDir = ".localcluster.certs"

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 2048

// GenerateCerts generates CA and client certificates and private keys to be
// used with a cluster. It returns a function that will clean up the generated
// files.
func GenerateCerts(ctx context.Context) func() {
	maybePanic(os.RemoveAll(certsDir))

	maybePanic(security.CreateCAPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		keyLen, 96*time.Hour, false, false))

	// Root user.
	maybePanic(security.CreateClientPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		1024, 48*time.Hour, false, security.RootUserName(), true /* generate pk8 key */))

	// Test user.
	maybePanic(security.CreateClientPair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
		1024, 48*time.Hour, false, security.TestUserName(), true /* generate pk8 key */))

	// Certs for starting a cockroach server. Key size is from cli/cert.go:defaultKeySize.
	maybePanic(security.CreateNodePair(
		certsDir, filepath.Join(certsDir, security.EmbeddedCAKey),
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
