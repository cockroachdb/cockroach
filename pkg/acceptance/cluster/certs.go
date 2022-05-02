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
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/security"
)

var absCertsDir string

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 2048

// AbsCertsDir returns the absolute path to the certificate directory.
func AbsCertsDir() string {
	return absCertsDir
}

// GenerateCerts generates CA and client certificates and private keys to be
// used with a cluster. It returns the absolute path to the certs directory and
// a function that will clean up the generated files.
func GenerateCerts(ctx context.Context) func() {
	const certsDir = ".localcluster.certs"

	// docker-compose tests change their working directory,
	// so they need to know the absolute path to the certificate directory.
	if bazel.BuiltWithBazel() {
		var err error
		absCertsDir, err = ioutil.TempDir("", "certs")
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		absCertsDir, err = filepath.Abs(certsDir)
		if err != nil {
			panic(err)
		}
		maybePanic(os.RemoveAll(absCertsDir))
	}

	maybePanic(security.CreateCAPair(
		absCertsDir, filepath.Join(absCertsDir, security.EmbeddedCAKey),
		keyLen, 96*time.Hour, false, false))

	// Root user.
	maybePanic(security.CreateClientPair(
		absCertsDir, filepath.Join(absCertsDir, security.EmbeddedCAKey),
		2048, 48*time.Hour, false, security.RootUserName(), true /* generate pk8 key */))

	// Test user.
	maybePanic(security.CreateClientPair(
		absCertsDir, filepath.Join(absCertsDir, security.EmbeddedCAKey),
		1024, 48*time.Hour, false, security.TestUserName(), true /* generate pk8 key */))

	// Certs for starting a cockroach server. Key size is from cli/cert.go:defaultKeySize.
	maybePanic(security.CreateNodePair(
		absCertsDir, filepath.Join(absCertsDir, security.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, []string{"localhost", "cockroach"}))

	// Store a copy of the client certificate and private key in a PKCS#12
	// bundle, which is the only format understood by Npgsql (.NET).
	{
		execCmd("openssl", "pkcs12", "-export", "-password", "pass:",
			"-in", filepath.Join(absCertsDir, "client.root.crt"),
			"-inkey", filepath.Join(absCertsDir, "client.root.key"),
			"-out", filepath.Join(absCertsDir, "client.root.pk12"))
	}

	return func() { _ = os.RemoveAll(absCertsDir) }
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
