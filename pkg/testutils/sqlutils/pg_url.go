// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

// PGUrl returns a postgres connection url which connects to this server with the given user, and a
// cleanup function which must be called after all connections created using the connection url have
// been closed.
//
// In order to connect securely using postgres, this method will create temporary on-disk copies of
// certain embedded security certificates. The certificates will be created in a new temporary
// directory. The returned cleanup function will delete this temporary directory.
// Note that two calls to this function for the same `user` will generate different
// copies of the certificates, so the cleanup function must always be called.
//
// Args:
//  prefix: A prefix to be prepended to the temp file names generated, for debugging.
func PGUrl(t testing.TB, servingAddr, prefix string, user *url.Userinfo) (url.URL, func()) {
	host, port, err := net.SplitHostPort(servingAddr)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(benesch): Audit usage of prefix and replace the following line with
	// `testutils.TempDir(t)` if prefix can always be `t.Name()`.
	tempDir, err := ioutil.TempDir("", fileutil.EscapeFilename(prefix))
	if err != nil {
		t.Fatal(err)
	}

	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	certPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.crt", user.Username()))
	keyPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.key", user.Username()))

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCAPath := securitytest.RestrictedCopy(t, caPath, tempDir, "ca")
	tempCertPath := securitytest.RestrictedCopy(t, certPath, tempDir, "cert")
	tempKeyPath := securitytest.RestrictedCopy(t, keyPath, tempDir, "key")
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslrootcert", tempCAPath)
	options.Add("sslcert", tempCertPath)
	options.Add("sslkey", tempKeyPath)

	return url.URL{
			Scheme:   "postgres",
			User:     user,
			Host:     net.JoinHostPort(host, port),
			RawQuery: options.Encode(),
		}, func() {
			if err := os.RemoveAll(tempDir); err != nil {
				// Not Fatal() because we might already be panicking.
				t.Error(err)
			}
		}
}
