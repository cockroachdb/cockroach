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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestManagerWithEmbedded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager("test_certs", security.CommandTLSSettings{})
	if err != nil {
		t.Error(err)
	}

	// Verify loaded certs.
	if cm.CACert() == nil {
		t.Error("expected non-nil CACert")
	}
	if cm.NodeCert() == nil {
		t.Error("expected non-nil NodeCert")
	}
	clientCerts := cm.ClientCerts()
	if a, e := len(clientCerts), 3; a != e {
		t.Errorf("expected %d client certs, found %d", e, a)
	}

	if _, ok := clientCerts[security.RootUserName()]; !ok {
		t.Error("no client cert for root user found")
	}

	// Verify that we can build tls.Config objects.
	if _, err := cm.GetServerTLSConfig(); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.NodeUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.RootUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.TestUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(
		security.MakeSQLUsernameFromPreNormalizedString("testuser2")); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(
		security.MakeSQLUsernameFromPreNormalizedString("my-random-user")); err == nil {
		t.Error("unexpected success")
	}
}

func TestManagerWithPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Do not mock cert access for this test.
	security.ResetAssetLoader()
	defer ResetTest()

	defer func() { _ = security.SetCertPrincipalMap(nil) }()
	defer func() {
		_ = os.Setenv("COCKROACH_CERT_NODE_USER", security.NodeUser)
		envutil.ClearEnvCache()
	}()
	require.NoError(t, os.Setenv("COCKROACH_CERT_NODE_USER", "node.crdb.io"))

	certsDir, err := ioutil.TempDir("", "certs_test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(certsDir))
	}()

	caKey := filepath.Join(certsDir, "ca.key")
	require.NoError(t, security.CreateCAPair(
		certsDir, caKey, testKeySize, time.Hour*96, true, true,
	))
	require.NoError(t, security.CreateClientPair(
		certsDir, caKey, testKeySize, time.Hour*48, true, security.TestUserName(), false,
	))
	require.NoError(t, security.CreateNodePair(
		certsDir, caKey, testKeySize, time.Hour*48, true, []string{"127.0.0.1", "foo"},
	))

	setCertPrincipalMap := func(s string) {
		require.NoError(t, security.SetCertPrincipalMap(strings.Split(s, ",")))
	}
	newCertificateManager := func() error {
		_, err := security.NewCertificateManager(certsDir, security.CommandTLSSettings{})
		return err
	}
	loadUserCert := func(user security.SQLUsername) error {
		cm, err := security.NewCertificateManager(certsDir, security.CommandTLSSettings{})
		if err != nil {
			return err
		}
		ci := cm.ClientCerts()[user]
		if ci == nil {
			return fmt.Errorf("user %q not found", user)
		}
		return ci.Error
	}

	setCertPrincipalMap("")
	require.Regexp(t, `node certificate has principals \["node.crdb.io" "foo"\]`, newCertificateManager())

	// We can map the "node.crdb.io" principal to "node".
	setCertPrincipalMap("node.crdb.io:node")
	require.NoError(t, newCertificateManager())

	// We can map the "foo" principal to "node".
	setCertPrincipalMap("foo:node")
	require.NoError(t, newCertificateManager())

	// Mapping the "testuser" principal to a different name should result in an
	// error as it no longer matches the file name.
	setCertPrincipalMap("testuser:foo,node.crdb.io:node")
	require.Regexp(t, `client certificate has principals \["foo"\], expected "testuser"`, loadUserCert(security.TestUserName()))

	// Renaming "client.testuser.crt" to "client.foo.crt" allows us to load it
	// under that name.
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.crt"),
		filepath.Join(certsDir, "client.foo.crt")))
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.key"),
		filepath.Join(certsDir, "client.foo.key")))
	setCertPrincipalMap("testuser:foo,node.crdb.io:node")
	require.NoError(t, loadUserCert(security.MakeSQLUsernameFromPreNormalizedString("foo")))
}
