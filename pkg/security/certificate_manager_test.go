// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	// We expect 6 client certificates for root, testuser, testuser2, testuser3,
	// testuser_cn_only, testuser_san_only, testuser_cn_and_san.
	if a, e := len(clientCerts), 7; a != e {
		t.Errorf("expected %d client certs, found %d", e, a)
	}

	if _, ok := clientCerts[username.RootUserName()]; !ok {
		t.Error("no client cert for root user found")
	}

	// Verify that we can build tls.Config objects.
	if _, err := cm.GetServerTLSConfig(); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(username.NodeUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(username.RootUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(username.TestUserName()); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(
		username.MakeSQLUsernameFromPreNormalizedString("testuser2")); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(
		username.MakeSQLUsernameFromPreNormalizedString("my-random-user")); err == nil {
		t.Error("unexpected success")
	}
}

func TestManagerWithPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Do not mock cert access for this test.
	securityassets.ResetLoader()
	defer ResetTest()

	defer func() { _ = security.SetCertPrincipalMap(nil) }()
	defer func() {
		_ = os.Unsetenv("COCKROACH_CERT_NODE_USER")
		envutil.ClearEnvCache()
	}()
	require.NoError(t, os.Setenv("COCKROACH_CERT_NODE_USER", "node.crdb.io"))

	certsDir := t.TempDir()

	caKey := filepath.Join(certsDir, "ca.key")
	require.NoError(t, security.CreateCAPair(
		certsDir, caKey, testKeySize, time.Hour*96, true, true,
	))
	require.NoError(t, security.CreateClientPair(
		certsDir, caKey, testKeySize, time.Hour*48, true, username.TestUserName(),
		[]roachpb.TenantID{roachpb.SystemTenantID}, nil /* tenantNameScope */, false,
	))
	require.NoError(t, security.CreateClientPair(
		certsDir, caKey, testKeySize, time.Hour*48, true, username.TestUserName(),
		nil /* tenantScope */, []roachpb.TenantName{roachpb.TenantName(roachpb.SystemTenantID.String())},
		false,
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
	loadUserCert := func(user username.SQLUsername) error {
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

	// at this point certificate need not have principals match node user
	setCertPrincipalMap("")
	require.NoError(t, newCertificateManager())

	// Renaming "client.testuser.crt" to "client.foo.crt" allows us to load it
	// under that name.
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.crt"),
		filepath.Join(certsDir, "client.foo.crt")))
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.key"),
		filepath.Join(certsDir, "client.foo.key")))
	setCertPrincipalMap("testuser:foo,node.crdb.io:node")
	require.NoError(t, loadUserCert(username.MakeSQLUsernameFromPreNormalizedString("foo")))
}
