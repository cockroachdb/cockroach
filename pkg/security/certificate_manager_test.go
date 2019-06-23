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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestManagerWithEmbedded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager("test_certs")
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
	if a, e := len(clientCerts), 2; a != e {
		t.Errorf("expected %d client certs, found %d", e, a)
	}

	if _, ok := clientCerts[security.RootUser]; !ok {
		t.Error("no client cert for root user found")
	}

	// Verify that we can build tls.Config objects.
	if _, err := cm.GetServerTLSConfig(); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.NodeUser); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.RootUser); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig("testuser"); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig("my-random-user"); err == nil {
		t.Error("unexpected success")
	}
}
