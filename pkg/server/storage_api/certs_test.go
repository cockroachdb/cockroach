// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCertificatesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	var response serverpb.CertificatesResponse
	if err := srvtestutils.GetStatusJSONProto(ts, "certificates/local", &response); err != nil {
		t.Fatal(err)
	}

	// We expect 6 certificates: CA, node, and client certs for root, testuser,
	// testuser2, testuser3, testuser_cn_only, testuser_san_only,
	// testuser_cn_and_san.
	if a, e := len(response.Certificates), 9; a != e {
		t.Errorf("expected %d certificates, found %d", e, a)
	}

	// The response is ordered: CA cert followed by node cert.
	cert := response.Certificates[0]
	if a, e := cert.Type, serverpb.CertificateDetails_CA; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	}

	cert = response.Certificates[1]
	if a, e := cert.Type, serverpb.CertificateDetails_NODE; a != e {
		t.Errorf("wrong type %s, expected %s", a, e)
	} else if cert.ErrorMessage != "" {
		t.Errorf("expected cert without error, got %v", cert.ErrorMessage)
	}
}
