// Copyright 2021 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestDummyCreateCACertAndKey is a placeholder for actual testing functions
// TODO(aaron-crl): [tests] write unit tests
func TestDummyCreateCACertAndKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, _, err := security.CreateCACertAndKey(time.Hour, "test CA cert generation")
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}
}

// TestDummyCreateServiceCertAndKey is a placeholder for actual testing functions
// TODO(aaron-crl): [tests] write unit tests
func TestDummyCreateServiceCertAndKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	caCert, caKey, err := security.CreateCACertAndKey(time.Hour, "test CA cert generation")
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}

	_, _, err = security.CreateServiceCertAndKey(
		time.Minute,
		"test Service cert generation",
		"localhost",
		caCert,
		caKey,
	)
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}
}
