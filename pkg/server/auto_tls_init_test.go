// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestDummyInitializeFromConfig is a placeholder for actual testing functions
// TODO(aaron-crl): [tests] write unit tests
func TestDummyInitializeFromConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	certBundle := CertificateBundle{}
	cfg := base.Config{}
	err := certBundle.InitializeFromConfig(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}
}

// TestDummyInitializeNodeFromBundle is a placeholder for actual testing functions
// TODO(aaron-crl): [tests] write unit tests
func TestDummyInitializeNodeFromBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	certBundle := CertificateBundle{}
	cfg := base.Config{}
	err := certBundle.InitializeNodeFromBundle(cfg)
	if err != nil {
		t.Fatalf("expected err=nil, got: %s", err)
	}
}
