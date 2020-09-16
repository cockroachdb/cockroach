// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../../util/leaktest/add-leaktest.sh *_test.go

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
}

func TestForbiddenDeps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Verify kv does not depend on storage (or any of its subpackages).
	buildutil.VerifyNoImports(t,
		"github.com/cockroachdb/cockroach/pkg/kv", true,
		// TODO(tschottdorf): should really disallow ./storage/... but at the
		// time of writing there's a (legit) dependency on `enginepb`.
		[]string{
			"github.com/cockroachdb/cockroach/pkg/storage",
			"github.com/cockroachdb/cockroach/pkg/storage/engine",
		},
		[]string{})
}

func TestMain(m *testing.M) {
	serverutils.InitTestServerFactory(server.TestServerFactory)
	randutil.SeedForTests()
	os.Exit(m.Run())
}
