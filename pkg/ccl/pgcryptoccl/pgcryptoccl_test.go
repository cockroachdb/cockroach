// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package pgcryptoccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCipherFunctionEnterpriseLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

	for name, query := range map[string]string{
		"decrypt":    `SELECT decrypt('\xdb5f149a7caf0cd275ca18c203a212c9', 'key', 'aes')`,
		"decrypt_iv": `SELECT decrypt_iv('\x91b4ef63852013c8da53829da662b871', 'key', '123', 'aes')`,
		"encrypt":    `SELECT encrypt('abc', 'key', 'aes')`,
		"encrypt_iv": `SELECT encrypt_iv('abc', 'key', '123', 'aes')`,
	} {
		t.Run(name, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "enterprise_license", func(t *testing.T, hasLicense bool) {
				if hasLicense {
					defer ccl.TestingEnableEnterprise()()
				} else {
					defer ccl.TestingDisableEnterprise()()
				}

				rows, err := db.QueryContext(ctx, query)

				if hasLicense {
					require.NoError(t, err)
					require.NoError(t, rows.Close())
				} else {
					require.ErrorContains(t, err, "use of pgcrypto cipher functions requires an enterprise license")
				}
			})
		})
	}
}
