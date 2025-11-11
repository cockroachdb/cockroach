// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlnemesis"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TODO: for some reason a goroutine is leaked right now.
func TestSQLNemesis(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	config := sqlnemesis.GeneratorConfig{
		NumIterations:   3,
		OpsPerIteration: 100,
		Validators: []sqlnemesis.Validator{
			&sqlnemesis.InspectValidator{},
		},
	}
	failures, err := sqlnemesis.RunNemesis(sqlDB, rng, config)
	require.NoError(t, err)
	for _, failure := range failures {
		require.NoError(t, failure)
	}
}
