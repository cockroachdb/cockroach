// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBarrier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	tsrv := srv.ApplicationLayer()

	start := append(tsrv.Codec().TenantPrefix(), []byte("/a")...)
	end := append(tsrv.Codec().TenantPrefix(), []byte("/b")...)

	// TODO(erikgrinaker): add some actual testing here.
	lai, desc, err := kvDB.BarrierWithLAI(ctx, start, end)
	require.NoError(t, err)
	require.NotZero(t, lai)
	require.NotZero(t, desc)
	t.Logf("lai=%d desc=%s", lai, desc)
}
