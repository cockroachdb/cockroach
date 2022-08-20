// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTxnWithStepping tests that if the user opts into stepping, they
// get stepping.
func TestTxnWithStepping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cf := s.CollectionFactory().(*descs.CollectionFactory)
	scratchKey, err := s.ScratchRange()
	require.NoError(t, err)
	// Write a key, read in the transaction without stepping, ensure we
	// do not see the value, step the transaction, then ensure that we do.
	require.NoError(t, cf.Txn(ctx, kvDB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		if err := txn.Put(ctx, scratchKey, 1); err != nil {
			return err
		}
		{
			got, err := txn.Get(ctx, scratchKey)
			if err != nil {
				return err
			}
			if got.Exists() {
				return errors.AssertionFailedf("expected no value, got %v", got)
			}
		}
		if err := txn.Step(ctx); err != nil {
			return err
		}
		{
			got, err := txn.Get(ctx, scratchKey)
			if err != nil {
				return err
			}
			if got.ValueInt() != 1 {
				return errors.AssertionFailedf("expected 1, got %v", got)
			}
		}
		return nil
	}, descs.SteppingEnabled()))
}
