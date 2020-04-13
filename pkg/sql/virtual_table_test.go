// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVirtualTableGenerators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("test cleanup", func(t *testing.T) {
		worker := func(addRow func(...tree.Datum) error) error {
			if err := addRow(tree.NewDInt(1)); err != nil {
				return err
			}
			if err := addRow(tree.NewDInt(2)); err != nil {
				return err
			}
			return nil
		}

		next, cleanup := setupGenerator(worker)
		d, err := next()
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, tree.Datums{tree.NewDInt(1)}, d)

		// Check that we can safely cleanup in the middle of execution.
		cleanup()
	})

	t.Run("test worker error", func(t *testing.T) {
		// Test that if the worker returns an error we catch it.
		worker := func(addRow func(...tree.Datum) error) error {
			if err := addRow(tree.NewDInt(1)); err != nil {
				return err
			}
			if err := addRow(tree.NewDInt(2)); err != nil {
				return err
			}
			return errors.New("dummy error")
		}
		next, cleanup := setupGenerator(worker)
		_, err := next()
		require.NoError(t, err)
		_, err = next()
		require.NoError(t, err)
		_, err = next()
		require.Error(t, err)
		cleanup()
	})

	t.Run("test no next", func(t *testing.T) {
		// Test we don't leak anything if we call cleanup before next.
		worker := func(addRow func(...tree.Datum) error) error {
			return nil
		}
		_, cleanup := setupGenerator(worker)
		cleanup()
	})
}
