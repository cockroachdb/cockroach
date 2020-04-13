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
	"github.com/stretchr/testify/require"
)

func TestVirtualTableGenerators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	worker := func(addRow func(...tree.Datum) error) error {
		if err := addRow(tree.NewDInt(1)); err != nil {
			return err
		}
		if err := addRow(tree.NewDInt(2)); err != nil {
			return err
		}
		return nil
	}

	cleanup, start, next := setupGenerator(worker)
	go start()
	d, err := next()
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, tree.Datums{tree.NewDInt(1)}, d)

	// Check that we can safely cleanup in the middle of execution.
	cleanup()
}
