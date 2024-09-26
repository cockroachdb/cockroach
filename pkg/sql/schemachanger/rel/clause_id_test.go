// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClauseIDBuilder(t *testing.T) {
	cb := clauseIDBuilder{}
	require.Equal(t, 0, cb.nextID())
	require.Equal(t, 1, cb.nextID())
	subCB := cb.newBuilderForSubquery()
	require.Equal(t, 2, subCB.nextID())
	require.Equal(t, 2, subCB.nextID())
	subSubCB := subCB.newBuilderForSubquery()
	require.Equal(t, 2, subSubCB.nextID())
	require.Equal(t, 3, cb.nextID())
}
