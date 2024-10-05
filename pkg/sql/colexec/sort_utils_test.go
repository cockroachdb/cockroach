// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type sortTestCase struct {
	description string
	tuples      colexectestutils.Tuples
	expected    colexectestutils.Tuples
	typs        []*types.T
	ordCols     []execinfrapb.Ordering_Column
	matchLen    int
	k           uint64
}
