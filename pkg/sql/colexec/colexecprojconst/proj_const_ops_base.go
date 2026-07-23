// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecprojconst

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// projConstOpBase contains all of the fields for projections with a constant,
// except for the constant itself.
type projConstOpBase struct {
	colexecop.OneInputHelper
	allocator         *colmem.Allocator
	colIdx            int
	outputIdx         int
	calledOnNullInput bool
}
