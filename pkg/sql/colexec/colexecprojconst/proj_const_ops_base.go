// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecprojconst

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

// projConstOpBase contains all of the fields for projections with a constant,
// except for the constant itself.
type projConstOpBase struct {
	colexecop.OneInputHelper
	allocator    *colmem.Allocator
	colIdx       int
	outputIdx    int
	nullableArgs bool
}
