// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// DatumContainerElem is abstract type for elements inside DatumContainer, this
// type in reality should be tree.Datum. However, in order te avoid pulling in
// pkg/sql/sem/tree package into the coldata package, we have to rely on duck
// typing and runtime cast instead.
type DatumContainerElem interface{}

// DatumContainer is the interface for a specialized vector that operates on
// tree.Datum for vectorized engine. In order to avoid import tree package the
// implementation of DatumContainer lives in pkg/sql/colmem/datum_vec.go.
type DatumContainer interface {
	Get(i int) DatumContainerElem
	Set(i int, v DatumContainerElem)
	Slice(start, end int) DatumContainer
	CopySlice(src DatumContainer, destIdx, srcStartIdx, srcEndIdx int)
	AppendSlice(src DatumContainer, destIdx, srcStartIdx, srcEndIdx int)
	AppendVal(v DatumContainerElem)
	SetLength(l int)
	MarshalAt(i int) ([]byte, error)
	UnmarshalTo(i int, b []byte) error
	SetType(t *types.T)
	Len() int
	Cap() int
}
