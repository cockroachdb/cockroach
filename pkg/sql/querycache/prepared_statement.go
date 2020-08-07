// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package querycache

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// PrepareMetadata encapsulates information about a statement that is gathered
// during Prepare and is later used during Describe or Execute.
type PrepareMetadata struct {
	// Note that AST may be nil if the prepared statement is empty.
	parser.Statement

	// AnonymizedStr is the anonymized statement string suitable for recording
	// in statement statistics.
	AnonymizedStr string

	// Provides TypeHints and Types fields which contain placeholder typing
	// information.
	tree.PlaceholderTypesInfo

	// Columns are the types and names of the query output columns.
	Columns colinfo.ResultColumns

	// InferredTypes represents the inferred types for placeholder, using protocol
	// identifiers. Used for reporting on Describe.
	InferredTypes []oid.Oid
}

// MemoryEstimate returns an estimation (in bytes) of how much memory is used by
// the prepare metadata.
func (pm *PrepareMetadata) MemoryEstimate() int64 {
	res := int64(unsafe.Sizeof(*pm))
	res += int64(len(pm.SQL))
	// We don't have a good way of estimating the size of the AST. Just assume
	// it's a small multiple of the string length.
	res += 2 * int64(len(pm.SQL))

	res += int64(len(pm.AnonymizedStr))

	res += int64(len(pm.TypeHints)+len(pm.Types)) *
		int64(unsafe.Sizeof(tree.PlaceholderIdx(0))+unsafe.Sizeof((*types.T)(nil)))

	res += int64(len(pm.Columns)) * int64(unsafe.Sizeof(colinfo.ResultColumn{}))
	res += int64(len(pm.InferredTypes)) * int64(unsafe.Sizeof(oid.Oid(0)))

	return res
}
