// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
	Columns ResultColumns

	// InTypes represents the inferred types for placeholder, using protocol
	// identifiers. Used for reporting on Describe.
	InTypes []oid.Oid
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
		int64(unsafe.Sizeof(types.PlaceholderIdx(0))+unsafe.Sizeof(types.T(nil)))

	res += int64(len(pm.Columns)) * int64(unsafe.Sizeof(ResultColumn{}))
	res += int64(len(pm.InTypes)) * int64(unsafe.Sizeof(oid.Oid(0)))
	return res
}
