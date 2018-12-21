// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

// PrepareMetadata encapsulates information about a statement that is gathered
// during Prepare and is later used during Describe or Execute.
type PrepareMetadata struct {
	// AnonymizedStr is the anonymized statement string suitable for recording
	// in statement statistics.
	AnonymizedStr string
	// Statement is the parsed, prepared SQL statement. It may be nil if the
	// prepared statement is empty.
	Statement tree.Statement

	tree.PlaceholderTypesInfo

	Columns ResultColumns

	// InTypes represents the inferred types for placeholder, using protocol
	// identifiers. Used for reporting on Describe.
	InTypes []oid.Oid
}
