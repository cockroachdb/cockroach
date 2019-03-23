// Copyright 2018 The Cockroach Authors.
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

package catpb

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]ForeignKeyReference_Match{
	tree.MatchSimple:  ForeignKeyReference_SIMPLE,
	tree.MatchFull:    ForeignKeyReference_FULL,
	tree.MatchPartial: ForeignKeyReference_PARTIAL,
}

// ForeignKeyReferenceMatchValue allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match CompositeKeyMatchMethodValue.
var ForeignKeyReferenceMatchValue = [...]tree.CompositeKeyMatchMethod{
	ForeignKeyReference_SIMPLE:  tree.MatchSimple,
	ForeignKeyReference_FULL:    tree.MatchFull,
	ForeignKeyReference_PARTIAL: tree.MatchPartial,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Match) String() string {
	switch x {
	case ForeignKeyReference_SIMPLE:
		return "MATCH SIMPLE"
	case ForeignKeyReference_FULL:
		return "MATCH FULL"
	case ForeignKeyReference_PARTIAL:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}

// ForeignKeyReferenceActionValue allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionValue = [...]ForeignKeyReference_Action{
	tree.NoAction:   ForeignKeyReference_NO_ACTION,
	tree.Restrict:   ForeignKeyReference_RESTRICT,
	tree.SetDefault: ForeignKeyReference_SET_DEFAULT,
	tree.SetNull:    ForeignKeyReference_SET_NULL,
	tree.Cascade:    ForeignKeyReference_CASCADE,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Action) String() string {
	switch x {
	case ForeignKeyReference_RESTRICT:
		return "RESTRICT"
	case ForeignKeyReference_SET_DEFAULT:
		return "SET DEFAULT"
	case ForeignKeyReference_SET_NULL:
		return "SET NULL"
	case ForeignKeyReference_CASCADE:
		return "CASCADE"
	default:
		return strconv.Itoa(int(x))
	}
}
