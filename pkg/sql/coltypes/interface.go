// Copyright 2015 The Cockroach Authors.
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

package coltypes

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// ColTypeFormatter knows how to format a ColType to a bytes.Buffer.
type ColTypeFormatter interface {
	fmt.Stringer

	// TypeName returns the base name of the type, suitable to generate
	// column names for cast expressions.
	TypeName() string

	// Format returns a non-lossy string representation of the coltype.
	// NOTE: It is important that two coltypes that should be different print out
	//       different string representations. The optimizer relies on unique
	//       string representations in order to intern the coltypes during
	//       memoization.
	Format(buf *bytes.Buffer, flags lex.EncodeFlags)
}

// ColTypeAsString print a T to a string.
func ColTypeAsString(n ColTypeFormatter) string {
	var buf bytes.Buffer
	n.Format(&buf, lex.EncNoFlags)
	return buf.String()
}

// CastTargetType represents a type that is a valid cast target.
type CastTargetType interface {
	ColTypeFormatter
	castTargetType()
}

// T represents a type in a column definition.
type T interface {
	CastTargetType

	columnType()
}

func (*TArray) columnType()          {}
func (*TBitArray) columnType()       {}
func (*TBool) columnType()           {}
func (*TBytes) columnType()          {}
func (*TCollatedString) columnType() {}
func (*TDate) columnType()           {}
func (*TDecimal) columnType()        {}
func (*TFloat) columnType()          {}
func (*TIPAddr) columnType()         {}
func (*TInt) columnType()            {}
func (*TInterval) columnType()       {}
func (*TJSON) columnType()           {}
func (*TName) columnType()           {}
func (*TOid) columnType()            {}
func (*TSerial) columnType()         {}
func (*TString) columnType()         {}
func (*TTime) columnType()           {}
func (*TTimestamp) columnType()      {}
func (*TTimestampTZ) columnType()    {}
func (*TUUID) columnType()           {}
func (*TVector) columnType()         {}
func (TTuple) columnType()           {}

// All Ts also implement CastTargetType.
func (*TArray) castTargetType()          {}
func (*TBitArray) castTargetType()       {}
func (*TBool) castTargetType()           {}
func (*TBytes) castTargetType()          {}
func (*TCollatedString) castTargetType() {}
func (*TDate) castTargetType()           {}
func (*TDecimal) castTargetType()        {}
func (*TFloat) castTargetType()          {}
func (*TIPAddr) castTargetType()         {}
func (*TInt) castTargetType()            {}
func (*TInterval) castTargetType()       {}
func (*TJSON) castTargetType()           {}
func (*TName) castTargetType()           {}
func (*TOid) castTargetType()            {}
func (*TSerial) castTargetType()         {}
func (*TString) castTargetType()         {}
func (*TTime) castTargetType()           {}
func (*TTimestamp) castTargetType()      {}
func (*TTimestampTZ) castTargetType()    {}
func (*TUUID) castTargetType()           {}
func (*TVector) castTargetType()         {}
func (TTuple) castTargetType()           {}

func (node *TArray) String() string          { return ColTypeAsString(node) }
func (node *TBitArray) String() string       { return ColTypeAsString(node) }
func (node *TBool) String() string           { return ColTypeAsString(node) }
func (node *TBytes) String() string          { return ColTypeAsString(node) }
func (node *TCollatedString) String() string { return ColTypeAsString(node) }
func (node *TDate) String() string           { return ColTypeAsString(node) }
func (node *TDecimal) String() string        { return ColTypeAsString(node) }
func (node *TFloat) String() string          { return ColTypeAsString(node) }
func (node *TIPAddr) String() string         { return ColTypeAsString(node) }
func (node *TInt) String() string            { return ColTypeAsString(node) }
func (node *TInterval) String() string       { return ColTypeAsString(node) }
func (node *TJSON) String() string           { return ColTypeAsString(node) }
func (node *TName) String() string           { return ColTypeAsString(node) }
func (node *TOid) String() string            { return ColTypeAsString(node) }
func (node *TSerial) String() string         { return ColTypeAsString(node) }
func (node *TString) String() string         { return ColTypeAsString(node) }
func (node *TTime) String() string           { return ColTypeAsString(node) }
func (node *TTimestamp) String() string      { return ColTypeAsString(node) }
func (node *TTimestampTZ) String() string    { return ColTypeAsString(node) }
func (node *TUUID) String() string           { return ColTypeAsString(node) }
func (node *TVector) String() string         { return ColTypeAsString(node) }
func (node TTuple) String() string           { return ColTypeAsString(node) }
