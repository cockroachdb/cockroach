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

func (*TBool) columnType()           {}
func (*TInt) columnType()            {}
func (*TFloat) columnType()          {}
func (*TDecimal) columnType()        {}
func (*TDate) columnType()           {}
func (*TTime) columnType()           {}
func (*TTimestamp) columnType()      {}
func (*TTimestampTZ) columnType()    {}
func (*TInterval) columnType()       {}
func (*TJSON) columnType()           {}
func (*TUUID) columnType()           {}
func (*TIPAddr) columnType()         {}
func (*TString) columnType()         {}
func (*TName) columnType()           {}
func (*TBytes) columnType()          {}
func (*TCollatedString) columnType() {}
func (*TArray) columnType()          {}
func (*TVector) columnType()         {}
func (*TOid) columnType()            {}

// All Ts also implement CastTargetType.
func (*TBool) castTargetType()           {}
func (*TInt) castTargetType()            {}
func (*TFloat) castTargetType()          {}
func (*TDecimal) castTargetType()        {}
func (*TDate) castTargetType()           {}
func (*TTime) castTargetType()           {}
func (*TTimestamp) castTargetType()      {}
func (*TTimestampTZ) castTargetType()    {}
func (*TInterval) castTargetType()       {}
func (*TJSON) castTargetType()           {}
func (*TUUID) castTargetType()           {}
func (*TIPAddr) castTargetType()         {}
func (*TString) castTargetType()         {}
func (*TName) castTargetType()           {}
func (*TBytes) castTargetType()          {}
func (*TCollatedString) castTargetType() {}
func (*TArray) castTargetType()          {}
func (*TVector) castTargetType()         {}
func (*TOid) castTargetType()            {}

func (node *TBool) String() string           { return ColTypeAsString(node) }
func (node *TInt) String() string            { return ColTypeAsString(node) }
func (node *TFloat) String() string          { return ColTypeAsString(node) }
func (node *TDecimal) String() string        { return ColTypeAsString(node) }
func (node *TDate) String() string           { return ColTypeAsString(node) }
func (node *TTime) String() string           { return ColTypeAsString(node) }
func (node *TTimestamp) String() string      { return ColTypeAsString(node) }
func (node *TTimestampTZ) String() string    { return ColTypeAsString(node) }
func (node *TInterval) String() string       { return ColTypeAsString(node) }
func (node *TJSON) String() string           { return ColTypeAsString(node) }
func (node *TUUID) String() string           { return ColTypeAsString(node) }
func (node *TIPAddr) String() string         { return ColTypeAsString(node) }
func (node *TString) String() string         { return ColTypeAsString(node) }
func (node *TName) String() string           { return ColTypeAsString(node) }
func (node *TBytes) String() string          { return ColTypeAsString(node) }
func (node *TCollatedString) String() string { return ColTypeAsString(node) }
func (node *TArray) String() string          { return ColTypeAsString(node) }
func (node *TVector) String() string         { return ColTypeAsString(node) }
func (node *TOid) String() string            { return ColTypeAsString(node) }
