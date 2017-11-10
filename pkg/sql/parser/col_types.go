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

package parser

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

// ColTypeAsString print a ColumnType to a string.
func ColTypeAsString(n ColTypeFormatter) string {
	var buf bytes.Buffer
	n.Format(&buf, lex.EncodeFlags{})
	return buf.String()
}

// CastTargetType represents a type that is a valid cast target.
type CastTargetType interface {
	ColTypeFormatter
	castTargetType()
}

// ColumnType represents a type in a column definition.
type ColumnType interface {
	CastTargetType

	columnType()
}

func (*BoolColType) columnType()           {}
func (*IntColType) columnType()            {}
func (*FloatColType) columnType()          {}
func (*DecimalColType) columnType()        {}
func (*DateColType) columnType()           {}
func (*TimestampColType) columnType()      {}
func (*TimestampTZColType) columnType()    {}
func (*IntervalColType) columnType()       {}
func (*JSONColType) columnType()           {}
func (*UUIDColType) columnType()           {}
func (*IPAddrColType) columnType()         {}
func (*StringColType) columnType()         {}
func (*NameColType) columnType()           {}
func (*BytesColType) columnType()          {}
func (*CollatedStringColType) columnType() {}
func (*ArrayColType) columnType()          {}
func (*VectorColType) columnType()         {}
func (*OidColType) columnType()            {}

// All ColumnTypes also implement CastTargetType.
func (*BoolColType) castTargetType()           {}
func (*IntColType) castTargetType()            {}
func (*FloatColType) castTargetType()          {}
func (*DecimalColType) castTargetType()        {}
func (*DateColType) castTargetType()           {}
func (*TimestampColType) castTargetType()      {}
func (*TimestampTZColType) castTargetType()    {}
func (*IntervalColType) castTargetType()       {}
func (*JSONColType) castTargetType()           {}
func (*UUIDColType) castTargetType()           {}
func (*IPAddrColType) castTargetType()         {}
func (*StringColType) castTargetType()         {}
func (*NameColType) castTargetType()           {}
func (*BytesColType) castTargetType()          {}
func (*CollatedStringColType) castTargetType() {}
func (*ArrayColType) castTargetType()          {}
func (*VectorColType) castTargetType()         {}
func (*OidColType) castTargetType()            {}

func (node *BoolColType) String() string           { return ColTypeAsString(node) }
func (node *IntColType) String() string            { return ColTypeAsString(node) }
func (node *FloatColType) String() string          { return ColTypeAsString(node) }
func (node *DecimalColType) String() string        { return ColTypeAsString(node) }
func (node *DateColType) String() string           { return ColTypeAsString(node) }
func (node *TimestampColType) String() string      { return ColTypeAsString(node) }
func (node *TimestampTZColType) String() string    { return ColTypeAsString(node) }
func (node *IntervalColType) String() string       { return ColTypeAsString(node) }
func (node *JSONColType) String() string           { return ColTypeAsString(node) }
func (node *UUIDColType) String() string           { return ColTypeAsString(node) }
func (node *IPAddrColType) String() string         { return ColTypeAsString(node) }
func (node *StringColType) String() string         { return ColTypeAsString(node) }
func (node *NameColType) String() string           { return ColTypeAsString(node) }
func (node *BytesColType) String() string          { return ColTypeAsString(node) }
func (node *CollatedStringColType) String() string { return ColTypeAsString(node) }
func (node *ArrayColType) String() string          { return ColTypeAsString(node) }
func (node *VectorColType) String() string         { return ColTypeAsString(node) }
func (node *OidColType) String() string            { return ColTypeAsString(node) }
